# ============================================================
# scheduler_master.py — Single-Browser Multi-League Daemon
# ============================================================
"""
Centralized 24/7 scheduler: 1 browser, all leagues, all matches.

Replaces 5 × scheduler.py processes with 1 master process that shares
a single nodriver Chrome instance across all leagues and live matches.

Usage:
    python scheduler_master.py                           # All leagues
    python scheduler_master.py --leagues EPL LALIGA      # Subset
    python scheduler_master.py --dry-run
    python scheduler_master.py --test-notify

Architecture:
    SharedBrowser ─── 1 nodriver, asyncio.Lock, recycles every 200 reqs
    ScheduleManager ─ fetches ALL leagues in 1 session
    LiveTrackingPool  round-robin polling, serialized API calls
    DailyMaintenance  BLOCKED while pool has active matches
    PostMatchWorker ─ subprocess-based, runs after each match
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import random
import signal
import subprocess
import sys
import tempfile
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from services import insight_producer, insight_worker, live_insight

# ── Paths ──
ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sofascore"))

from curl_cffi.requests import AsyncSession

import sofascore.config_sofascore as cfg

# ── SofaScore tournament IDs ──
TOURNAMENT_IDS = {
    "EPL": 17,
    "LALIGA": 8,
    "BUNDESLIGA": 35,
    "SERIEA": 23,
    "LIGUE1": 34,
    "UCL": 7,
    "UEL": 677,  # ← thêm UEL
    "EREDIVISIE": 37,
    "LIGA_PORTUGAL": 238,
    "RFPL": 203,
}

# ── Source support matrix ──
LEAGUE_SOURCES = {
    "EPL": {"understat": True, "fbref": True, "sofascore": True, "transfermarkt": True},
    "LALIGA": {
        "understat": True,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "BUNDESLIGA": {
        "understat": True,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "SERIEA": {
        "understat": True,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "LIGUE1": {
        "understat": True,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "UCL": {
        "understat": False,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "UEL": {
        "understat": False,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "EREDIVISIE": {
        "understat": False,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "LIGA_PORTUGAL": {
        "understat": False,
        "fbref": True,
        "sofascore": True,
        "transfermarkt": True,
    },
    "RFPL": {
        "understat": True,
        "fbref": False,
        "sofascore": True,
        "transfermarkt": True,
    },
}

# ── Retry / recycle config ──
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [60, 300, 900]
BROWSER_RECYCLE_EVERY = 200  # recycle Chrome every N requests


# ════════════════════════════════════════════════════════════
# ANTI-BAN STATE MACHINE (plan_live Step 1)
# ════════════════════════════════════════════════════════════


class AntiBanState(Enum):
    NORMAL = "NORMAL"
    THROTTLED_429 = "THROTTLED_429"
    DEGRADED_403 = "DEGRADED_403"
    FALLBACK = "FALLBACK"  # intermediate after DEGRADED_403 timeout


class AntiBanStateMachine:
    """Manages anti-ban runtime states with hard timeouts and exit evaluators."""

    # Hard timeout per state (seconds)
    _TIMEOUT = {
        AntiBanState.THROTTLED_429: 20 * 60,  # 20 min
        AntiBanState.DEGRADED_403: 45 * 60,  # 45 min
        AntiBanState.FALLBACK: 10 * 60,  # 10 min
    }

    # Tier intervals (seconds) per state
    TIER_A_INTERVAL = {
        AntiBanState.NORMAL: 60,
        AntiBanState.THROTTLED_429: 60,
        AntiBanState.DEGRADED_403: 105,  # 90-120 midpoint
        AntiBanState.FALLBACK: 120,
    }
    TIER_B_INTERVAL = {
        AntiBanState.NORMAL: 180,  # every 3rd poll ≈ 3×60
        AntiBanState.THROTTLED_429: 240,
        AntiBanState.DEGRADED_403: 300,
        AntiBanState.FALLBACK: 0,  # 0 = paused
    }
    TIER_C_INTERVAL = {
        AntiBanState.NORMAL: 300,
        AntiBanState.THROTTLED_429: 120,  # event-driven only, 120s cooldown
        AntiBanState.DEGRADED_403: 0,  # 0 = fully paused
        AntiBanState.FALLBACK: 0,  # 0 = fully paused
    }

    def __init__(self, log: logging.Logger):
        self.log = log
        self.state = AntiBanState.NORMAL
        self._entered_at: float = time.monotonic()
        # Rolling window of (timestamp, status_code, tier) tuples
        self._history: deque[tuple[float, int, str]] = deque(maxlen=100)
        # Consecutive counters
        self._consecutive_429: int = 0
        self._consecutive_403: int = 0
        self._consecutive_200_fallback: int = 0  # for FALLBACK exit
        self._recycle_requested: bool = False

    @property
    def needs_recycle(self) -> bool:
        """Check and clear the one-shot recycle flag."""
        if self._recycle_requested:
            self._recycle_requested = False
            return True
        return False

    @property
    def global_sleep_multiplier(self) -> float:
        """Extra jitter multiplier for throttled states."""
        if self.state == AntiBanState.THROTTLED_429:
            return 1.2  # +20%
        return 1.0

    def record_response(self, status_code: int, tier: str = "A") -> None:
        """Record an HTTP response and evaluate state transitions."""
        now = time.monotonic()
        self._history.append((now, status_code, tier))

        if status_code == 200:
            self._consecutive_429 = 0
            self._consecutive_403 = 0
            if self.state == AntiBanState.FALLBACK:
                self._consecutive_200_fallback += 1
        elif status_code == 429:
            self._consecutive_429 += 1
            self._consecutive_403 = 0
            self._consecutive_200_fallback = 0
        elif status_code == 403:
            self._consecutive_403 += 1
            self._consecutive_429 = 0
            self._consecutive_200_fallback = 0

        self._evaluate(now)

    def check_timeout(self) -> None:
        """Call periodically to check hard timeouts."""
        if self.state == AntiBanState.NORMAL:
            return
        now = time.monotonic()
        elapsed = now - self._entered_at
        timeout = self._TIMEOUT.get(self.state, 0)
        if timeout and elapsed >= timeout:
            if self.state == AntiBanState.DEGRADED_403:
                self._transition(
                    AntiBanState.FALLBACK, f"hard timeout after {elapsed / 60:.0f}m"
                )
            elif self.state == AntiBanState.FALLBACK:
                self._transition(
                    AntiBanState.NORMAL, f"FALLBACK expired after {elapsed / 60:.0f}m"
                )
            else:
                # THROTTLED_429 timeout → try NORMAL
                self._transition(
                    AntiBanState.NORMAL, f"hard timeout after {elapsed / 60:.0f}m"
                )

    def _evaluate(self, now: float) -> None:
        """Evaluate state transitions based on current counters and history."""
        # ── Entry conditions ──
        if self.state == AntiBanState.NORMAL:
            if self._consecutive_429 >= 3:
                self._transition(
                    AntiBanState.THROTTLED_429,
                    f"consecutive_429={self._consecutive_429}",
                )
            elif self._consecutive_403 >= 2:
                self._transition(
                    AntiBanState.DEGRADED_403,
                    f"consecutive_403={self._consecutive_403}",
                )
            return

        if self.state == AntiBanState.FALLBACK:
            # Re-enter DEGRADED on any 403
            if self._consecutive_403 >= 1:
                self._transition(AntiBanState.DEGRADED_403, "403 during FALLBACK")
                return
            # Exit FALLBACK → NORMAL after 5 consecutive 200 on Tier A
            if self._consecutive_200_fallback >= 5:
                self._transition(AntiBanState.NORMAL, "5 consecutive 200 in FALLBACK")
            return

        # ── Exit conditions for THROTTLED_429 ──
        if self.state == AntiBanState.THROTTLED_429:
            if self._check_mixed_exit(now):
                self._transition(
                    AntiBanState.NORMAL, "mixed-endpoint exit criteria met"
                )
            return

        # ── Exit conditions for DEGRADED_403 ──
        if self.state == AntiBanState.DEGRADED_403:
            window = 600  # 10 min
            recent = [
                (t, sc, tier) for t, sc, tier in self._history if now - t <= window
            ]
            ok_200 = [r for r in recent if r[1] == 200]
            any_403 = any(r[1] == 403 for r in recent)
            if len(ok_200) >= 8 and not any_403:
                self._transition(
                    AntiBanState.NORMAL, f"10min: {len(ok_200)} x 200, no 403"
                )

    def _check_mixed_exit(self, now: float) -> bool:
        """Check THROTTLED_429 exit: mixed endpoint 200s in rolling 10min window."""
        window = 600
        recent = [(t, sc, tier) for t, sc, tier in self._history if now - t <= window]
        ok_200 = [r for r in recent if r[1] == 200]
        if len(ok_200) < 5:
            return False
        has_tier_a = any(r[2] == "A" for r in ok_200)
        has_non_a = any(r[2] in ("B", "C") for r in ok_200)
        if not (has_tier_a and has_non_a):
            return False
        # No burst of 429 in last 2 minutes
        last_2min = [r for r in recent if now - r[0] <= 120 and r[1] == 429]
        return len(last_2min) < 2

    def _transition(self, new_state: AntiBanState, reason: str) -> None:
        old = self.state
        self.state = new_state
        self._entered_at = time.monotonic()
        self._consecutive_200_fallback = 0
        if new_state in (AntiBanState.DEGRADED_403, AntiBanState.FALLBACK):
            self._recycle_requested = True
        self.log.warning("⚡ State %s → %s (%s)", old.value, new_state.value, reason)

    def should_allow_fast_poll(self) -> bool:
        """Whether 0-15 min fast polling (45s) is allowed."""
        return self.state == AntiBanState.NORMAL

    def tier_c_cooldown(self) -> int:
        """Minimum seconds between Tier C fetches for a single match."""
        if self.state == AntiBanState.NORMAL:
            return 90
        elif self.state == AntiBanState.THROTTLED_429:
            return 120
        else:
            return 0  # 0 = Tier C paused

    def capacity_adjusted_interval(self, tier: str, active_matches: int) -> int:
        """Adjust tier interval by active match count (plan_live Step 5)."""
        base = {
            "A": self.TIER_A_INTERVAL[self.state],
            "B": self.TIER_B_INTERVAL[self.state],
            "C": self.TIER_C_INTERVAL[self.state],
        }.get(tier, 60)
        if active_matches <= 5:
            return base
        elif active_matches <= 10:
            if tier == "B":
                return max(base, 240)
            elif tier == "C":
                return max(base, 420)
        else:  # > 10
            if tier == "B":
                return max(base, 300)
            elif tier == "C":
                return max(base, 480)  # event-driven only, 8min hard cap
        return base


# ════════════════════════════════════════════════════════════
# LIVE METRICS (plan_live Step 2)
# ════════════════════════════════════════════════════════════


class LiveMetrics:
    """Lightweight counters emitted every 60s for observability."""

    def __init__(self, log: logging.Logger):
        self.log = log
        self._start = time.monotonic()
        self._last_emit: float = 0.0
        # Counters (reset each emit window)
        self.requests_total: int = 0
        self.tier_a_calls: int = 0
        self.tier_b_calls: int = 0
        self.tier_c_calls: int = 0
        self.skipped_polls: int = 0
        self.errors_429: int = 0
        self.errors_403: int = 0

    def record_request(self, tier: str = "A", status_code: int = 200) -> None:
        self.requests_total += 1
        if tier == "A":
            self.tier_a_calls += 1
        elif tier == "B":
            self.tier_b_calls += 1
        elif tier == "C":
            self.tier_c_calls += 1
        if status_code == 429:
            self.errors_429 += 1
        elif status_code == 403:
            self.errors_403 += 1

    def record_skip(self) -> None:
        self.skipped_polls += 1

    def maybe_emit(self, active_matches: int, state: AntiBanState) -> None:
        """Emit metric log line every 60s."""
        now = time.monotonic()
        if now - self._last_emit < 60:
            return
        elapsed = max(now - self._last_emit, 1) if self._last_emit else 60
        rpm = self.requests_total / (elapsed / 60)
        self.log.info(
            "📈 METRICS | rpm=%.1f | tier_a=%d tier_b=%d tier_c=%d | "
            "skip=%d | 429=%d 403=%d | active=%d | state=%s",
            rpm,
            self.tier_a_calls,
            self.tier_b_calls,
            self.tier_c_calls,
            self.skipped_polls,
            self.errors_429,
            self.errors_403,
            active_matches,
            state.value,
        )
        # Reset window counters
        self.requests_total = 0
        self.tier_a_calls = 0
        self.tier_b_calls = 0
        self.tier_c_calls = 0
        self.skipped_polls = 0
        self.errors_429 = 0
        self.errors_403 = 0
        self._last_emit = now


# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════


def setup_logging() -> logging.Logger:
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log = logging.getLogger("master")
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [MASTER] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    fh = logging.handlers.RotatingFileHandler(
        log_dir / "scheduler_master.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)
    return log


# ════════════════════════════════════════════════════════════
# DISCORD NOTIFIER
# ════════════════════════════════════════════════════════════


class Notifier:
    """Routes messages to specific Discord webhooks based on event type."""

    EMOJI = {
        "match_start": "🟢",
        "goal": "⚽",
        "match_end": "🏁",
        "match_event": "⚡",
        "post_match_done": "📊",
        "error": "🔴",
        "daily_done": "🔧",
        "info": "ℹ️",
        "recycle": "♻️",
        "ai_insight": "🤖",
    }

    # Embed color map (decimal)
    COLOR = {
        "goal": 0x00FF00,        # Green
        "match_start": 0x3498DB,  # Blue
        "match_end": 0x95A5A6,   # Gray
        "error": 0xFF0000,       # Red
        "ai_insight": 0xFFA500,  # Orange
        "info": 0x2ECC71,        # Emerald
        "daily_done": 0x9B59B6,  # Purple
    }

    # Map event types to specific webhook environment variables
    ROUTING = {
        "match_start": "DISCORD_WEBHOOK_LIVE",
        "goal": "DISCORD_WEBHOOK_LIVE",
        "match_event": "DISCORD_WEBHOOK_LIVE",
        "match_end": "DISCORD_WEBHOOK_LIVE",
        "ai_insight": "DISCORD_WEBHOOK_LIVE",
        "error": "DISCORD_WEBHOOK_ERROR",
        "info": "DISCORD_WEBHOOK_INFO",
        "daily_done": "DISCORD_WEBHOOK_INFO",
        "post_match_done": "DISCORD_WEBHOOK_INFO",
        "recycle": "DISCORD_WEBHOOK_INFO",
    }

    def __init__(self, log: logging.Logger):
        self.log = log
        # Default webhook if specific ones aren't set
        self.default_webhook = os.environ.get("DISCORD_WEBHOOK", "")

    @property
    def is_enabled(self) -> bool:
        return bool(
            self.default_webhook
            or any(os.environ.get(env) for env in set(self.ROUTING.values()))
        )

    def _get_webhook_url(self, event_type: str) -> str:
        env_var = self.ROUTING.get(event_type, "")
        webhook_url = os.environ.get(env_var) if env_var else ""
        return webhook_url or self.default_webhook

    def _post_webhook(self, webhook_url: str, payload: dict) -> bool:
        """Post JSON to Discord webhook with 429 retry_after handling."""
        if not webhook_url:
            return False
        import urllib.request
        import urllib.error

        data = json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "VertexFootballScraper/2.0",
        }

        for attempt in range(2):  # max 1 retry
            try:
                req = urllib.request.Request(
                    webhook_url, data=data, headers=headers, method="POST"
                )
                resp = urllib.request.urlopen(req, timeout=10)
                return True
            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt == 0:
                    try:
                        body = json.loads(e.read().decode())
                        retry_after = body.get("retry_after", 5)
                    except Exception:
                        retry_after = 5
                    self.log.warning(
                        "Discord rate limited, retrying after %.1fs", retry_after
                    )
                    time.sleep(retry_after)
                    continue
                self.log.warning("Discord HTTP %d: %s", e.code, e.reason)
                return False
            except Exception as exc:
                self.log.warning("Discord send failed: %s", exc)
                return False
        return False

    def send(self, event_type: str, message: str) -> None:
        """Legacy plain-text send (backward compatible)."""
        emoji = self.EMOJI.get(event_type, "📢")
        full_msg = f"{emoji} **[MASTER]** {message}"
        self.log.info("NOTIFY [%s]: %s", event_type, message)

        webhook_url = self._get_webhook_url(event_type)
        self._post_webhook(webhook_url, {"content": full_msg})

    def send_embed(
        self,
        event_type: str,
        title: str,
        *,
        text_en: str = "",
        text_vi: str = "",
        description: str = "",
        footer: str = "",
        fields: list[dict] | None = None,
    ) -> None:
        """Send a Rich Embed to Discord with optional dual-language fields."""
        color = self.COLOR.get(event_type, 0x95A5A6)
        emoji = self.EMOJI.get(event_type, "📢")

        embed: dict = {
            "title": f"{emoji} {title}",
            "color": color,
        }
        if description:
            embed["description"] = description

        embed_fields = []
        if text_en:
            embed_fields.append({"name": "🇺🇸 English", "value": text_en, "inline": False})
        if text_vi:
            embed_fields.append({"name": "🇻🇳 Tiếng Việt", "value": text_vi, "inline": False})
        if fields:
            embed_fields.extend(fields)
        if embed_fields:
            embed["fields"] = embed_fields

        if footer:
            embed["footer"] = {"text": footer}

        self.log.info("NOTIFY-EMBED [%s]: %s", event_type, title)
        webhook_url = self._get_webhook_url(event_type)
        self._post_webhook(webhook_url, {"embeds": [embed]})


# ════════════════════════════════════════════════════════════
# HTTP CLIENT — curl_cffi (Replaces SharedBrowser)
# ════════════════════════════════════════════════════════════


class CurlCffiClient:
    """Lightweight HTTP client that bypasses Cloudflare using TLS impersonation."""

    # Retry backoff timings (seconds)
    RETRY_BACKOFF = [4, 10, 20]
    MAX_RETRIES = 3

    def __init__(
        self, log: logging.Logger, notifier: "Notifier", *, dry_run: bool = False
    ):
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._session: AsyncSession | None = None
        self._lock = asyncio.Lock()
        self._request_count = 0
        self._last_request: float = 0.0
        self._consecutive_403 = 0
        self._consecutive_429 = 0
        self._last_block_alert = 0.0
        self.antiban: AntiBanStateMachine | None = None
        self.metrics: LiveMetrics | None = None

    def _check_block_alert(self, url: str, code: int) -> None:
        """Trigger a Discord alert if Cloudflare/rate-limit blocking is persistent."""
        count = self._consecutive_403 if code == 403 else self._consecutive_429
        if count >= 5:
            now = time.time()
            if now - self._last_block_alert > 3600:  # Alert once per hour max
                if code == 403:
                    self.log.error(
                        "CRITICAL: Cloudflare persistently blocked our TLS footprint (403 Forbidden)."
                    )
                    self.notifier.send(
                        "error",
                        f"**🚨 CLOUDFLARE BLOCK ALERT**\n"
                        f"Received {count} consecutive `403 Forbidden` responses.\n"
                        f"The impersonate profile (`chrome120`) may have been detected and blocked by Cloudflare.\n"
                        f"Last blocked URL: `{url}`\n"
                        f"*Action required: Update impersonate version in CurlCffiClient.*",
                    )
                else:
                    self.log.error(
                        "CRITICAL: SofaScore rate limiting our IP (429 Too Many Requests)."
                    )
                    self.notifier.send(
                        "error",
                        f"**⚡ RATE LIMIT ALERT**\n"
                        f"Received {count} consecutive `429 Too Many Requests` responses.\n"
                        f"SofaScore is throttling our requests. IP may be temporarily blocked.\n"
                        f"*The system will auto-backoff, but monitor closely.*",
                    )
                self._last_block_alert = now

    async def start(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] HTTP Client start skipped")
            return
        self.log.info("🌐 Starting Curl CFFI HTTP Client (impersonating Chrome 120)...")
        self._session = AsyncSession(
            impersonate="chrome120",
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Cache-Control": "no-cache",
                "Referer": "https://www.sofascore.com/",
                "Origin": "https://www.sofascore.com",
            },
        )
        self._request_count = 0

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def recycle(self) -> None:
        """curl_cffi doesn't leak memory like Chrome, but we recycle the session to clear cookies/state."""
        self.log.info(
            "♻️  Recycling HTTP session (after %d requests)...", self._request_count
        )
        await self.stop()
        await self.start()

    def needs_recycle(self) -> bool:
        return self._request_count >= BROWSER_RECYCLE_EVERY

    async def get_json(self, endpoint: str, *, tier: str = "A") -> dict | list | None:
        """Fetch JSON from SofaScore API with retry and jitter.
        Lock scope narrowed: only wraps the actual HTTP call.
        Jitter before lock + backoff after lock = anti-bot natural spacing."""
        if self.dry_run:
            return None

        url = f"{cfg.SS_API_BASE}{endpoint}"
        sc = 0  # track last status code for backoff decisions outside lock

        for attempt in range(self.MAX_RETRIES):
            # 1. JITTER OUTSIDE LOCK: natural spacing between requests
            #    prevents bot-like burst when multiple matches queue up
            mult = self.antiban.global_sleep_multiplier if self.antiban else 1.0
            jitter = random.uniform(1.5, 3.5) * mult
            await asyncio.sleep(jitter)

            # 2. NARROW LOCK: only wraps the actual HTTP request
            async with self._lock:
                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=15)
                    sc = resp.status_code

                    # ── record into anti-ban state machine + metrics ──
                    if self.antiban:
                        self.antiban.record_response(sc, tier)
                        self.antiban.check_timeout()
                        if self.antiban.needs_recycle:
                            self.log.info("♻️  Anti-ban triggered session recycle")
                            await self.recycle()
                    if self.metrics:
                        self.metrics.record_request(tier, sc)

                    if sc == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        data = resp.json()
                        if isinstance(data, dict) and data.get("error"):
                            if data["error"].get("code") == 404:
                                return None
                        return data

                    elif sc == 403:
                        self._consecutive_403 += 1
                        self.log.warning(
                            "HTTP 403 Forbidden (attempt %d/%d): %s",
                            attempt + 1,
                            self.MAX_RETRIES,
                            url,
                        )
                        self._check_block_alert(url, 403)

                    elif sc == 429:
                        self._consecutive_429 += 1
                        self.log.warning(
                            "HTTP 429 Rate Limited (attempt %d/%d): %s",
                            attempt + 1,
                            self.MAX_RETRIES,
                            url,
                        )
                        self._check_block_alert(url, 429)

                    else:
                        self.log.warning("HTTP %d: %s", sc, url)

                except Exception as exc:
                    self.log.debug(
                        "Request failed (attempt %d): %s — %s",
                        attempt + 1,
                        endpoint,
                        exc,
                    )
                    sc = 0  # mark as failed for backoff logic below

            # 3. BACKOFF OUTSIDE LOCK: sleep without blocking other matches
            if attempt < self.MAX_RETRIES - 1:
                if sc == 429:
                    backoff = self.RETRY_BACKOFF[attempt] * 3
                    self.log.info("  ⏳ Rate limit backoff %ds...", backoff)
                    await asyncio.sleep(backoff)
                elif sc == 403:
                    backoff = self.RETRY_BACKOFF[attempt] * 2
                    self.log.info("  ⏳ Backoff %ds before retry...", backoff)
                    await asyncio.sleep(backoff)
                elif sc != 200:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt])

        return None

    async def get_schedule_json(self, date_str: str) -> dict:
        """Fetch scheduled-events for a date with retry.
        Lock scope narrowed: same pattern as get_json."""
        if self.dry_run:
            return {}

        url = f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}"
        sc = 0

        for attempt in range(self.MAX_RETRIES):
            # 1. JITTER OUTSIDE LOCK
            mult = self.antiban.global_sleep_multiplier if self.antiban else 1.0
            jitter = random.uniform(1.5, 3.5) * mult
            await asyncio.sleep(jitter)

            # 2. NARROW LOCK: only the HTTP call
            async with self._lock:
                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=20)
                    sc = resp.status_code

                    # ── anti-ban + metrics ──
                    if self.antiban:
                        self.antiban.record_response(sc, "A")
                        self.antiban.check_timeout()
                        if self.antiban.needs_recycle:
                            self.log.info(
                                "♻️  Anti-ban triggered session recycle (schedule)"
                            )
                            await self.recycle()
                    if self.metrics:
                        self.metrics.record_request("A", sc)

                    if sc == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        return resp.json()
                    elif sc == 403:
                        self._consecutive_403 += 1
                        self.log.warning(
                            "HTTP 403 on schedule fetch (attempt %d): %s",
                            attempt + 1,
                            url,
                        )
                        self._check_block_alert(url, 403)
                    elif sc == 429:
                        self._consecutive_429 += 1
                        self.log.warning(
                            "HTTP 429 on schedule fetch (attempt %d): %s",
                            attempt + 1,
                            url,
                        )
                        self._check_block_alert(url, 429)

                except Exception as exc:
                    self.log.warning(
                        "Schedule fetch failed for %s (attempt %d): %s",
                        date_str,
                        attempt + 1,
                        exc,
                    )
                    sc = 0

            # 3. BACKOFF OUTSIDE LOCK
            if attempt < self.MAX_RETRIES - 1:
                if sc == 429:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 3)
                elif sc == 403:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 2)
                elif sc != 200:
                    await asyncio.sleep(self.RETRY_BACKOFF[attempt])

        return {}


# ════════════════════════════════════════════════════════════
# SCHEDULE MANAGER — fetch matches for ALL leagues at once
# ════════════════════════════════════════════════════════════


class ScheduleManager:
    def __init__(
        self,
        tournament_ids: dict[str, int],
        browser: CurlCffiClient,
        log: logging.Logger,
    ):
        self.tournament_ids = tournament_ids  # {"EPL": 17, "LALIGA": 8, ...}
        self.browser = browser
        self.log = log

    async def get_upcoming(self) -> list[dict]:
        """Fetch today+tomorrow matches for ALL tracked leagues."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")

        all_matches = []
        for date_str in (today, tomorrow):
            self.log.info("Fetching schedule for %s...", date_str)
            data = await self.browser.get_schedule_json(date_str)
            if not data:
                continue

            tracked_ids = set(self.tournament_ids.values())
            for ev in data.get("events", []):
                unique_t = ev.get("tournament", {}).get("uniqueTournament", {})
                t_id = unique_t.get("id")
                if t_id not in tracked_ids:
                    continue

                # Find league name for this tournament
                league = next(
                    (lg for lg, tid in self.tournament_ids.items() if tid == t_id), "?"
                )

                home = ev.get("homeTeam", {})
                away = ev.get("awayTeam", {})
                status = ev.get("status", {})
                hs = ev.get("homeScore", {})
                aws = ev.get("awayScore", {})
                kickoff_ts = ev.get("startTimestamp", 0)

                all_matches.append(
                    {
                        "event_id": ev.get("id"),
                        "league": league,
                        "tournament_id": t_id,
                        "home_team": home.get("name", "?"),
                        "away_team": away.get("name", "?"),
                        "home_score": hs.get("current"),
                        "away_score": aws.get("current"),
                        "status": status.get("type", ""),
                        "kickoff_utc": datetime.fromtimestamp(
                            kickoff_ts, tz=timezone.utc
                        )
                        if kickoff_ts
                        else None,
                        "kickoff_ts": kickoff_ts,
                        "round": ev.get("roundInfo", {}).get("round", 0),
                    }
                )

        # Filter: only notstarted or inprogress, deduplicate, sort
        upcoming = [
            m for m in all_matches if m["status"] in ("notstarted", "inprogress")
        ]
        seen = set()
        unique = []
        for m in upcoming:
            if m["event_id"] not in seen:
                seen.add(m["event_id"])
                unique.append(m)
        unique.sort(key=lambda m: m.get("kickoff_ts", 0))

        self.log.info(
            "Found %d upcoming matches across %d leagues",
            len(unique),
            len(self.tournament_ids),
        )
        for m in unique:
            kt = m["kickoff_utc"].strftime("%H:%M") if m["kickoff_utc"] else "?"
            self.log.info(
                "  • [%s] %s vs %s @ %s UTC [%s]",
                m["league"],
                m["home_team"],
                m["away_team"],
                kt,
                m["status"],
            )

        # Write to DB so frontend can display upcoming matches
        # We only UPSERT the basic info; LiveTrackingPool handles updates later
        self._upsert_upcoming_to_db(unique)

        return unique

    def _upsert_upcoming_to_db(self, upcoming: list[dict]) -> None:
        """Upsert upcoming matches to live_snapshots table."""
        if not upcoming:
            return

        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()

            # Prepare data
            data = []
            for m in upcoming:
                data.append(
                    (
                        m["event_id"],
                        m["home_team"],
                        m["away_team"],
                        m["home_score"] or 0,
                        m["away_score"] or 0,
                        m["status"],
                        0,  # minute
                        json.dumps({}),  # empty statistics
                        json.dumps([]),  # empty incidents
                    )
                )

            # Batch upsert
            cur.executemany(
                """
                INSERT INTO live_snapshots
                    (event_id, home_team, away_team, home_score, away_score,
                     status, minute, statistics_json, incidents_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_team = EXCLUDED.home_team,
                    away_team = EXCLUDED.away_team,
                    status = EXCLUDED.status,
                    loaded_at = NOW()
            """,
                data,
            )
            conn.commit()

        except Exception as e:
            self.log.error("Failed to upsert upcoming match schedule to DB: %s", e)


# ════════════════════════════════════════════════════════════
# LIVE MATCH STATE (from live_match.py)
# ════════════════════════════════════════════════════════════

# Tier C trigger types (plan_live Step 3)
_TIER_C_TRIGGERS = frozenset({"goal", "penalty", "varDecision"})


def _is_tier_c_trigger(inc: dict) -> bool:
    """Return True if incident should trigger a Tier C lineup refresh."""
    inc_type = inc.get("incidentType", "")
    if inc_type in _TIER_C_TRIGGERS:
        return True
    # Red card or second-yellow-to-red
    if inc_type == "card" and inc.get("incidentClass") in ("red", "yellowRed"):
        return True
    # Late substitution (minute >= 70)
    if inc_type == "substitution" and (inc.get("time") or 0) >= 70:
        return True
    return False


@dataclass
class LiveMatchState:
    event_id: int = 0
    league: str = ""
    home_team: str = ""
    away_team: str = ""
    home_score: int = 0
    away_score: int = 0
    status: str = "notstarted"
    minute: int = 0
    incidents: list[dict] = field(default_factory=list)
    statistics: dict[str, dict] = field(default_factory=dict)
    insight_text: str = ""
    poll_count: int = 0
    last_updated: str = ""
    start_timestamp: int = 0
    last_drift_check: float = 0.0  # monotonic time of last drift check
    # Tier C tracking (plan_live Step 3)
    _last_tier_c_ts: float = 0.0
    _tier_c_pending: bool = False
    # Tier B tracking (statistics fetch interval)
    _last_tier_b_ts: float = 0.0


# ════════════════════════════════════════════════════════════
# LIVE TRACKING POOL — round-robin polling, 1 browser
# ════════════════════════════════════════════════════════════


class LiveTrackingPool:
    """Tracks multiple concurrent matches via round-robin polling on HTTP client."""

    _DRIFT_INTERVAL = 300  # 5 minutes between drift checks

    def __init__(
        self,
        browser: CurlCffiClient,
        log: logging.Logger,
        notifier: Notifier,
        *,
        dry_run: bool = False,
    ):
        self.browser = browser
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._matches: dict[int, LiveMatchState] = {}
        self.live_drift_mismatch_count: int = 0

    @property
    def is_empty(self) -> bool:
        return len(self._matches) == 0

    @property
    def active_count(self) -> int:
        return len(self._matches)

    def add_match(self, match: dict) -> None:
        eid = match["event_id"]
        if eid in self._matches:
            return
        self._matches[eid] = LiveMatchState(
            event_id=eid,
            league=match.get("league", ""),
            home_team=match["home_team"],
            away_team=match["away_team"],
            status=match["status"],
            start_timestamp=match.get("kickoff_ts", 0),
        )
        self.log.info(
            "➕ Added to pool: [%s] %s vs %s (event=%d)",
            match.get("league", "?"),
            match["home_team"],
            match["away_team"],
            eid,
        )
        self.notifier.send(
            "match_start",
            f"[{match.get('league', '?')}] {match['home_team']} vs {match['away_team']} — tracking started",
        )

    async def poll_all(self) -> list[dict]:
        """
        Poll all active matches once (round-robin).
        Returns list of matches that just finished.
        """
        finished = []
        event_ids = list(self._matches.keys())

        for eid in event_ids:
            state = self._matches.get(eid)
            if not state:
                continue

            still_playing = await self._poll_one(state)

            if not still_playing:
                self.log.info(
                    "🏁 Match finished: [%s] %s %d-%d %s",
                    state.league,
                    state.home_team,
                    state.home_score,
                    state.away_score,
                    state.away_team,
                )
                self.notifier.send(
                    "match_end",
                    f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}",
                )

                # ── Post-match flush (plan_live Step 4) ──
                flush_ok = await self._final_flush(state)
                self._save_to_db(state, flush_incomplete=not flush_ok)

                finished.append(
                    {
                        "event_id": eid,
                        "league": state.league,
                        "home_team": state.home_team,
                        "away_team": state.away_team,
                    }
                )
                del self._matches[eid]

            # Smooth pacing: spread polling evenly across 60 seconds
            if len(event_ids) > 1:
                per_match_interval = max(60.0 / len(event_ids), 3.0)
                await asyncio.sleep(per_match_interval)

        return finished

    async def _final_flush(self, state: LiveMatchState) -> bool:
        """Final pull sequence on match finish (plan_live Step 4).
        Returns True if all endpoints fetched successfully."""
        _RETRY_DELAYS = (10, 30, 60)
        endpoints = {
            "event": f"/event/{state.event_id}",
            "incidents": f"/event/{state.event_id}/incidents",
            "statistics": f"/event/{state.event_id}/statistics",
            "lineups": f"/event/{state.event_id}/lineups",
        }
        failed = set(endpoints.keys())

        for delay_idx in range(len(_RETRY_DELAYS) + 1):
            still_failed = set()
            for name in list(failed):
                tier = (
                    "C" if name == "lineups" else ("B" if name == "statistics" else "A")
                )
                data = await self.browser.get_json(endpoints[name], tier=tier)
                if data:
                    # Apply data to state
                    if name == "event":
                        ev = data.get("event", data)
                        state.home_score = (
                            ev.get("homeScore", {}).get("current", state.home_score)
                            or 0
                        )
                        state.away_score = (
                            ev.get("awayScore", {}).get("current", state.away_score)
                            or 0
                        )
                        state.status = ev.get("status", {}).get("type", state.status)
                    elif name == "incidents":
                        state.incidents = data.get("incidents", state.incidents)
                    elif name == "statistics":
                        stats = {}
                        for period in data.get("statistics", []):
                            if period.get("period") != "ALL":
                                continue
                            for group in period.get("groups", []):
                                for item in group.get("statisticsItems", []):
                                    stats[item.get("name", "")] = {
                                        "home": item.get("home", ""),
                                        "away": item.get("away", ""),
                                    }
                        if stats:
                            state.statistics = stats
                    elif name == "lineups":
                        self._upsert_lineup_from_data(state, data)
                else:
                    still_failed.add(name)

            failed = still_failed
            if not failed:
                self.log.info("  ✅ Final flush complete for event %d", state.event_id)
                return True
            if delay_idx < len(_RETRY_DELAYS):
                delay = _RETRY_DELAYS[delay_idx]
                self.log.info(
                    "  ⏳ Final flush retry in %ds (missing: %s)",
                    delay,
                    ", ".join(sorted(failed)),
                )
                await asyncio.sleep(delay)

        self.log.warning(
            "  ⚠ Final flush incomplete for event %d (missing: %s)",
            state.event_id,
            ", ".join(sorted(failed)),
        )
        return False

    async def _poll_one(self, state: LiveMatchState) -> bool:
        """Poll a single match. Returns True if still in progress."""
        state.poll_count += 1
        state.last_updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        if self.dry_run:
            self.log.info(
                "[DRY-RUN] Poll #%d: %s vs %s",
                state.poll_count,
                state.home_team,
                state.away_team,
            )
            return True

        # ── Resolve anti-ban state machine (may be None) ──
        ab = self.browser.antiban
        metrics = self.browser.metrics
        now_mono = time.monotonic()

        # ── 0-15 min fast-poll guard ──
        if state.start_timestamp:
            elapsed_match_min = max(
                0,
                (int(datetime.now(timezone.utc).timestamp()) - state.start_timestamp)
                // 60,
            )
        else:
            elapsed_match_min = state.minute

        # In first 15 min: if antiban says no fast poll, skip every other poll
        if elapsed_match_min < 15 and ab and not ab.should_allow_fast_poll():
            if state.poll_count % 2 == 0:
                self.log.debug(
                    "⏸️ 0-15min guard: skipping poll #%d for %s vs %s",
                    state.poll_count,
                    state.home_team,
                    state.away_team,
                )
                if metrics:
                    metrics.record_skip()
                return True

        # 1. Match info (Tier A — always)
        info = await self.browser.get_json(f"/event/{state.event_id}", tier="A")
        if info:
            ev = info.get("event", info)
            hs = ev.get("homeScore", {})
            aws = ev.get("awayScore", {})
            st = ev.get("status", {})

            old_score = (state.home_score, state.away_score)
            state.home_team = ev.get("homeTeam", {}).get("name", state.home_team)
            state.away_team = ev.get("awayTeam", {}).get("name", state.away_team)
            state.home_score = hs.get("current", state.home_score) or 0
            state.away_score = aws.get("current", state.away_score) or 0
            state.status = st.get("type", state.status)
            state.start_timestamp = ev.get("startTimestamp", state.start_timestamp)

            new_score = (state.home_score, state.away_score)
            if new_score != old_score and state.poll_count > 1:
                self.notifier.send(
                    "goal",
                    f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}",
                )

            # Calculate minute
            if state.status == "inprogress" and state.start_timestamp:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                state.minute = max(0, min(120, (now_ts - state.start_timestamp) // 60))

        # 2. Incidents (Tier A — always)
        inc_data = await self.browser.get_json(
            f"/event/{state.event_id}/incidents", tier="A"
        )
        # FIX Issue #1: Save old_incidents BEFORE updating state.incidents
        # so both Discord alerts AND Tier C detection use the correct old list.
        old_incidents = state.incidents  # snapshot before overwrite

        if inc_data:
            current_incidents = inc_data.get("incidents", [])

            # Detect new incidents for Discord alerts (using old_incidents)
            if state.poll_count > 1 and old_incidents:
                old_ids = {i.get("id") for i in old_incidents if i.get("id")}
                for inc in current_incidents:
                    iid = inc.get("id")
                    if iid and iid not in old_ids:
                        inc_type = inc.get("incidentType", "")
                        inc_class = inc.get("incidentClass", "")
                        player = inc.get("player", {}).get("name", "")
                        time_str = f"{inc.get('time', '?')}'"
                        if inc.get("addedTime"):
                            time_str += f"+{inc.get('addedTime')}'"

                        if inc_type == "card" and inc_class in ("red", "yellowRed"):
                            self.notifier.send(
                                "match_event",
                                f"🟥 **RED CARD** [{state.league}] {state.home_team} vs {state.away_team} | {player} ({time_str})",
                            )
                        elif inc_type == "varDecision":
                            self.notifier.send(
                                "match_event",
                                f"📺 **VAR DECISION** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}",
                            )
                        elif inc_type == "penalty":
                            self.notifier.send(
                                "match_event",
                                f"🎯 **PENALTY** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}",
                            )

            state.incidents = current_incidents

        # 3. Statistics — Tier B (interval driven by anti-ban state + capacity)
        n_active = len(self._matches)
        tier_b_interval = ab.capacity_adjusted_interval("B", n_active) if ab else 180
        should_fetch_stats = (tier_b_interval > 0) and (
            now_mono - state._last_tier_b_ts >= tier_b_interval
        )
        if should_fetch_stats:
            stat_data = await self.browser.get_json(
                f"/event/{state.event_id}/statistics", tier="B"
            )
            if stat_data:
                stats = {}
                for period in stat_data.get("statistics", []):
                    if period.get("period") != "ALL":
                        continue
                    for group in period.get("groups", []):
                        for item in group.get("statisticsItems", []):
                            stats[item.get("name", "")] = {
                                "home": item.get("home", ""),
                                "away": item.get("away", ""),
                            }
                state.statistics = stats
            state._last_tier_b_ts = now_mono

        # 3b. Lineups — Tier C (event-driven with cooldown, plan_live Step 3)
        tier_c_cooldown = ab.tier_c_cooldown() if ab else 90
        tier_c_fetched = False
        if tier_c_cooldown > 0:  # 0 = Tier C paused
            # FIX Issue #1: Detect triggers using old_incidents (saved before overwrite)
            if inc_data and state.poll_count > 1 and old_incidents:
                old_ids = {i.get("id") for i in old_incidents if i.get("id")}
                for inc in inc_data.get("incidents") or []:
                    iid = inc.get("id")
                    if iid and iid not in old_ids and _is_tier_c_trigger(inc):
                        state._tier_c_pending = True
                        break

            # Respect per-match cooldown
            elapsed_c = now_mono - state._last_tier_c_ts
            # Capacity hard cap for >10 matches
            if ab and n_active > 10:
                tier_c_cooldown = max(tier_c_cooldown, 480)
            if state._tier_c_pending and elapsed_c >= tier_c_cooldown:
                lineup_data = await self.browser.get_json(
                    f"/event/{state.event_id}/lineups", tier="C"
                )
                if lineup_data:
                    self._upsert_lineup_from_data(state, lineup_data)
                    tier_c_fetched = True
                state._tier_c_pending = False
                state._last_tier_c_ts = now_mono

        # 4. Momentum Analysis & Insights
        # Generate insight only if we just fetched fresh stats
        if should_fetch_stats and state.statistics:
            score, insight = live_insight.analyze(
                home_team=state.home_team,
                away_team=state.away_team,
                minute=state.minute,
                home_score=state.home_score,
                away_score=state.away_score,
                statistics=state.statistics,
                incidents=state.incidents,
            )
            if insight:
                # Log insight on discord if it changes
                if insight != state.insight_text:
                    self.notifier.send(
                        "live",
                        f"💡 [INSIGHT] {state.league} ({state.home_team} vs {state.away_team}): {insight}",
                    )
                state.insight_text = insight

            # Phase B (shadow): enqueue live_badge job for AI pipeline
            if score > 0:
                try:
                    insight_producer.enqueue_live_badge(
                        event_id=state.event_id,
                        league_id=state.league,
                        home_team=state.home_team,
                        away_team=state.away_team,
                        home_score=state.home_score,
                        away_score=state.away_score,
                        minute=state.minute,
                        statistics=state.statistics,
                        incidents=state.incidents,
                        momentum_score=score,
                    )
                except Exception as exc:
                    self.log.debug("Insight producer error: %s", exc)

        # 5. Save to DB every poll
        self._save_to_db(state)

        # 6. Drift guard: compare live_match_state vs live_snapshots
        drift_now = time.monotonic()
        should_check_drift = False
        if state.status == "finished":
            should_check_drift = True  # mandatory at finish
        elif state.status == "inprogress":
            if drift_now - state.last_drift_check >= self._DRIFT_INTERVAL:
                should_check_drift = True
        if should_check_drift:
            self._check_drift(state)
            state.last_drift_check = drift_now

        # 7. Metrics emit (every 60s)
        if metrics:
            ab_state = ab.state if ab else AntiBanState.NORMAL
            metrics.maybe_emit(len(self._matches), ab_state)

        self.log.info(
            "  📊 [%s] %s %d-%d %s | %s' | poll #%d | %s",
            state.league,
            state.home_team,
            state.home_score,
            state.away_score,
            state.away_team,
            state.minute,
            state.poll_count,
            ab.state.value if ab else "NORMAL",
        )

        return state.status != "finished"

    def _check_drift(self, state: LiveMatchState) -> None:
        """Compare live_match_state vs live_snapshots for key fields.
        Emits ERROR log + increments metric counter on mismatch.
        Never raises — polling loop must stay alive."""
        from db.config_db import get_connection

        conn = None
        cur = None
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute(
                """
                SELECT ls.home_score, ls.away_score, ls.status, ls.minute,
                       lm.home_score, lm.away_score, lm.status, lm.minute
                FROM live_snapshots ls
                JOIN live_match_state lm ON lm.event_id = ls.event_id
                WHERE ls.event_id = %s
            """,
                (state.event_id,),
            )
            row = cur.fetchone()
            if row is None:
                return  # one side missing, skip (first write race)
            ls_hs, ls_as, ls_st, ls_min = row[0], row[1], row[2], row[3]
            lm_hs, lm_as, lm_st, lm_min = row[4], row[5], row[6], row[7]
            diffs = []
            if ls_hs != lm_hs:
                diffs.append(f"home_score snap={ls_hs} state={lm_hs}")
            if ls_as != lm_as:
                diffs.append(f"away_score snap={ls_as} state={lm_as}")
            if ls_st != lm_st:
                diffs.append(f"status snap={ls_st} state={lm_st}")
            if ls_min != lm_min:
                diffs.append(f"minute snap={ls_min} state={lm_min}")
            if diffs:
                self.live_drift_mismatch_count += 1
                self.log.error(
                    "DRIFT MISMATCH event=%d [%s] %s vs %s: %s (total_mismatches=%d)",
                    state.event_id,
                    state.league,
                    state.home_team,
                    state.away_team,
                    "; ".join(diffs),
                    self.live_drift_mismatch_count,
                )
        except Exception as exc:
            self.log.warning("Drift check failed for event %d: %s", state.event_id, exc)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    def _save_to_db(
        self, state: LiveMatchState, *, flush_incomplete: bool = False
    ) -> None:
        """Upsert to live_snapshots + live_match_state + live_incidents (dual-write)."""
        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()

            # ── 1. Legacy: live_snapshots (unchanged) ──
            cur.execute(
                """
                INSERT INTO live_snapshots
                    (event_id, home_team, away_team, home_score, away_score,
                     status, minute, statistics_json, incidents_json, insight_text, poll_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    status = EXCLUDED.status,
                    minute = EXCLUDED.minute,
                    statistics_json = EXCLUDED.statistics_json,
                    incidents_json = EXCLUDED.incidents_json,
                    insight_text = EXCLUDED.insight_text,
                    poll_count = EXCLUDED.poll_count,
                    loaded_at = NOW()
            """,
                (
                    state.event_id,
                    state.home_team,
                    state.away_team,
                    state.home_score,
                    state.away_score,
                    state.status,
                    state.minute,
                    json.dumps(state.statistics, ensure_ascii=False),
                    json.dumps(state.incidents, ensure_ascii=False),
                    state.insight_text,
                    state.poll_count,
                ),
            )

            # ── 2. Incidents (seq auto-increments) ──
            for inc in state.incidents:
                inc_type = inc.get("incidentType", "")
                if inc_type not in ("goal", "card", "substitution", "varDecision"):
                    continue
                cur.execute(
                    """
                    INSERT INTO live_incidents
                        (event_id, incident_type, minute, added_time,
                         player_name, player_in_name, player_out_name,
                         is_home, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, incident_type, minute,
                                 COALESCE(added_time, -1),
                                 COALESCE(player_name, ''), is_home)
                    DO UPDATE SET
                        added_time     = EXCLUDED.added_time,
                        player_in_name = EXCLUDED.player_in_name,
                        player_out_name= EXCLUDED.player_out_name,
                        is_home        = EXCLUDED.is_home,
                        detail         = EXCLUDED.detail,
                        loaded_at      = NOW()
                """,
                    (
                        state.event_id,
                        inc_type,
                        inc.get("time"),
                        inc.get("addedTime"),
                        inc.get("player", {}).get("name"),
                        inc.get("playerIn", {}).get("name"),
                        inc.get("playerOut", {}).get("name"),
                        inc.get("isHome"),
                        inc.get("incidentClass", ""),
                    ),
                )

            # ── 3. New: live_match_state (lightweight, no blob) ──
            # Build stats_core_json: only UI-critical subset.
            stats_core = {}
            _CORE_KEYS = (
                "Ball possession",
                "Shots on target",
                "Expected goals",
                "Dangerous attacks",
            )
            for key in _CORE_KEYS:
                if key in state.statistics:
                    stats_core[key] = state.statistics[key]
            stats_core_json = (
                json.dumps(stats_core, ensure_ascii=False) if stats_core else None
            )

            # Compute last processed seq after incident upserts so cursor is current.
            cur.execute(
                "SELECT MAX(seq) FROM live_incidents WHERE event_id = %s",
                (state.event_id,),
            )
            max_seq = (cur.fetchone() or (None,))[0]

            cur.execute(
                """
                INSERT INTO live_match_state
                    (event_id, league_id, home_team, away_team, home_score, away_score,
                     status, minute, poll_count, insight_text, stats_core_json,
                     last_processed_seq, flush_incomplete)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_score         = EXCLUDED.home_score,
                    away_score         = EXCLUDED.away_score,
                    status             = EXCLUDED.status,
                    minute             = EXCLUDED.minute,
                    poll_count         = EXCLUDED.poll_count,
                    insight_text       = EXCLUDED.insight_text,
                    stats_core_json    = EXCLUDED.stats_core_json,
                    last_processed_seq = EXCLUDED.last_processed_seq,
                    flush_incomplete   = EXCLUDED.flush_incomplete,
                    loaded_at          = NOW()
            """,
                (
                    state.event_id,
                    state.league,
                    state.home_team,
                    state.away_team,
                    state.home_score,
                    state.away_score,
                    state.status,
                    state.minute,
                    state.poll_count,
                    state.insight_text,
                    stats_core_json,
                    max_seq,
                    flush_incomplete,
                ),
            )

            conn.commit()
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("DB save failed for event %d: %s", state.event_id, exc)

    def _upsert_lineup_from_data(self, state: LiveMatchState, data: dict) -> None:
        """Upsert lineup rows from Tier C /lineups response (plan_live Step 3)."""
        try:
            import psycopg2.extras

            from db.config_db import get_connection

            rows = []
            for side in ("home", "away"):
                lineup_data = data.get(side, {})
                formation = lineup_data.get("formation")
                team_name = state.home_team if side == "home" else state.away_team
                for p in lineup_data.get("players", []):
                    pi = p.get("player", {})
                    stats = p.get("statistics", {})
                    pid = pi.get("id")
                    if not pid:
                        continue
                    rows.append(
                        (
                            state.event_id,
                            pid,
                            pi.get("name") or pi.get("shortName"),
                            side,
                            team_name,
                            pi.get("position"),
                            pi.get("jerseyNumber"),
                            p.get("substitute", False),
                            stats.get("minutesPlayed"),
                            stats.get("rating"),
                            formation,
                            "confirmed",
                            state.league,
                            "",
                        )
                    )
            if not rows:
                return

            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(
                        cur,
                        """
                        INSERT INTO match_lineups
                            (event_id, player_id, player_name, team_side, team_name,
                             position, jersey_number, is_substitute, minutes_played,
                             rating, formation, status, league_id, season)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id, player_id, league_id) DO UPDATE SET
                            minutes_played = COALESCE(EXCLUDED.minutes_played, match_lineups.minutes_played),
                            rating = COALESCE(EXCLUDED.rating, match_lineups.rating),
                            is_substitute = EXCLUDED.is_substitute,
                            formation = EXCLUDED.formation,
                            loaded_at = NOW()
                    """,
                        rows,
                        page_size=100,
                    )
                conn.commit()
                self.log.info(
                    "  📋 Tier C lineup refresh: %s vs %s — %d players",
                    state.home_team,
                    state.away_team,
                    len(rows),
                )
            finally:
                conn.close()
        except Exception as exc:
            self.log.warning(
                "Tier C lineup upsert failed for event %d: %s", state.event_id, exc
            )


# ════════════════════════════════════════════════════════════
# SUBPROCESS RUNNER — with retry logic
# ════════════════════════════════════════════════════════════


def run_with_retry(
    cmd: list[str],
    cwd: Path,
    label: str,
    log: logging.Logger,
    *,
    dry_run: bool = False,
    timeout: int = 7200,
    shutdown_event: asyncio.Event | None = None,
) -> bool:
    if dry_run:
        log.info("[DRY-RUN] %s", label)
        return True
    for attempt in range(MAX_ATTEMPTS):
        if shutdown_event and shutdown_event.is_set():
            log.info("⏹ %s — aborted (shutdown)", label)
            return False
        log.info("▶ %s (attempt %d/%d)", label, attempt + 1, MAX_ATTEMPTS)
        t0 = time.perf_counter()
        try:
            child_env = os.environ.copy()
            child_env.setdefault("PYTHONUTF8", "1")
            child_env.setdefault("PYTHONIOENCODING", "utf-8")

            with (
                tempfile.NamedTemporaryFile(
                    mode="w+", encoding="utf-8", errors="replace", delete=False
                ) as out_f,
                tempfile.NamedTemporaryFile(
                    mode="w+", encoding="utf-8", errors="replace", delete=False
                ) as err_f,
            ):
                out_path = out_f.name
                err_path = err_f.name

                proc = subprocess.Popen(
                    cmd,
                    cwd=str(cwd),
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    env=child_env,
                )

                timed_out = False
                next_heartbeat = t0 + 30
                while True:
                    if shutdown_event and shutdown_event.is_set():
                        log.info("⏹ %s — terminating child process (shutdown)", label)
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                        return False

                    if (time.monotonic() - t0) >= timeout:
                        timed_out = True
                        proc.terminate()
                        try:
                            proc.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=5)
                        break

                    try:
                        proc.wait(timeout=2)
                        break
                    except subprocess.TimeoutExpired:
                        now = time.monotonic()
                        if now >= next_heartbeat:
                            log.info(
                                "… %s still running (%.0fs elapsed)", label, now - t0
                            )
                            next_heartbeat = now + 30
                        continue

            stdout = ""
            stderr = ""
            try:
                with open(out_path, "r", encoding="utf-8", errors="replace") as f:
                    stdout = f.read()
            except Exception:
                pass
            try:
                with open(err_path, "r", encoding="utf-8", errors="replace") as f:
                    stderr = f.read()
            except Exception:
                pass
            try:
                os.remove(out_path)
            except Exception:
                pass
            try:
                os.remove(err_path)
            except Exception:
                pass

            if timed_out:
                log.warning("✗ %s — timeout", label)
            elif proc.returncode == 0:
                log.info("✓ %s — %.1fs", label, time.perf_counter() - t0)
                return True
            else:
                log.warning(
                    "✗ %s — exit code %d. Error details:", label, proc.returncode
                )
            if stderr:
                for line in stderr.strip().split("\n"):
                    log.warning("  | %s", line)
        except subprocess.TimeoutExpired:
            log.warning("✗ %s — timeout", label)
        except Exception as exc:
            log.warning("✗ %s — %s", label, exc)
        if attempt < MAX_ATTEMPTS - 1:
            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            log.info("  ⏳ Retrying in %ds...", wait)
            # Interruptible backoff sleep
            deadline = time.monotonic() + wait
            while time.monotonic() < deadline:
                if shutdown_event and shutdown_event.is_set():
                    log.info("⏹ %s — aborted during backoff (shutdown)", label)
                    return False
                time.sleep(min(5, max(0, deadline - time.monotonic())))
    log.error("✗ %s — FAILED after %d attempts", label, MAX_ATTEMPTS)
    return False


# ════════════════════════════════════════════════════════════
# POST-MATCH WORKER
# ════════════════════════════════════════════════════════════


class PostMatchWorker:
    def __init__(
        self,
        log: logging.Logger,
        notifier: Notifier,
        browser: CurlCffiClient | None = None,
        *,
        dry_run: bool = False,
        shutdown_event: asyncio.Event | None = None,
    ):
        self.log = log
        self.notifier = notifier
        self.browser = browser
        self.dry_run = dry_run
        self._shutdown = shutdown_event

    def run(self, match: dict) -> bool:
        league = match["league"]
        home, away = match["home_team"], match["away_team"]
        self.log.info("─" * 50)
        self.log.info("POST-MATCH: [%s] %s vs %s", league, home, away)

        sources = LEAGUE_SOURCES.get(league, {})
        ok = True

        if sources.get("understat"):
            ok &= run_with_retry(
                [PYTHON, "async_scraper.py", "--league", league],
                ROOT / "understat",
                f"PostMatch/Understat [{league}]",
                self.log,
                dry_run=self.dry_run,
                shutdown_event=self._shutdown,
            )

        if sources.get("sofascore"):
            ok &= run_with_retry(
                [
                    PYTHON,
                    "sofascore_client.py",
                    "--league",
                    league,
                    "--match-limit",
                    "5",
                ],
                ROOT / "sofascore",
                f"PostMatch/SofaScore [{league}]",
                self.log,
                dry_run=self.dry_run,
                shutdown_event=self._shutdown,
            )

        ok &= run_with_retry(
            [PYTHON, "-m", "db.loader", "--league", league],
            ROOT,
            f"PostMatch/DBLoad [{league}]",
            self.log,
            dry_run=self.dry_run,
            shutdown_event=self._shutdown,
        )

        # Generate 30-second match story via AI
        self._generate_match_story(match)

        # Update standings from SofaScore API (no Chrome needed!)
        if self.browser:
            tournament_id = TOURNAMENT_IDS.get(league)
            if tournament_id:
                try:
                    import asyncio

                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # Fire-and-forget with error boundary wrapper
                        task = asyncio.ensure_future(
                            self._run_standings_safe(league, tournament_id)
                        )

                        # Prevent "Task exception was never retrieved" warning
                        def _swallow_task_exc(t: asyncio.Task) -> None:
                            if not t.cancelled():
                                t.exception()  # mark as retrieved, không re-raise

                        task.add_done_callback(_swallow_task_exc)
                    else:
                        loop.run_until_complete(
                            self._run_standings_safe(league, tournament_id)
                        )
                except Exception as exc:
                    self.log.warning("Standings update scheduling failed: %s", exc)

        if ok:
            self.notifier.send(
                "post_match_done", f"[{league}] Post-match done: {home} vs {away}"
            )
        else:
            self.notifier.send(
                "error", f"[{league}] Post-match errors: {home} vs {away}"
            )
        return ok

    def _generate_match_story(self, match: dict) -> None:
        """Generate a 30-second AI match story and save to DB.
        Phase D: also enqueues a match_story pipeline job."""
        try:
            from db.config_db import get_connection
            from services import match_story

            event_id = match.get("event_id")
            if not event_id:
                return

            # Read live_snapshots for stats/incidents
            statistics = {}
            incidents = []
            try:
                conn = get_connection()
                cur = conn.cursor()
                cur.execute(
                    "SELECT statistics_json, incidents_json, home_score, away_score "
                    "FROM live_snapshots WHERE event_id = %s",
                    (event_id,),
                )
                row = cur.fetchone()
                cur.close()
                conn.close()
                if row:
                    statistics = (
                        row[0]
                        if isinstance(row[0], dict)
                        else json.loads(row[0] or "{}")
                    )
                    incidents = (
                        row[1]
                        if isinstance(row[1], list)
                        else json.loads(row[1] or "[]")
                    )
                    # Use DB scores if available (more accurate final score)
                    match["home_score"] = row[2] or match.get("home_score", 0)
                    match["away_score"] = row[3] or match.get("away_score", 0)
            except Exception as exc:
                self.log.warning("Could not read live_snapshots for story: %s", exc)

            ok = match_story.generate_and_save(
                event_id=event_id,
                league=match["league"],
                home_team=match["home_team"],
                away_team=match["away_team"],
                home_score=match.get("home_score", 0),
                away_score=match.get("away_score", 0),
                statistics=statistics,
                incidents=incidents,
            )
            if ok:
                self.notifier.send(
                    "info",
                    f"📝 [{match['league']}] Match story generated: "
                    f"{match['home_team']} vs {match['away_team']}",
                )

            # Phase D: enqueue match_story pipeline job (shadow mode)
            try:
                insight_producer.enqueue_match_story(
                    event_id=event_id,
                    league_id=match["league"],
                    home_team=match["home_team"],
                    away_team=match["away_team"],
                    home_score=match.get("home_score", 0),
                    away_score=match.get("away_score", 0),
                    statistics=statistics,
                    incidents=incidents,
                )
            except Exception as exc:
                self.log.debug("Match story pipeline enqueue error: %s", exc)

        except Exception as exc:
            self.log.warning("Match story generation failed: %s", exc)

    async def _run_standings_safe(self, league: str, tournament_id: int) -> None:
        """
        Safe wrapper cho standings update với error boundary.
        Log rõ + Discord alert thay vì để exception biến mất im lặng.
        """
        try:
            await self._update_standings_from_sofascore(league, tournament_id)
            self.log.info("✓ Standings updated: %s", league)
        except Exception as exc:
            # Log đủ thông tin để debug: league, tournament_id, error message
            self.log.error(
                "⚠ Standings update FAILED — league=%s tournament_id=%d — %s",
                league,
                tournament_id,
                exc,
            )
            # Discord alert để người vận hành biết cần manual check
            self.notifier.send(
                "error",
                f"⚠ Standings update failed: `{league}` (tournament={tournament_id})\n"
                f"```{exc}```\n"
                f"Manual trigger: `python run_pipeline.py --league {league} --load-only`",
            )

    async def _update_standings_from_sofascore(
        self, league: str, tournament_id: int
    ) -> None:
        """Fetch latest standings from SofaScore API and upsert into DB."""
        self.log.info("📊 Fetching standings for %s from SofaScore API...", league)

        # Step 1: Get current season ID
        seasons_data = await self.browser.get_json(
            f"/unique-tournament/{tournament_id}/seasons"
        )
        if not seasons_data:
            self.log.warning("Could not fetch seasons for %s", league)
            return

        seasons = seasons_data.get("seasons", [])
        if not seasons:
            self.log.warning("No seasons found for %s", league)
            return

        season_id = seasons[0]["id"]
        season_name = seasons[0].get("name", "?")

        # Step 2: Get standings
        standings_data = await self.browser.get_json(
            f"/unique-tournament/{tournament_id}/season/{season_id}/standings/total"
        )
        if not standings_data:
            self.log.warning("Could not fetch standings for %s", league)
            return

        all_standings = standings_data.get("standings", [])
        if not all_standings:
            return

        rows = all_standings[0].get("rows", [])
        if not rows:
            return

        # Step 3: Map to DB schema and upsert
        # Convert season name "Premier League 25/26" -> "2025-2026"
        season_str = season_name
        import re

        m = re.search(r"(\d{2})/(\d{2})$", season_name)
        if m:
            y1, y2 = int(m.group(1)), int(m.group(2))
            season_str = f"20{y1}-20{y2}"

        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()

            for row in rows:
                team = row.get("team", {})
                team_id = str(team.get("id", ""))
                team_name = team.get("name", "?")
                position = row.get("position", 0)
                matches_played = row.get("matches", 0)
                wins = row.get("wins", 0)
                draws = row.get("draws", 0)
                losses = row.get("losses", 0)
                goals_for = row.get("scoresFor", 0)
                goals_against = row.get("scoresAgainst", 0)
                goal_diff = goals_for - goals_against
                points = row.get("points", 0)
                points_avg = round(points / max(matches_played, 1), 2)

                cur.execute(
                    """
                    INSERT INTO standings
                        (position, team_name, team_id, matches_played, wins, draws, losses,
                         goals_for, goals_against, goal_difference, points, points_avg,
                         league_id, season)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (team_id, league_id, season) DO UPDATE SET
                        position = EXCLUDED.position,
                        team_name = EXCLUDED.team_name,
                        matches_played = EXCLUDED.matches_played,
                        wins = EXCLUDED.wins,
                        draws = EXCLUDED.draws,
                        losses = EXCLUDED.losses,
                        goals_for = EXCLUDED.goals_for,
                        goals_against = EXCLUDED.goals_against,
                        goal_difference = EXCLUDED.goal_difference,
                        points = EXCLUDED.points,
                        points_avg = EXCLUDED.points_avg,
                        loaded_at = NOW()
                """,
                    (
                        position,
                        team_name,
                        team_id,
                        matches_played,
                        wins,
                        draws,
                        losses,
                        goals_for,
                        goals_against,
                        goal_diff,
                        points,
                        points_avg,
                        league,
                        season_str,
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()

            self.log.info(
                "✅ Standings updated: %s — %d teams (%s)",
                league,
                len(rows),
                season_str,
            )

            # Send top 5 to Discord
            top5 = sorted(rows, key=lambda r: r.get("position", 99))[:5]
            lines = [f"📊 **BXH {league}** (sau vòng đấu):"]
            for r in top5:
                t = r.get("team", {}).get("name", "?")
                p = r.get("position", 0)
                pts = r.get("points", 0)
                w, d, l = r.get("wins", 0), r.get("draws", 0), r.get("losses", 0)
                lines.append(f"  #{p} {t} — {pts}pts ({w}W {d}D {l}L)")
            self.notifier.send("info", "\n".join(lines))

        except Exception as exc:
            self.log.error("Failed to upsert standings for %s: %s", league, exc)


# ════════════════════════════════════════════════════════════
# DAILY MAINTENANCE — with barrier gate
# ════════════════════════════════════════════════════════════


class DailyMaintenance:
    def __init__(
        self,
        leagues: list[str],
        log: logging.Logger,
        notifier: Notifier,
        *,
        dry_run: bool = False,
        shutdown_event: asyncio.Event | None = None,
        skip_crossref_build: bool = False,
        task_timeout_seconds: int = 7200,
        fbref_standings_only: bool = False,
        fbref_no_match_passing: bool = False,
        fbref_team_limit: int = 0,
        fbref_match_limit: int = 0,
        skip_ai_trend: bool = False,
    ):
        self.leagues = leagues
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._shutdown = shutdown_event
        self.skip_crossref_build = skip_crossref_build
        self.task_timeout_seconds = max(60, int(task_timeout_seconds))
        self.fbref_standings_only = fbref_standings_only
        self.fbref_no_match_passing = fbref_no_match_passing
        self.fbref_team_limit = max(0, int(fbref_team_limit))
        self.fbref_match_limit = max(0, int(fbref_match_limit))
        self.skip_ai_trend = skip_ai_trend
        self.last_run_date: str | None = None

    def is_due(self) -> bool:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self.last_run_date != today

    def run(self) -> bool:
        self.log.info("═" * 50)
        self.log.info("DAILY MAINTENANCE (all leagues)")
        self.log.info("═" * 50)

        # Window 3 ngày: safety margin cho 2 trường hợp thực tế:
        #   1. Nếu daily maintenance bị skip 1 ngày (crash, restart, maintenance window)
        #      → ngày đó sẽ không bị miss.
        #   2. FBref thường cập nhật số liệu sau trận vài tiếng, đôi khi qua ngày hôm sau
        #      mới có đầy đủ data → cần re-fetch ngày hôm qua lần nữa.
        # Cost thêm không đáng kể so với việc quét cả mùa (~3 ngày vs ~9 tháng).
        _LOOKBACK_DAYS = 3
        since_date = (
            datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
        ).strftime("%Y-%m-%d")
        self.log.info("  since_date: %s (lookback=%d days)", since_date, _LOOKBACK_DAYS)

        ok = True
        for league in self.leagues:
            if self._shutdown and self._shutdown.is_set():
                self.log.info("⏹ Daily maintenance aborted (shutdown)")
                return False
            sources = LEAGUE_SOURCES.get(league, {})

            # Understat: limit 10 trận gần nhất (daily compensate missed matches)
            if sources.get("understat"):
                ok &= run_with_retry(
                    [PYTHON, "async_scraper.py", "--league", league, "--limit", "10"],
                    ROOT / "understat",
                    f"Daily/Understat [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )

            # SofaScore: limit 10 trận (lineup + passing + advanced stats)
            if sources.get("sofascore"):
                ok &= run_with_retry(
                    [
                        PYTHON,
                        "sofascore_client.py",
                        "--league",
                        league,
                        "--match-limit",
                        "10",
                    ],
                    ROOT / "sofascore",
                    f"Daily/SofaScore [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )

            if sources.get("fbref"):
                fbref_cmd = [PYTHON, "fbref_scraper.py", "--league", league]
                if self.fbref_standings_only:
                    fbref_cmd.append("--standings-only")
                    # --standings-only không chạy STEP 4 (match reports) nên
                    # --since-date không có tác dụng → không cần append.
                else:
                    if self.fbref_no_match_passing:
                        fbref_cmd.append("--no-match-passing")
                    else:
                        # Giới hạn match reports theo ngày để tránh re-scrape cả mùa.
                        # since_date = now - 3 days, computed once at top of run().
                        fbref_cmd.extend(["--since-date", since_date])
                    if self.fbref_team_limit > 0:
                        fbref_cmd.extend(["--limit", str(self.fbref_team_limit)])
                    if self.fbref_match_limit > 0:
                        fbref_cmd.extend(["--match-limit", str(self.fbref_match_limit)])
                ok &= run_with_retry(
                    fbref_cmd,
                    ROOT / "fbref",
                    f"Daily/FBref [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )
            if sources.get("transfermarkt"):
                # Daily TM dùng --metadata-only: chỉ scrape team overview pages
                # (stadium, manager, formation) — bỏ qua kader/market value pages.
                # Lý do: market values trên TM cập nhật ~1 lần/tuần (thứ 4),
                # không cần scrape full kader daily (20 đội × 30+ cầu thủ).
                # Market values đầy đủ được cập nhật qua run_pipeline.py chạy weekly.
                ok &= run_with_retry(
                    [PYTHON, "tm_scraper.py", "--league", league, "--metadata-only"],
                    ROOT / "transfermarkt",
                    f"Daily/Transfermarkt-metadata [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )
            ok &= run_with_retry(
                [PYTHON, "-m", "db.loader", "--league", league],
                ROOT,
                f"Daily/DBLoad [{league}]",
                self.log,
                dry_run=self.dry_run,
                timeout=self.task_timeout_seconds,
                shutdown_event=self._shutdown,
            )

            if not self.skip_crossref_build:
                ok &= run_with_retry(
                    [
                        PYTHON,
                        "tools/maintenance/build_team_canonical.py",
                        "--league",
                        league,
                    ],
                    ROOT,
                    f"Daily/TeamCanonical [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )
                ok &= run_with_retry(
                    [
                        PYTHON,
                        "tools/maintenance/build_match_crossref.py",
                        "--league",
                        league,
                    ],
                    ROOT,
                    f"Daily/MatchCrossref [{league}]",
                    self.log,
                    dry_run=self.dry_run,
                    timeout=self.task_timeout_seconds,
                    shutdown_event=self._shutdown,
                )

        if self._shutdown and self._shutdown.is_set():
            self.log.info("⏹ Daily maintenance aborted before post-steps (shutdown)")
            return False

        # AI: Nightly player performance trend analysis
        if self.skip_ai_trend:
            self.log.info("⏭ Daily AI trend disabled (--skip-ai-trend)")
        else:
            self._analyze_player_trends()

        if self._shutdown and self._shutdown.is_set():
            self.log.info("⏹ Daily maintenance aborted after AI trends (shutdown)")
            return False

        # Cleanup live data
        self._cleanup_live_data()

        if self._shutdown and self._shutdown.is_set():
            self.log.info("⏹ Daily maintenance aborted after cleanup (shutdown)")
            return False

        # Disk space cleanup: remove old output files
        self._cleanup_disk_space()

        if self._shutdown and self._shutdown.is_set():
            self.log.info("⏹ Daily maintenance aborted after disk cleanup (shutdown)")
            return False

        # AI insight jobs retention cleanup
        self._cleanup_old_insight_jobs()

        if self._shutdown and self._shutdown.is_set():
            self.log.info(
                "⏹ Daily maintenance aborted after insight retention cleanup (shutdown)"
            )
            return False

        # Refresh views
        self._refresh_views()

        self.last_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Send Daily Health Report (Rich Embed)
        self._send_health_report(ok)
        return ok

    def _analyze_player_trends(self) -> None:
        """Run nightly AI player performance trend analysis for all leagues.
        Phase D: also enqueues player_trend pipeline jobs (shadow mode)."""
        if self.dry_run:
            self.log.info("[DRY-RUN] Would analyze player trends")
            return
        try:
            from services import player_trend

            total = 0
            for league in self.leagues:
                if self._shutdown and self._shutdown.is_set():
                    return
                self.log.info("▶ Daily/AITrend [%s]", league)
                count = player_trend.run_and_save(league)
                total += count
                self.log.info("✓ Daily/AITrend [%s] analyzed=%d", league, count)

                # Phase D: enqueue player_trend pipeline jobs for notable players
                try:
                    insights = player_trend.analyze_all_players(league)
                    for p in insights:
                        if p["trend"] in ("GREEN", "RED"):
                            insight_producer.enqueue_player_trend(
                                league_id=league,
                                player_id=p["player_id"],
                                player_name=p["player_name"],
                                trend=p["trend"],
                                trend_score=p["score"],
                                goals=p.get("goals", []),
                                assists=p.get("assists", []),
                                xg_arr=p.get("xg_arr", []),
                                xa_arr=p.get("xa_arr", []),
                                match_count=p.get("match_count", 0),
                            )
                except Exception as exc:
                    self.log.debug("Player trend pipeline enqueue error: %s", exc)

            self.log.info("✓ Player trends: %d players analyzed", total)
            self.notifier.send(
                "info", f"📈 Player trend analysis complete: {total} players"
            )
        except Exception as exc:
            self.log.warning("Player trend analysis failed: %s", exc)

    def _cleanup_live_data(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] Would cleanup_live_data(7)")
            return
        # Compensation: clear flush_incomplete for stale finished matches
        self._compensate_flush_incomplete()
        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM cleanup_live_data(7)")
            result = cur.fetchone()
            conn.commit()
            if result:
                self.log.info(
                    "✓ Cleanup: %d snapshots, %d incidents deleted",
                    result[0],
                    result[1],
                )
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("Cleanup failed: %s", exc)

    def _compensate_flush_incomplete(self) -> None:
        """Compensation worker for matches with flush_incomplete = TRUE (plan_live Step 4).
        Marks them as compensated so post-match re-processing can pick them up."""
        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT event_id, league_id, home_team, away_team
                FROM live_match_state
                WHERE flush_incomplete = TRUE
                  AND status = 'finished'
            """)
            rows = cur.fetchall()
            if not rows:
                cur.close()
                conn.close()
                return

            self.log.info(
                "🔧 Compensation: %d matches with flush_incomplete", len(rows)
            )
            for event_id, league_id, home, away in rows:
                self.log.info(
                    "  🔧 Compensating event %d [%s] %s vs %s",
                    event_id,
                    league_id,
                    home,
                    away,
                )
                # Mark as compensated — post-match worker will re-process
                cur.execute(
                    """
                    UPDATE live_match_state
                    SET flush_incomplete = FALSE, loaded_at = NOW()
                    WHERE event_id = %s
                """,
                    (event_id,),
                )
            conn.commit()

            # Re-trigger post-match for each compensated match
            for event_id, league_id, home, away in rows:
                self.notifier.send(
                    "info",
                    f"🔧 Compensation re-trigger: [{league_id}] {home} vs {away}",
                )

            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("Flush incomplete compensation failed: %s", exc)

    def _cleanup_old_insight_jobs(self) -> None:
        """Retention cleanup: delete old ai_insight_jobs rows (90 days).
        Guarded: never deletes jobs that have linked feedback (FK RESTRICT)."""
        if self.dry_run:
            self.log.info("[DRY-RUN] Would cleanup old insight jobs")
            return
        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()
            cur.execute("""
                DELETE FROM ai_insight_jobs
                WHERE status IN ('succeeded', 'dropped', 'failed')
                  AND created_at < NOW() - INTERVAL '90 days'
                  AND NOT EXISTS (
                      SELECT 1 FROM ai_insight_feedback f
                      WHERE f.job_id = ai_insight_jobs.id
                  )
            """)
            deleted = cur.rowcount
            conn.commit()
            cur.close()
            conn.close()
            if deleted:
                self.log.info("✓ Insight retention: deleted %d old jobs", deleted)
        except Exception as exc:
            self.log.warning("Insight retention cleanup failed: %s", exc)

    def _refresh_views(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] Would refresh materialized views")
            return
        try:
            from db.config_db import get_connection

            conn = get_connection()
            cur = conn.cursor()
            for mv in (
                "mv_tm_player_candidates",
                "mv_player_profiles",
                "mv_team_profiles",
                "mv_shot_agg",
                "mv_player_complete_stats",
            ):
                try:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                    conn.commit()
                    self.log.info("✓ Refreshed %s", mv)
                except Exception as exc:
                    conn.rollback()
                    self.log.error("❌ Failed to refresh %s: %s", mv, exc)
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("MV refresh error: %s", exc)

    # ── Disk space management ──────────────────────────────
    _OUTPUT_DIRS = [
        "understat/output",
        "fbref/output",
        "sofascore/output",
        "transfermarkt/output",
    ]
    _RETENTION_DAYS = 7
    _LARGE_FILE_THRESHOLD_MB = 50

    def _cleanup_disk_space(self) -> None:
        """Delete output CSV/JSON files older than _RETENTION_DAYS.
        Logs per-directory breakdown and warns about large files."""
        if self.dry_run:
            self.log.info("[DRY-RUN] Would cleanup disk space")
            return

        cutoff = time.time() - (self._RETENTION_DAYS * 86400)
        total_files = 0
        total_bytes = 0

        for rel_dir in self._OUTPUT_DIRS:
            dir_path = ROOT / rel_dir
            if not dir_path.exists():
                continue

            dir_files = 0
            dir_bytes = 0

            for f in dir_path.rglob("*"):
                if not f.is_file():
                    continue
                if f.suffix.lower() not in (".csv", ".json", ".html"):
                    continue

                try:
                    fstat = f.stat()
                    size = fstat.st_size

                    # Safety check: warn about large files still remaining
                    size_mb = size / (1024 * 1024)
                    if size_mb > self._LARGE_FILE_THRESHOLD_MB:
                        self.log.warning(
                            "[DiskCleanup] ⚠ Large file detected: %s (%.1f MB)",
                            f.relative_to(ROOT), size_mb,
                        )

                    # Delete if older than retention period
                    if fstat.st_mtime < cutoff:
                        f.unlink()
                        dir_files += 1
                        dir_bytes += size
                except Exception as exc:
                    self.log.debug("[DiskCleanup] Cannot process %s: %s", f, exc)

            if dir_files > 0:
                mb_freed = dir_bytes / (1024 * 1024)
                self.log.info(
                    "[DiskCleanup] %s: %d files deleted, %.1f MB freed",
                    rel_dir, dir_files, mb_freed,
                )
            total_files += dir_files
            total_bytes += dir_bytes

        if total_files > 0:
            self.log.info(
                "[DiskCleanup] TOTAL: %d files, %.1f MB freed",
                total_files, total_bytes / (1024 * 1024),
            )
        else:
            self.log.info("[DiskCleanup] No old output files to clean.")

    def _send_health_report(self, maintenance_ok: bool) -> None:
        """Send a Daily Health Report as Rich Embed to Discord.
        Queries DB for AI job stats and CB state transitions (last 24h)."""
        if self.dry_run:
            self.log.info("[DRY-RUN] Would send health report")
            return

        # ── Gather stats from DB ──
        ai_stats = {"total": 0, "succeeded": 0, "dropped": 0, "failed": 0}
        cb_events = []
        try:
            from db.config_db import get_connection
            conn = get_connection()
            cur = conn.cursor()

            # AI job stats (last 24h)
            cur.execute("""
                SELECT status, COUNT(*)
                FROM ai_insight_jobs
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY status
            """)
            for status, count in cur.fetchall():
                ai_stats["total"] += count
                if status in ai_stats:
                    ai_stats[status] = count

            # CB state transitions (last 24h)
            cur.execute("""
                SELECT breaker_name, old_state, new_state, reason,
                       TO_CHAR(logged_at, 'HH24:MI') as ts
                FROM cb_state_log
                WHERE logged_at >= NOW() - INTERVAL '24 hours'
                ORDER BY logged_at DESC
                LIMIT 10
            """)
            cb_events = cur.fetchall()

            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("Health report DB query error: %s", exc)

        # ── Build embed fields ──
        status_emoji = "✅" if maintenance_ok else "❌"
        status_text = "All tasks completed" if maintenance_ok else "Some tasks had errors"

        # AI performance summary
        if ai_stats["total"] > 0:
            success_rate = ai_stats["succeeded"] / ai_stats["total"] * 100
            ai_summary = (
                f"Total: **{ai_stats['total']}** jobs\n"
                f"✅ Succeeded: **{ai_stats['succeeded']}** ({success_rate:.0f}%)\n"
                f"🗑️ Dropped: **{ai_stats['dropped']}**\n"
                f"❌ Failed: **{ai_stats['failed']}**"
            )
        else:
            ai_summary = "No AI jobs in the last 24h."

        # CB health
        if cb_events:
            cb_lines = []
            for name, old_s, new_s, reason, ts in cb_events[:5]:
                cb_lines.append(f"`{ts}` {name}: {old_s}→{new_s}")
            cb_summary = "\n".join(cb_lines)
        else:
            cb_summary = "🟢 No circuit breaker events (all healthy)"

        # Leagues
        league_list = ", ".join(self.leagues)

        fields = [
            {"name": "🏟️ Leagues", "value": league_list, "inline": True},
            {"name": "📊 Maintenance", "value": f"{status_emoji} {status_text}", "inline": True},
            {"name": "🤖 AI Pipeline (24h)", "value": ai_summary, "inline": False},
            {"name": "⚡ Circuit Breaker", "value": cb_summary, "inline": False},
        ]

        self.notifier.send_embed(
            "daily_done",
            "📋 Daily Health Report",
            description=f"Report for {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            fields=fields,
            footer="Vertex Football Scraper v2.0 | e2-micro",
        )


# ════════════════════════════════════════════════════════════
# MASTER SCHEDULER
# ════════════════════════════════════════════════════════════


class MasterScheduler:
    """Single-process HTTP worker daemon for all leagues."""

    def __init__(
        self,
        leagues: list[str],
        *,
        dry_run: bool = False,
        skip_crossref_build: bool = False,
        skip_daily_maintenance: bool = False,
        force_daily_now: bool = False,
        daily_timeout_seconds: int = 7200,
        fbref_standings_only: bool = False,
        fbref_no_match_passing: bool = False,
        fbref_team_limit: int = 0,
        fbref_match_limit: int = 0,
        skip_ai_trend: bool = False,
    ):
        self.leagues = [l.upper() for l in leagues]
        self.dry_run = dry_run
        self.force_daily_now = force_daily_now
        # AI Insight Pipeline: shadow_mode=True keeps old direct flow active,
        # set INSIGHT_SHADOW_MODE=0 to switch to pipeline-driven publish (Phase C)
        self.insight_shadow_mode = os.environ.get("INSIGHT_SHADOW_MODE", "1") != "0"
        self.log = setup_logging()
        self.notifier = Notifier(self.log)
        self._shutdown = asyncio.Event()

        self.tournament_ids = {l: TOURNAMENT_IDS[l] for l in self.leagues}
        self.browser = CurlCffiClient(self.log, self.notifier, dry_run=dry_run)
        # ── Anti-ban state machine + metrics (plan_live Step 1+2) ──
        self.browser.antiban = AntiBanStateMachine(self.log)
        self.browser.metrics = LiveMetrics(self.log)
        self.schedule = ScheduleManager(self.tournament_ids, self.browser, self.log)
        self.pool = LiveTrackingPool(
            self.browser, self.log, self.notifier, dry_run=dry_run
        )
        self.post_match = PostMatchWorker(
            self.log,
            self.notifier,
            browser=self.browser,
            dry_run=dry_run,
            shutdown_event=self._shutdown,
        )
        # FIX Issue #3: Thread pool so post-match subprocesses don't block event loop
        self._post_match_executor = ThreadPoolExecutor(
            max_workers=3, thread_name_prefix="post_match"
        )
        self.daily = DailyMaintenance(
            self.leagues,
            self.log,
            self.notifier,
            dry_run=dry_run,
            shutdown_event=self._shutdown,
            skip_crossref_build=skip_crossref_build,
            task_timeout_seconds=daily_timeout_seconds,
            fbref_standings_only=fbref_standings_only,
            fbref_no_match_passing=fbref_no_match_passing,
            fbref_team_limit=fbref_team_limit,
            fbref_match_limit=fbref_match_limit,
            skip_ai_trend=skip_ai_trend,
        )
        if skip_daily_maintenance:
            # Mark daily as already done for today so restart/test can continue immediately.
            self.daily.last_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            self.log.info(
                "Daily maintenance bypassed for this run (--skip-daily-maintenance)"
            )
        self.last_news_fetch: datetime | None = None
        self._lineup_fetched: set[int] = set()  # event_ids already fetched lineups

    def _install_signals(self) -> None:
        def handler(*_):
            self.log.info("Shutdown signal received...")
            self._shutdown.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, handler)

    def _sleep_interruptible(self, seconds: float) -> bool:
        """Sleep for N seconds, return False if shutdown requested."""
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self._shutdown.is_set():
                return False
            time.sleep(min(10, max(0, deadline - time.monotonic())))
        return not self._shutdown.is_set()

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.0f}m"
        else:
            return f"{int(seconds // 3600)}h{int((seconds % 3600) // 60):02d}m"

    async def run(self) -> None:
        self._install_signals()

        self.log.info("═" * 60)
        self.log.info("MASTER SCHEDULER STARTED")
        self.log.info("═" * 60)
        self.log.info("  Leagues:    %s", ", ".join(self.leagues))
        self.log.info("  Dry run:    %s", self.dry_run)
        self.log.info("  PID:        %d", os.getpid())
        self.log.info("  Recycle:    every %d requests", BROWSER_RECYCLE_EVERY)
        self.log.info("  Discord:    %s", "YES" if self.notifier.is_enabled else "NO")
        self.log.info(
            "  Insight:    %s", "SHADOW" if self.insight_shadow_mode else "LIVE"
        )
        self.log.info("  Anti-ban:   %s", self.browser.antiban.state.value)
        self.log.info("═" * 60)

        await self.browser.start()
        self.notifier.send(
            "info", f"Master started — {', '.join(self.leagues)} (PID={os.getpid()})"
        )

        try:
            while not self._shutdown.is_set():
                try:
                    await self._cycle()
                except Exception as exc:
                    self.log.exception("Cycle error: %s", exc)
                    self.notifier.send("error", f"Cycle error: {exc}")
                    if not self._sleep_interruptible(60):
                        break
        finally:
            await self.browser.stop()
            self.log.info("MASTER SCHEDULER STOPPED")
            self.notifier.send("info", "Master stopped")

    async def _cycle(self) -> None:
        now = datetime.now(timezone.utc)
        self.log.info("─" * 50)
        self.log.info("CYCLE START — %s", now.strftime("%Y-%m-%d %H:%M UTC"))

        # ── 1. Daily maintenance (with barrier) ──
        if self.daily.is_due() and (now.hour >= 6 or self.force_daily_now):
            if self.pool.is_empty:
                self.log.info("🚧 Pool is empty — running daily maintenance")
                self._lineup_fetched.clear()  # Reset lineup tracking daily
                self.daily.run()
            else:
                self.log.info(
                    "⏸️  Pool has %d active matches — deferring maintenance",
                    self.pool.active_count,
                )

        # ── 2. Browser recycle (when pool is idle) ──
        if self.browser.needs_recycle() and self.pool.is_empty:
            await self.browser.recycle()
            self.notifier.send("recycle", "Browser recycled (memory cleanup)")

        # ── 3. Fetch schedule ──
        upcoming = await self.schedule.get_upcoming()

        if not upcoming and self.pool.is_empty:
            # Nothing to do — sleep until next check (max 30 mins so news works)
            next_check = now + timedelta(minutes=30)
            self.log.info(
                "💤 No matches — sleeping 30m until %s UTC",
                next_check.strftime("%H:%M"),
            )
            self._check_news(now)
            if not self._sleep_interruptible(1800):
                return
            return

        # ── 3.5 Phase 1: Fetch lineups ~60min before kickoff ──
        await self._check_lineups(upcoming)

        # ── 4. Add upcoming matches to pool when ready ──
        for match in upcoming:
            kickoff = match.get("kickoff_utc")
            if not kickoff:
                continue
            time_to_kickoff = (kickoff - datetime.now(timezone.utc)).total_seconds()

            # Add to pool if within 15 minutes or already in progress
            if time_to_kickoff <= 900 or match["status"] == "inprogress":
                self.pool.add_match(match)
                # Phase 2: Refresh lineup when entering pool (-15min)
                await self._fetch_and_save_lineup(match)

        # ── 5. Poll active matches ──
        if not self.pool.is_empty:
            finished_matches = await self.pool.poll_all()

            # ── 5b. AI Insight worker cycle ──
            try:
                processed = insight_worker.run_worker_cycle(
                    shadow_mode=self.insight_shadow_mode,
                )
                if processed:
                    self.log.info("🤖 Insight worker processed %d jobs", processed)
            except Exception as exc:
                self.log.debug("Insight worker error: %s", exc)

            # FIX Issue #3: Run post-match in thread pool to avoid blocking event loop.
            # Each post_match.run() can take 5-15 minutes (subprocess calls).
            for fm in finished_matches:
                asyncio.get_event_loop().run_in_executor(
                    self._post_match_executor, self.post_match.run, fm
                )

            # Sleep based on number of active matches
            if not self.pool.is_empty:
                # poll_all() already spread its requests across ~60 seconds via per_match_interval
                # We just need a short 5s technical break before the next cycle starts
                self.log.info(
                    "⏳ Cycle complete for %d active matches. Next cycle shortly...",
                    self.pool.active_count,
                )
                if not self._sleep_interruptible(5):
                    return
                return  # Go straight to next cycle

        # ── 6. If pool is empty but matches are coming later ──
        if upcoming:
            next_kickoff = min(
                m["kickoff_utc"] for m in upcoming if m.get("kickoff_utc")
            )
            wake_time = next_kickoff - timedelta(minutes=60)
            now = datetime.now(timezone.utc)
            if wake_time > now:
                delta = (wake_time - now).total_seconds()
                self.log.info(
                    "💤 Next match in %s — sleeping until %s UTC",
                    self._fmt_duration(delta),
                    wake_time.strftime("%H:%M"),
                )
                self._check_news(now)
                # Max sleep 1800 to ensure news radar runs
                if not self._sleep_interruptible(min(delta, 1800)):
                    return
            else:
                self._check_news(now)
                # Guard against CPU spin-loops if a match fails to enter the pool
                if not self._sleep_interruptible(60):
                    return
            return

        self._check_news(datetime.now(timezone.utc))
        # Brief pause before next cycle
        if not self._sleep_interruptible(60):
            return

    def _check_news(self, now: datetime) -> None:
        """Run the RSS news and injury radar every 30 minutes."""
        if (
            self.last_news_fetch is None
            or (now - self.last_news_fetch).total_seconds() >= 1800
        ):
            if self.dry_run:
                self.log.info("[DRY-RUN] Would fetch RSS news")
            else:
                try:
                    from services import news_radar
                    self.log.info("📰 Starting RSS news radar fetch...")
                    saved = news_radar.run_and_save()
                    if saved > 0:
                        self.log.info("✓ RSS news radar fetch complete: %d items", saved)
                    else:
                        self.log.info("✓ RSS news radar fetch complete: 0 new items")
                except Exception as exc:
                    self.log.error("News radar error: %s", exc)
            self.last_news_fetch = now

    async def _check_lineups(self, upcoming: list[dict]) -> None:
        """Phase 1: Fetch lineups for matches within 60 minutes of kickoff."""
        now = datetime.now(timezone.utc)
        for match in upcoming:
            kickoff = match.get("kickoff_utc")
            if not kickoff:
                continue
            ttk = (kickoff - now).total_seconds()
            event_id = match.get("event_id")
            # Fetch if within 60 min AND not yet fetched in this daily cycle
            if 0 < ttk <= 3600 and event_id and event_id not in self._lineup_fetched:
                await self._fetch_and_save_lineup(match)
                self._lineup_fetched.add(event_id)

    async def _fetch_and_save_lineup(self, match: dict) -> None:
        """Fetch lineup from SofaScore and UPSERT into match_lineups table."""
        event_id = match.get("event_id")
        if not event_id:
            return

        home = match.get("home_team", "?")
        away = match.get("away_team", "?")
        league = match.get("league", "")

        if self.dry_run:
            self.log.info(
                "[DRY-RUN] Would fetch lineup: %s vs %s (event=%d)",
                home,
                away,
                event_id,
            )
            return

        try:
            # Fetch lineup via browser client
            data = await self.browser.get_json(f"/event/{event_id}/lineups")
            if not data:
                self.log.warning("  ⚠ No lineup data for %s vs %s", home, away)
                return

            # Parse lineup (same logic as sofascore_client.fetch_match_lineups)
            rows = []
            for side in ("home", "away"):
                lineup_data = data.get(side, {})
                formation = lineup_data.get("formation")
                team_name = home if side == "home" else away
                for p in lineup_data.get("players", []):
                    pi = p.get("player", {})
                    stats = p.get("statistics", {})
                    pid = pi.get("id")
                    if not pid:
                        continue
                    rows.append(
                        (
                            event_id,
                            pid,
                            pi.get("name") or pi.get("shortName"),
                            side,
                            team_name,
                            pi.get("position"),
                            pi.get("jerseyNumber"),
                            p.get("substitute", False),
                            stats.get("minutesPlayed"),
                            stats.get("rating"),
                            formation,
                            "confirmed",
                            league,
                            "",  # season placeholder
                        )
                    )

            if not rows:
                self.log.info("  ⚠ Empty lineup for %s vs %s", home, away)
                return

            # UPSERT into match_lineups
            from db.config_db import get_connection

            conn = get_connection()
            try:
                with conn.cursor() as cur:
                    import psycopg2.extras

                    sql = """
                        INSERT INTO match_lineups
                            (event_id, player_id, player_name, team_side, team_name,
                             position, jersey_number, is_substitute, minutes_played,
                             rating, formation, status, league_id, season)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (event_id, player_id, league_id) DO UPDATE SET
                            player_name = EXCLUDED.player_name,
                            team_side = EXCLUDED.team_side,
                            team_name = EXCLUDED.team_name,
                            position = EXCLUDED.position,
                            jersey_number = EXCLUDED.jersey_number,
                            is_substitute = EXCLUDED.is_substitute,
                            minutes_played = COALESCE(EXCLUDED.minutes_played, match_lineups.minutes_played),
                            rating = COALESCE(EXCLUDED.rating, match_lineups.rating),
                            formation = EXCLUDED.formation,
                            status = EXCLUDED.status,
                            loaded_at = NOW()
                    """
                    psycopg2.extras.execute_batch(cur, sql, rows, page_size=100)
                conn.commit()
                self.log.info(
                    "  📋 Lineup saved: %s vs %s — %d players (event=%d)",
                    home,
                    away,
                    len(rows),
                    event_id,
                )
            finally:
                conn.close()

        except Exception as exc:
            self.log.error("Lineup fetch error for event %d: %s", event_id, exc)


# ════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════


def main() -> None:
    # Load .env
    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, val = line.partition("=")
                os.environ.setdefault(key.strip(), val.strip())

    parser = argparse.ArgumentParser(
        description="Vertex Master Scheduler — Single-browser 24/7 daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scheduler_master.py                           # All 7 leagues
  python scheduler_master.py --leagues EPL LALIGA      # Subset
  python scheduler_master.py --dry-run
  python scheduler_master.py --test-notify
        """,
    )
    parser.add_argument(
        "--leagues",
        nargs="+",
        default=["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1", "UCL", "UEL"],
        help="Leagues to track (default: all 7)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log schedule without running scrapers or browser",
    )
    parser.add_argument(
        "--test-notify",
        action="store_true",
        help="Send test Discord notification and exit",
    )
    parser.add_argument(
        "--skip-crossref-build",
        action="store_true",
        help="Skip daily team_canonical + match_crossref build",
    )
    parser.add_argument(
        "--skip-daily-maintenance",
        action="store_true",
        help="Skip daily maintenance for current process run",
    )
    parser.add_argument(
        "--force-daily-now",
        action="store_true",
        help="Force daily maintenance even before 06:00 UTC (debug)",
    )
    parser.add_argument(
        "--daily-timeout-seconds",
        type=int,
        default=7200,
        help="Timeout per daily subprocess task (default: 7200)",
    )
    parser.add_argument(
        "--fbref-standings-only",
        action="store_true",
        help="Daily FBref: scrape standings+fixtures only",
    )
    parser.add_argument(
        "--fbref-no-match-passing",
        action="store_true",
        help="Daily FBref: skip match-report passing data",
    )
    parser.add_argument(
        "--fbref-team-limit",
        type=int,
        default=0,
        help="Daily FBref: limit number of squad pages (0=all)",
    )
    parser.add_argument(
        "--fbref-match-limit",
        type=int,
        default=0,
        help="Daily FBref: limit match reports (0=all)",
    )
    parser.add_argument(
        "--skip-ai-trend",
        action="store_true",
        help="Daily: skip AI player trend stage (debug/stability)",
    )

    args = parser.parse_args()
    leagues = [l.upper() for l in args.leagues]

    for l in leagues:
        if l not in TOURNAMENT_IDS:
            print(f"Unknown league: {l}")
            print(f"Supported: {', '.join(sorted(TOURNAMENT_IDS.keys()))}")
            sys.exit(1)

    if args.test_notify:
        log = setup_logging()
        n = Notifier(log)
        if not n.is_enabled:
            print("❌ Không có DISCORD_WEBHOOK nào được cấu hình trong .env")
            sys.exit(1)
        n.send("test", "🧪 Test notification from scheduler_master.py — OK!")
        print("✅ Test notification sent successfully!")
        sys.exit(0)

    scheduler = MasterScheduler(
        leagues,
        dry_run=args.dry_run,
        skip_crossref_build=args.skip_crossref_build,
        skip_daily_maintenance=args.skip_daily_maintenance,
        force_daily_now=args.force_daily_now,
        daily_timeout_seconds=args.daily_timeout_seconds,
        fbref_standings_only=args.fbref_standings_only,
        fbref_no_match_passing=args.fbref_no_match_passing,
        fbref_team_limit=args.fbref_team_limit,
        fbref_match_limit=args.fbref_match_limit,
        skip_ai_trend=args.skip_ai_trend,
    )
    asyncio.run(scheduler.run())


if __name__ == "__main__":
    main()
