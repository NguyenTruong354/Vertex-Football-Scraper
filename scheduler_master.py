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
import random
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── Paths ──
ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "sofascore"))

import sofascore.config_sofascore as cfg
from curl_cffi.requests import AsyncSession

# ── SofaScore tournament IDs ──
TOURNAMENT_IDS = {
    "EPL": 17, "LALIGA": 8, "BUNDESLIGA": 35,
    "SERIEA": 23, "LIGUE1": 34, "UCL": 7,
    "EREDIVISIE": 37, "LIGA_PORTUGAL": 238, "RFPL": 203,
}

# ── Source support matrix ──
LEAGUE_SOURCES = {
    "EPL":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LALIGA":     {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "BUNDESLIGA": {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "SERIEA":     {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGUE1":     {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
}

# ── Retry / recycle config ──
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [60, 300, 900]
BROWSER_RECYCLE_EVERY = 200   # recycle Chrome every N requests


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
        maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8",
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
        "match_start": "🟢", "goal": "⚽", "match_end": "🏁", "match_event": "⚡",
        "post_match_done": "📊", "error": "🔴", "daily_done": "🔧",
        "info": "ℹ️", "recycle": "♻️",
    }
    
    # Map event types to specific webhook environment variables
    ROUTING = {
        "match_start": "DISCORD_WEBHOOK_LIVE",
        "goal": "DISCORD_WEBHOOK_LIVE",
        "match_event": "DISCORD_WEBHOOK_LIVE",
        "match_end": "DISCORD_WEBHOOK_LIVE",
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
            self.default_webhook or 
            any(os.environ.get(env) for env in set(self.ROUTING.values()))
        )

    def send(self, event_type: str, message: str) -> None:
        emoji = self.EMOJI.get(event_type, "📢")
        full_msg = f"{emoji} **[MASTER]** {message}"
        self.log.info("NOTIFY [%s]: %s", event_type, message)
        
        # Determine which webhook to use
        env_var = self.ROUTING.get(event_type, "")
        webhook_url = os.environ.get(env_var) if env_var else ""
        
        # Fallback to default
        if not webhook_url:
            webhook_url = self.default_webhook
            
        if not webhook_url:
            return  # No webhook configured for this event
            
        try:
            import urllib.request
            payload = json.dumps({"content": full_msg}).encode()
            req = urllib.request.Request(
                webhook_url, data=payload,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": "VertexFootballScraper/1.0",
                }, method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            self.log.warning("Discord send failed for [%s]: %s", event_type, exc)


# ════════════════════════════════════════════════════════════
# HTTP CLIENT — curl_cffi (Replaces SharedBrowser)
# ════════════════════════════════════════════════════════════

class CurlCffiClient:
    """Lightweight HTTP client that bypasses Cloudflare using TLS impersonation."""

    # Retry backoff timings (seconds)
    RETRY_BACKOFF = [4, 10, 20]
    MAX_RETRIES = 3

    def __init__(self, log: logging.Logger, notifier: "Notifier", *, dry_run: bool = False):
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

    def _check_block_alert(self, url: str, code: int) -> None:
        """Trigger a Discord alert if Cloudflare/rate-limit blocking is persistent."""
        count = self._consecutive_403 if code == 403 else self._consecutive_429
        if count >= 5:
            now = time.time()
            if now - self._last_block_alert > 3600:  # Alert once per hour max
                if code == 403:
                    self.log.error("CRITICAL: Cloudflare persistently blocked our TLS footprint (403 Forbidden).")
                    self.notifier.send("error", 
                        f"**🚨 CLOUDFLARE BLOCK ALERT**\n"
                        f"Received {count} consecutive `403 Forbidden` responses.\n"
                        f"The impersonate profile (`chrome120`) may have been detected and blocked by Cloudflare.\n"
                        f"Last blocked URL: `{url}`\n"
                        f"*Action required: Update impersonate version in CurlCffiClient.*"
                    )
                else:
                    self.log.error("CRITICAL: SofaScore rate limiting our IP (429 Too Many Requests).")
                    self.notifier.send("error",
                        f"**⚡ RATE LIMIT ALERT**\n"
                        f"Received {count} consecutive `429 Too Many Requests` responses.\n"
                        f"SofaScore is throttling our requests. IP may be temporarily blocked.\n"
                        f"*The system will auto-backoff, but monitor closely.*"
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
            }
        )
        self._request_count = 0

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def recycle(self) -> None:
        """curl_cffi doesn't leak memory like Chrome, but we recycle the session to clear cookies/state."""
        self.log.info("♻️  Recycling HTTP session (after %d requests)...", self._request_count)
        await self.stop()
        await self.start()

    def needs_recycle(self) -> bool:
        return self._request_count >= BROWSER_RECYCLE_EVERY

    async def get_json(self, endpoint: str) -> dict | list | None:
        """Fetch JSON from SofaScore API with retry and jitter. Serialized by Lock."""
        if self.dry_run:
            return None

        async with self._lock:
            url = f"{cfg.SS_API_BASE}{endpoint}"

            for attempt in range(self.MAX_RETRIES):
                # Rate limit with randomized jitter (2-4s) to look human
                elapsed = time.monotonic() - self._last_request
                jitter = random.uniform(2.0, 4.0)
                if elapsed < jitter:
                    await asyncio.sleep(jitter - elapsed)

                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=15)

                    if resp.status_code == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        data = resp.json()
                        if isinstance(data, dict) and data.get("error"):
                            if data["error"].get("code") == 404:
                                return None
                        return data

                    elif resp.status_code == 403:
                        self._consecutive_403 += 1
                        self.log.warning("HTTP 403 Forbidden (attempt %d/%d): %s",
                                         attempt + 1, self.MAX_RETRIES, url)
                        self._check_block_alert(url, 403)
                        if attempt < self.MAX_RETRIES - 1:
                            backoff = self.RETRY_BACKOFF[attempt] * 2
                            self.log.info("  ⏳ Backoff %ds before retry...", backoff)
                            await asyncio.sleep(backoff)

                    elif resp.status_code == 429:
                        self._consecutive_429 += 1
                        self.log.warning("HTTP 429 Rate Limited (attempt %d/%d): %s",
                                         attempt + 1, self.MAX_RETRIES, url)
                        self._check_block_alert(url, 429)
                        if attempt < self.MAX_RETRIES - 1:
                            backoff = self.RETRY_BACKOFF[attempt] * 3
                            self.log.info("  ⏳ Rate limit backoff %ds...", backoff)
                            await asyncio.sleep(backoff)

                    else:
                        self.log.warning("HTTP %d: %s", resp.status_code, url)
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF[attempt])

                except Exception as exc:
                    self.log.debug("Request failed (attempt %d): %s — %s",
                                   attempt + 1, endpoint, exc)
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(self.RETRY_BACKOFF[attempt])

            return None

    async def get_schedule_json(self, date_str: str) -> dict:
        """Fetch scheduled-events for a date with retry."""
        if self.dry_run:
            return {}

        async with self._lock:
            url = f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}"

            for attempt in range(self.MAX_RETRIES):
                elapsed = time.monotonic() - self._last_request
                jitter = random.uniform(2.0, 4.0)
                if elapsed < jitter:
                    await asyncio.sleep(jitter - elapsed)

                try:
                    if not self._session:
                        await self.start()

                    self._last_request = time.monotonic()
                    self._request_count += 1

                    resp = await self._session.get(url, timeout=20)
                    if resp.status_code == 200:
                        self._consecutive_403 = 0
                        self._consecutive_429 = 0
                        return resp.json()
                    elif resp.status_code == 403:
                        self._consecutive_403 += 1
                        self.log.warning("HTTP 403 on schedule fetch (attempt %d): %s", attempt + 1, url)
                        self._check_block_alert(url, 403)
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 2)
                    elif resp.status_code == 429:
                        self._consecutive_429 += 1
                        self.log.warning("HTTP 429 on schedule fetch (attempt %d): %s", attempt + 1, url)
                        self._check_block_alert(url, 429)
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_BACKOFF[attempt] * 3)

                except Exception as exc:
                    self.log.warning("Schedule fetch failed for %s (attempt %d): %s",
                                     date_str, attempt + 1, exc)
                    if attempt < self.MAX_RETRIES - 1:
                        await asyncio.sleep(self.RETRY_BACKOFF[attempt])

            return {}


# ════════════════════════════════════════════════════════════
# SCHEDULE MANAGER — fetch matches for ALL leagues at once
# ════════════════════════════════════════════════════════════

class ScheduleManager:
    def __init__(self, tournament_ids: dict[str, int], browser: CurlCffiClient,
                 log: logging.Logger):
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
                    (lg for lg, tid in self.tournament_ids.items() if tid == t_id),
                    "?"
                )

                home = ev.get("homeTeam", {})
                away = ev.get("awayTeam", {})
                status = ev.get("status", {})
                hs = ev.get("homeScore", {})
                aws = ev.get("awayScore", {})
                kickoff_ts = ev.get("startTimestamp", 0)

                all_matches.append({
                    "event_id": ev.get("id"),
                    "league": league,
                    "tournament_id": t_id,
                    "home_team": home.get("name", "?"),
                    "away_team": away.get("name", "?"),
                    "home_score": hs.get("current"),
                    "away_score": aws.get("current"),
                    "status": status.get("type", ""),
                    "kickoff_utc": datetime.fromtimestamp(kickoff_ts, tz=timezone.utc) if kickoff_ts else None,
                    "kickoff_ts": kickoff_ts,
                    "round": ev.get("roundInfo", {}).get("round", 0),
                })

        # Filter: only notstarted or inprogress, deduplicate, sort
        upcoming = [m for m in all_matches if m["status"] in ("notstarted", "inprogress")]
        seen = set()
        unique = []
        for m in upcoming:
            if m["event_id"] not in seen:
                seen.add(m["event_id"])
                unique.append(m)
        unique.sort(key=lambda m: m.get("kickoff_ts", 0))

        self.log.info("Found %d upcoming matches across %d leagues",
                      len(unique), len(self.tournament_ids))
        for m in unique:
            kt = m["kickoff_utc"].strftime("%H:%M") if m["kickoff_utc"] else "?"
            self.log.info("  • [%s] %s vs %s @ %s UTC [%s]",
                          m["league"], m["home_team"], m["away_team"], kt, m["status"])

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
                data.append((
                    m["event_id"],
                    m["home_team"],
                    m["away_team"],
                    m["home_score"] or 0,
                    m["away_score"] or 0,
                    m["status"],
                    0, # minute
                    json.dumps({}), # empty statistics
                    json.dumps([]), # empty incidents
                ))

            # Batch upsert
            cur.executemany("""
                INSERT INTO live_snapshots
                    (event_id, home_team, away_team, home_score, away_score, 
                     status, minute, statistics_json, incidents_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_team = EXCLUDED.home_team,
                    away_team = EXCLUDED.away_team,
                    status = EXCLUDED.status,
                    loaded_at = NOW()
            """, data)
            conn.commit()
            
        except Exception as e:
            self.log.error("Failed to upsert upcoming match schedule to DB: %s", e)


# ════════════════════════════════════════════════════════════
# LIVE MATCH STATE (from live_match.py)
# ════════════════════════════════════════════════════════════

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
    poll_count: int = 0
    last_updated: str = ""
    start_timestamp: int = 0


# ════════════════════════════════════════════════════════════
# LIVE TRACKING POOL — round-robin polling, 1 browser
# ════════════════════════════════════════════════════════════

class LiveTrackingPool:
    """Tracks multiple concurrent matches via round-robin polling on HTTP client."""

    def __init__(self, browser: CurlCffiClient, log: logging.Logger,
                 notifier: Notifier, *, dry_run: bool = False):
        self.browser = browser
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._matches: dict[int, LiveMatchState] = {}

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
        self.log.info("➕ Added to pool: [%s] %s vs %s (event=%d)",
                      match.get("league", "?"), match["home_team"],
                      match["away_team"], eid)
        self.notifier.send("match_start",
                           f"[{match.get('league', '?')}] {match['home_team']} vs {match['away_team']} — tracking started")

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
                self.log.info("🏁 Match finished: [%s] %s %d-%d %s",
                              state.league, state.home_team,
                              state.home_score, state.away_score, state.away_team)
                self.notifier.send("match_end",
                                   f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}")

                # Final DB save
                self._save_to_db(state)

                finished.append({
                    "event_id": eid,
                    "league": state.league,
                    "home_team": state.home_team,
                    "away_team": state.away_team,
                })
                del self._matches[eid]

            # Smooth pacing: spread polling evenly across 60 seconds
            if len(event_ids) > 1:
                per_match_interval = max(60.0 / len(event_ids), 3.0)
                await asyncio.sleep(per_match_interval)

        return finished

    async def _poll_one(self, state: LiveMatchState) -> bool:
        """Poll a single match. Returns True if still in progress."""
        state.poll_count += 1
        state.last_updated = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")

        if self.dry_run:
            self.log.info("[DRY-RUN] Poll #%d: %s vs %s",
                          state.poll_count, state.home_team, state.away_team)
            return True

        # 1. Match info
        info = await self.browser.get_json(f"/event/{state.event_id}")
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
                self.notifier.send("goal",
                                   f"[{state.league}] {state.home_team} {state.home_score}-{state.away_score} {state.away_team}")

            # Calculate minute
            if state.status == "inprogress" and state.start_timestamp:
                now_ts = int(datetime.now(timezone.utc).timestamp())
                state.minute = max(0, min(120, (now_ts - state.start_timestamp) // 60))

        # 2. Incidents
        inc_data = await self.browser.get_json(f"/event/{state.event_id}/incidents")
        if inc_data:
            current_incidents = inc_data.get("incidents", [])
            
            # Detect new incidents for Discord alerts
            if state.poll_count > 1 and state.incidents:
                old_ids = {i.get("id") for i in state.incidents if i.get("id")}
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
                            self.notifier.send("match_event", 
                                f"🟥 **RED CARD** [{state.league}] {state.home_team} vs {state.away_team} | {player} ({time_str})")
                        elif inc_type == "varDecision":
                            self.notifier.send("match_event", 
                                f"📺 **VAR DECISION** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}")
                        elif inc_type == "penalty":
                            self.notifier.send("match_event", 
                                f"🎯 **PENALTY** [{state.league}] {state.home_team} vs {state.away_team} | {time_str}")
            
            state.incidents = current_incidents

        # 3. Statistics — only every 3rd poll to reduce request volume
        if state.poll_count % 3 == 1:
            stat_data = await self.browser.get_json(f"/event/{state.event_id}/statistics")
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

        # 4. Save to DB every poll
        self._save_to_db(state)

        self.log.info("  📊 [%s] %s %d-%d %s | %s' | poll #%d",
                      state.league, state.home_team, state.home_score,
                      state.away_score, state.away_team,
                      state.minute, state.poll_count)

        return state.status != "finished"

    def _save_to_db(self, state: LiveMatchState) -> None:
        """Upsert to live_snapshots + live_incidents tables."""
        try:
            from db.config_db import get_connection
            conn = get_connection()
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO live_snapshots
                    (event_id, home_team, away_team, home_score, away_score,
                     status, minute, statistics_json, incidents_json, poll_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                    status = EXCLUDED.status,
                    minute = EXCLUDED.minute,
                    statistics_json = EXCLUDED.statistics_json,
                    incidents_json = EXCLUDED.incidents_json,
                    poll_count = EXCLUDED.poll_count,
                    loaded_at = NOW()
            """, (
                state.event_id, state.home_team, state.away_team,
                state.home_score, state.away_score,
                state.status, state.minute,
                json.dumps(state.statistics, ensure_ascii=False),
                json.dumps(state.incidents, ensure_ascii=False),
                state.poll_count,
            ))

            for inc in state.incidents:
                inc_type = inc.get("incidentType", "")
                if inc_type not in ("goal", "card", "substitution", "varDecision"):
                    continue
                cur.execute("""
                    INSERT INTO live_incidents
                        (event_id, incident_type, minute, added_time,
                         player_name, player_in_name, player_out_name,
                         is_home, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, incident_type, minute, COALESCE(player_name, ''))
                    DO UPDATE SET
                        added_time     = EXCLUDED.added_time,
                        player_in_name = EXCLUDED.player_in_name,
                        player_out_name= EXCLUDED.player_out_name,
                        is_home        = EXCLUDED.is_home,
                        detail         = EXCLUDED.detail,
                        loaded_at      = NOW()
                """, (
                    state.event_id, inc_type,
                    inc.get("time"), inc.get("addedTime"),
                    inc.get("player", {}).get("name"),
                    inc.get("playerIn", {}).get("name"),
                    inc.get("playerOut", {}).get("name"),
                    inc.get("isHome"),
                    inc.get("incidentClass", ""),
                ))

            conn.commit()
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("DB save failed for event %d: %s", state.event_id, exc)


# ════════════════════════════════════════════════════════════
# SUBPROCESS RUNNER — with retry logic
# ════════════════════════════════════════════════════════════

def run_with_retry(cmd: list[str], cwd: Path, label: str,
                   log: logging.Logger, *, dry_run: bool = False,
                   timeout: int = 7200,
                   shutdown_event: asyncio.Event | None = None) -> bool:
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
            proc = subprocess.run(
                cmd, cwd=str(cwd), capture_output=True, text=True, timeout=timeout,
            )
            if proc.returncode == 0:
                log.info("✓ %s — %.1fs", label, time.perf_counter() - t0)
                return True
            log.warning("✗ %s — exit code %d", label, proc.returncode)
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
    def __init__(self, log: logging.Logger, notifier: Notifier,
                 *, dry_run: bool = False,
                 shutdown_event: asyncio.Event | None = None):
        self.log = log
        self.notifier = notifier
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
                ROOT / "understat", f"PostMatch/Understat [{league}]",
                self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
            )

        if sources.get("sofascore"):
            ok &= run_with_retry(
                [PYTHON, "sofascore_client.py", "--league", league, "--match-limit", "5"],
                ROOT / "sofascore", f"PostMatch/SofaScore [{league}]",
                self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
            )

        ok &= run_with_retry(
            [PYTHON, "-m", "db.loader", "--league", league],
            ROOT, f"PostMatch/DBLoad [{league}]",
            self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
        )

        if ok:
            self.notifier.send("post_match_done",
                               f"[{league}] Post-match done: {home} vs {away}")
        else:
            self.notifier.send("error",
                               f"[{league}] Post-match errors: {home} vs {away}")
        return ok


# ════════════════════════════════════════════════════════════
# DAILY MAINTENANCE — with barrier gate
# ════════════════════════════════════════════════════════════

class DailyMaintenance:
    def __init__(self, leagues: list[str], log: logging.Logger,
                 notifier: Notifier, *, dry_run: bool = False,
                 shutdown_event: asyncio.Event | None = None):
        self.leagues = leagues
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._shutdown = shutdown_event
        self.last_run_date: str | None = None

    def is_due(self) -> bool:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self.last_run_date != today

    def run(self) -> bool:
        self.log.info("═" * 50)
        self.log.info("DAILY MAINTENANCE (all leagues)")
        self.log.info("═" * 50)

        ok = True
        for league in self.leagues:
            if self._shutdown and self._shutdown.is_set():
                self.log.info("⏹ Daily maintenance aborted (shutdown)")
                return False
            sources = LEAGUE_SOURCES.get(league, {})
            if sources.get("fbref"):
                ok &= run_with_retry(
                    [PYTHON, "fbref_scraper.py", "--league", league],
                    ROOT / "fbref", f"Daily/FBref [{league}]",
                    self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
                )
            if sources.get("transfermarkt"):
                ok &= run_with_retry(
                    [PYTHON, "tm_scraper.py", "--league", league],
                    ROOT / "transfermarkt", f"Daily/Transfermarkt [{league}]",
                    self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
                )
            ok &= run_with_retry(
                [PYTHON, "-m", "db.loader", "--league", league],
                ROOT, f"Daily/DBLoad [{league}]",
                self.log, dry_run=self.dry_run, shutdown_event=self._shutdown,
            )

        # Cleanup live data
        self._cleanup_live_data()
        # Refresh views
        self._refresh_views()

        self.last_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        if ok:
            self.notifier.send("daily_done", "Daily maintenance completed ✓")
        else:
            self.notifier.send("error", "Daily maintenance had errors")
        return ok

    def _cleanup_live_data(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] Would cleanup_live_data(7)")
            return
        try:
            from db.config_db import get_connection
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT * FROM cleanup_live_data(7)")
            result = cur.fetchone()
            conn.commit()
            if result:
                self.log.info("✓ Cleanup: %d snapshots, %d incidents deleted",
                              result[0], result[1])
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("Cleanup failed: %s", exc)

    def _refresh_views(self) -> None:
        if self.dry_run:
            self.log.info("[DRY-RUN] Would refresh materialized views")
            return
        try:
            from db.config_db import get_connection
            conn = get_connection()
            cur = conn.cursor()
            for mv in ("mv_player_season_xg", "mv_match_summary"):
                try:
                    cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {mv}")
                    conn.commit()
                    self.log.info("✓ Refreshed %s", mv)
                except Exception:
                    conn.rollback()
                    self.log.debug("  MV %s not found, skipping", mv)
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("MV refresh error: %s", exc)


# ════════════════════════════════════════════════════════════
# MASTER SCHEDULER
# ════════════════════════════════════════════════════════════

class MasterScheduler:
    """Single-process HTTP worker daemon for all leagues."""

    def __init__(self, leagues: list[str], *, dry_run: bool = False):
        self.leagues = [l.upper() for l in leagues]
        self.dry_run = dry_run
        self.log = setup_logging()
        self.notifier = Notifier(self.log)
        self._shutdown = asyncio.Event()

        self.tournament_ids = {l: TOURNAMENT_IDS[l] for l in self.leagues}
        self.browser = CurlCffiClient(self.log, self.notifier, dry_run=dry_run)
        self.schedule = ScheduleManager(self.tournament_ids, self.browser, self.log)
        self.pool = LiveTrackingPool(self.browser, self.log, self.notifier,
                                      dry_run=dry_run)
        self.post_match = PostMatchWorker(self.log, self.notifier, dry_run=dry_run,
                                           shutdown_event=self._shutdown)
        self.daily = DailyMaintenance(self.leagues, self.log, self.notifier,
                                       dry_run=dry_run, shutdown_event=self._shutdown)

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
        self.log.info("═" * 60)

        await self.browser.start()
        self.notifier.send("info", f"Master started — {', '.join(self.leagues)} (PID={os.getpid()})")

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
        if self.daily.is_due() and now.hour >= 6:
            if self.pool.is_empty:
                self.log.info("🚧 Pool is empty — running daily maintenance")
                self.daily.run()
            else:
                self.log.info("⏸️  Pool has %d active matches — deferring maintenance",
                              self.pool.active_count)

        # ── 2. Browser recycle (when pool is idle) ──
        if self.browser.needs_recycle() and self.pool.is_empty:
            await self.browser.recycle()
            self.notifier.send("recycle", "Browser recycled (memory cleanup)")

        # ── 3. Fetch schedule ──
        upcoming = await self.schedule.get_upcoming()

        if not upcoming and self.pool.is_empty:
            # Nothing to do — sleep until next check
            next_check = now + timedelta(hours=1)
            self.log.info("💤 No matches — sleeping 1h until %s UTC",
                          next_check.strftime("%H:%M"))
            if not self._sleep_interruptible(3600):
                return
            return

        # ── 4. Add upcoming matches to pool when ready ──
        for match in upcoming:
            kickoff = match.get("kickoff_utc")
            if not kickoff:
                continue
            time_to_kickoff = (kickoff - datetime.now(timezone.utc)).total_seconds()

            # Add to pool if within 15 minutes or already in progress
            if time_to_kickoff <= 900 or match["status"] == "inprogress":
                self.pool.add_match(match)

        # ── 5. Poll active matches ──
        if not self.pool.is_empty:
            finished_matches = await self.pool.poll_all()

            # Run post-match for each finished match
            for fm in finished_matches:
                self.post_match.run(fm)

            # Sleep based on number of active matches
            if not self.pool.is_empty:
                # poll_all() already spread its requests across ~60 seconds via per_match_interval
                # We just need a short 5s technical break before the next cycle starts
                self.log.info("⏳ Cycle complete for %d active matches. Next cycle shortly...", self.pool.active_count)
                if not self._sleep_interruptible(5):
                    return
                return  # Go straight to next cycle

        # ── 6. If pool is empty but matches are coming later ──
        if upcoming:
            next_kickoff = min(
                m["kickoff_utc"] for m in upcoming if m.get("kickoff_utc")
            )
            wake_time = next_kickoff - timedelta(minutes=15)
            now = datetime.now(timezone.utc)
            if wake_time > now:
                delta = (wake_time - now).total_seconds()
                self.log.info("💤 Next match in %s — sleeping until %s UTC",
                              self._fmt_duration(delta),
                              wake_time.strftime("%H:%M"))
                if not self._sleep_interruptible(min(delta, 3600)):
                    return
            return

        # Brief pause before next cycle
        if not self._sleep_interruptible(60):
            return


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
  python scheduler_master.py                           # All 5 leagues
  python scheduler_master.py --leagues EPL LALIGA      # Subset
  python scheduler_master.py --dry-run
  python scheduler_master.py --test-notify
        """,
    )
    parser.add_argument(
        "--leagues", nargs="+", default=["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
        help="Leagues to track (default: all 5)",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Log schedule without running scrapers or browser")
    parser.add_argument("--test-notify", action="store_true",
                        help="Send test Discord notification and exit")

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
        if not n.webhook_url:
            print("DISCORD_WEBHOOK not set in .env")
            sys.exit(1)
        n.send("info", "🧪 Test from scheduler_master.py — OK!")
        print("✓ Sent")
        sys.exit(0)

    scheduler = MasterScheduler(leagues, dry_run=args.dry_run)
    asyncio.run(scheduler.run())


if __name__ == "__main__":
    main()
