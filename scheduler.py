# ============================================================
# scheduler.py — Match-Schedule-Aware Daemon (24/7)
# ============================================================
"""
Smart scheduler: fetch real match schedule → sleep → live track → post-match scrape.

One league per process. Run multiple instances via systemd.

Usage:
    python scheduler.py --league EPL
    python scheduler.py --league LALIGA
    python scheduler.py --league EPL --dry-run
    python scheduler.py --league EPL --test-notify

Architecture:
    1. Fetch today's matches from SofaScore scheduled-events API
    2. No matches? → daily maintenance → sleep until 06:00 UTC tomorrow
    3. Next match found → sleep until kickoff - 15 min
    4. Wake → live tracking (poll every 60s, upsert to DB)
    5. Match finished → poll for post-match data availability → scrape
    6. More matches today? → goto 3. No more → goto 1.

Daily maintenance (06:00 UTC):
    → FBref, Transfermarkt scrapers
    → cleanup_live_data(7)
    → Refresh materialized views (if they exist)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import logging.handlers
import os
import signal
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

# ── Paths ──
ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

# ── SofaScore tournament IDs ──
TOURNAMENT_IDS = {
    "EPL": 17, "LALIGA": 8, "BUNDESLIGA": 35,
    "SERIEA": 23, "LIGUE1": 34, "UCL": 7,
    "EREDIVISIE": 37, "LIGA_PORTUGAL": 238, "RFPL": 203,
}

# ── Source support matrix ──
LEAGUE_SOURCES = {
    "EPL":           {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LALIGA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "BUNDESLIGA":    {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "SERIEA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGUE1":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "RFPL":          {"understat": True,  "fbref": False, "sofascore": True,  "transfermarkt": True},
    "UCL":           {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "EREDIVISIE":    {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGA_PORTUGAL": {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
}

# ── Retry config ──
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = [60, 300, 900]  # 1min, 5min, 15min


# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════

def setup_logging(league: str) -> logging.Logger:
    """Per-league log file + console output."""
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)

    log = logging.getLogger(f"scheduler.{league}")
    log.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        f"%(asctime)s [{league}] %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    # File — 10 MB × 5 rotation
    fh = logging.handlers.RotatingFileHandler(
        log_dir / f"scheduler_{league.lower()}.log",
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
    """Send notifications via Discord webhook. Falls back to log-only."""

    EMOJI = {
        "match_start": "🟢", "goal": "⚽", "match_end": "🏁",
        "post_match_done": "📊", "error": "🔴", "daily_done": "🔧",
        "info": "ℹ️",
    }

    def __init__(self, league: str, log: logging.Logger):
        self.league = league
        self.log = log
        self.webhook_url = os.environ.get("DISCORD_WEBHOOK", "")

    def send(self, event_type: str, message: str) -> None:
        emoji = self.EMOJI.get(event_type, "📢")
        full_msg = f"{emoji} **[{self.league}]** {message}"
        self.log.info("NOTIFY [%s]: %s", event_type, message)

        if not self.webhook_url:
            return

        try:
            import urllib.request
            payload = json.dumps({"content": full_msg}).encode()
            req = urllib.request.Request(
                self.webhook_url, data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=10)
        except Exception as exc:
            self.log.warning("Discord send failed: %s", exc)


# ════════════════════════════════════════════════════════════
# SCHEDULE MANAGER — fetch today's matches from SofaScore
# ════════════════════════════════════════════════════════════

# Inline script spawned as subprocess to fetch schedule via nodriver (Cloudflare bypass).
# Uses document.body.innerText instead of HTML parsing because the JSON is ~700KB.
_FETCH_SCHEDULE_SCRIPT = r'''
import asyncio, json, sys, re

async def main():
    import nodriver as uc
    date_str = sys.argv[1]
    url = f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{date_str}"

    browser_args = [
        "--disable-dev-shm-usage", "--no-sandbox", "--disable-gpu",
        "--disable-software-rasterizer", "--disable-extensions",
        "--disable-background-networking", "--disable-default-apps",
        "--mute-audio", "--no-first-run", "--disable-sync",
        "--disable-translate", "--blink-settings=imagesEnabled=false"
    ]
    browser = await uc.start(browser_args=browser_args)
    # First visit sofascore.com to establish cookies
    tab = await browser.get("https://www.sofascore.com/")
    await asyncio.sleep(3)

    # Now fetch the API endpoint
    tab = await browser.get(url)
    await asyncio.sleep(2)

    try:
        txt = await tab.evaluate("document.body.innerText")
        if txt and txt.strip().startswith("{"):
            # Validate JSON before printing
            data = json.loads(txt)
            # Print only essential fields to keep stdout manageable
            filtered = {"events": []}
            for ev in data.get("events", []):
                filtered["events"].append({
                    "id": ev.get("id"),
                    "slug": ev.get("slug"),
                    "tournament": ev.get("tournament"),
                    "homeTeam": ev.get("homeTeam"),
                    "awayTeam": ev.get("awayTeam"),
                    "homeScore": ev.get("homeScore"),
                    "awayScore": ev.get("awayScore"),
                    "status": ev.get("status"),
                    "startTimestamp": ev.get("startTimestamp"),
                    "roundInfo": ev.get("roundInfo"),
                })
            print(json.dumps(filtered, ensure_ascii=False))
        else:
            print("{}")
    except Exception as e:
        print("{}", file=sys.stderr)
        print(f"Parse error: {e}", file=sys.stderr)
    finally:
        try:
            browser.stop()
        except Exception:
            pass

asyncio.run(main())
'''


class ScheduleManager:
    """Fetch and filter matches for a given date via SofaScore (nodriver bypass)."""

    def __init__(self, tournament_id: int, log: logging.Logger):
        self.tournament_id = tournament_id
        self.log = log

    def _fetch_raw(self, date_str: str) -> dict:
        """Spawn a nodriver subprocess to fetch schedule JSON."""
        script_path = ROOT / "_tmp_fetch_schedule.py"
        try:
            script_path.write_text(_FETCH_SCHEDULE_SCRIPT, encoding="utf-8")
            proc = subprocess.run(
                [PYTHON, str(script_path), date_str],
                cwd=str(ROOT), capture_output=True, text=True,
                timeout=120,
            )

            # Try to parse JSON from stdout regardless of return code
            # (nodriver emits benign "Event loop is closed" on stderr)
            if proc.stdout and proc.stdout.strip():
                for line in reversed(proc.stdout.strip().splitlines()):
                    line = line.strip()
                    if line.startswith("{"):
                        try:
                            data = json.loads(line)
                            if data.get("events"):
                                return data
                        except json.JSONDecodeError:
                            continue

            # Only warn if no JSON was found at all
            if proc.returncode != 0:
                stderr_tail = proc.stderr[-300:] if proc.stderr else "no stderr"
                self.log.warning("Schedule fetch subprocess failed (no JSON): %s",
                                 stderr_tail)
            return {}
        except subprocess.TimeoutExpired:
            self.log.warning("Schedule fetch timeout (120s)")
            return {}
        except Exception as exc:
            self.log.warning("Schedule fetch error: %s", exc)
            return {}
        finally:
            if script_path.exists():
                try:
                    script_path.unlink()
                except OSError:
                    pass

    def fetch_matches(self, date_str: str | None = None) -> list[dict]:
        """
        Fetch scheduled events for a date, filter by tournament_id.
        Returns list of dicts with: event_id, home, away, kickoff_utc, status.
        """
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self.log.info("Fetching schedule for %s (tournament %d)...",
                      date_str, self.tournament_id)

        data = self._fetch_raw(date_str)
        if not data:
            return []

        matches = []
        for ev in data.get("events", []):
            tournament = ev.get("tournament", {})
            unique_t = tournament.get("uniqueTournament", {})
            if unique_t.get("id") != self.tournament_id:
                continue

            home = ev.get("homeTeam", {})
            away = ev.get("awayTeam", {})
            status = ev.get("status", {})
            hs = ev.get("homeScore", {})
            aws = ev.get("awayScore", {})
            kickoff_ts = ev.get("startTimestamp", 0)

            matches.append({
                "event_id": ev.get("id"),
                "home_team": home.get("name", "?"),
                "away_team": away.get("name", "?"),
                "home_score": hs.get("current"),
                "away_score": aws.get("current"),
                "status": status.get("type", ""),
                "kickoff_utc": datetime.fromtimestamp(kickoff_ts, tz=timezone.utc) if kickoff_ts else None,
                "kickoff_ts": kickoff_ts,
                "round": ev.get("roundInfo", {}).get("round", 0),
                "slug": ev.get("slug", ""),
            })

        matches.sort(key=lambda m: m.get("kickoff_ts", 0))
        self.log.info("Schedule %s: %d matches for tournament %d",
                      date_str, len(matches), self.tournament_id)
        for m in matches:
            kt = m["kickoff_utc"].strftime("%H:%M UTC") if m["kickoff_utc"] else "?"
            self.log.info("  • %s vs %s @ %s [%s]",
                          m["home_team"], m["away_team"], kt, m["status"])
        return matches

    def get_upcoming(self) -> list[dict]:
        """Get today's + tomorrow's unfinished matches, sorted by kickoff."""
        now = datetime.now(timezone.utc)
        today = now.strftime("%Y-%m-%d")
        tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")

        matches = self.fetch_matches(today) + self.fetch_matches(tomorrow)

        # Filter: only notstarted or inprogress
        upcoming = [m for m in matches if m["status"] in ("notstarted", "inprogress")]
        # Deduplicate by event_id
        seen = set()
        unique = []
        for m in upcoming:
            if m["event_id"] not in seen:
                seen.add(m["event_id"])
                unique.append(m)
        unique.sort(key=lambda m: m.get("kickoff_ts", 0))
        return unique


# ════════════════════════════════════════════════════════════
# LIVE TRACKER — automated live match polling
# ════════════════════════════════════════════════════════════

class LiveTracker:
    """Non-interactive live match tracker wrapping live_match.py logic."""

    def __init__(self, league: str, log: logging.Logger, notifier: Notifier,
                 *, dry_run: bool = False):
        self.league = league
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run

    async def track(self, match: dict, shutdown: asyncio.Event) -> bool:
        """
        Track a match live until it finishes.
        Returns True if match was tracked successfully.
        """
        event_id = match["event_id"]
        home = match["home_team"]
        away = match["away_team"]

        self.log.info("═" * 50)
        self.log.info("LIVE TRACKING: %s vs %s (event=%d)", home, away, event_id)
        self.log.info("═" * 50)

        self.notifier.send("match_start",
                           f"{home} vs {away} — Live tracking started")

        if self.dry_run:
            self.log.info("[DRY-RUN] Would track event %d", event_id)
            return True

        cmd = [
            PYTHON, "live_match.py",
            str(event_id),
            "--interval", "60",
            "--save-db",
        ]

        try:
            proc = subprocess.run(
                cmd, cwd=str(ROOT), capture_output=True, text=True,
                timeout=6 * 3600,  # 6h max for extra time / delays
            )
            if proc.returncode == 0:
                self.log.info("✓ Live tracking finished for %s vs %s", home, away)
                self.notifier.send("match_end",
                                   f"{home} vs {away} — Match finished")
                return True
            else:
                self.log.error("✗ Live tracking exit code %d", proc.returncode)
                if proc.stderr:
                    self.log.debug("stderr: %s", proc.stderr[-500:])
                return False
        except subprocess.TimeoutExpired:
            self.log.error("✗ Live tracking timeout (6h)")
            return False
        except Exception as exc:
            self.log.error("✗ Live tracking error: %s", exc)
            return False


# ════════════════════════════════════════════════════════════
# SUBPROCESS RUNNER — with retry logic
# ════════════════════════════════════════════════════════════

def run_with_retry(cmd: list[str], cwd: Path, label: str,
                   log: logging.Logger, *, dry_run: bool = False,
                   timeout: int = 7200) -> bool:
    """Run a subprocess with retry logic: 3 attempts, backoff [60, 300, 900]."""
    if dry_run:
        log.info("[DRY-RUN] %s — %s", label, " ".join(cmd))
        return True

    for attempt in range(MAX_ATTEMPTS):
        log.info("▶ %s (attempt %d/%d)", label, attempt + 1, MAX_ATTEMPTS)
        t0 = time.perf_counter()
        try:
            proc = subprocess.run(
                cmd, cwd=str(cwd), capture_output=True, text=True,
                timeout=timeout,
            )
            elapsed = time.perf_counter() - t0

            if proc.returncode == 0:
                log.info("✓ %s — %.1fs", label, elapsed)
                return True
            else:
                log.warning("✗ %s — exit code %d (%.1fs)", label, proc.returncode, elapsed)
                if proc.stderr:
                    log.debug("  stderr: %s", proc.stderr[-500:])
        except subprocess.TimeoutExpired:
            log.warning("✗ %s — timeout %ds", label, timeout)
        except Exception as exc:
            log.warning("✗ %s — %s", label, exc)

        # Backoff before retry (skip on last attempt)
        if attempt < MAX_ATTEMPTS - 1:
            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            log.info("  ⏳ Retrying in %ds...", wait)
            time.sleep(wait)

    log.error("✗ %s — FAILED after %d attempts", label, MAX_ATTEMPTS)
    return False


# ════════════════════════════════════════════════════════════
# POST-MATCH WORKER
# ════════════════════════════════════════════════════════════

class PostMatchWorker:
    """Run post-match scrapers after match ends."""

    def __init__(self, league: str, log: logging.Logger, notifier: Notifier,
                 *, dry_run: bool = False):
        self.league = league
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self.sources = LEAGUE_SOURCES.get(league, {})

    def run(self, match: dict) -> bool:
        """Run post-match pipeline: poll for data → scrape → DB load."""
        home = match["home_team"]
        away = match["away_team"]
        self.log.info("─" * 50)
        self.log.info("POST-MATCH: %s vs %s", home, away)
        self.log.info("─" * 50)

        ok = True

        # 1. Understat — poll until data available
        if self.sources.get("understat"):
            ok &= self._poll_and_scrape_understat(match)

        # 2. SofaScore post-match heatmaps (latest 5 matches)
        if self.sources.get("sofascore"):
            ok &= run_with_retry(
                [PYTHON, "sofascore_client.py", "--league", self.league,
                 "--match-limit", "5"],
                ROOT / "sofascore",
                f"PostMatch/SofaScore [{self.league}]",
                self.log, dry_run=self.dry_run,
            )

        # 3. DB Load — load new CSV data into PostgreSQL
        ok &= run_with_retry(
            [PYTHON, "-m", "db.loader", "--league", self.league],
            ROOT,
            f"PostMatch/DBLoad [{self.league}]",
            self.log, dry_run=self.dry_run,
        )

        # 4. Refresh materialized views
        self._refresh_views()

        if ok:
            self.notifier.send("post_match_done",
                               f"Post-match data collected for {home} vs {away}")
        else:
            self.notifier.send("error",
                               f"Post-match pipeline had errors for {home} vs {away}")
        return ok

    def _poll_and_scrape_understat(self, match: dict) -> bool:
        """Poll Understat every 5 min until data is available (max 3 attempts)."""
        for attempt in range(MAX_ATTEMPTS):
            self.log.info("  Polling Understat (attempt %d/%d)...",
                          attempt + 1, MAX_ATTEMPTS)

            ok = run_with_retry(
                [PYTHON, "async_scraper.py", "--league", self.league],
                ROOT / "understat",
                f"PostMatch/Understat [{self.league}]",
                self.log, dry_run=self.dry_run,
            )
            if ok:
                return True

            if attempt < MAX_ATTEMPTS - 1:
                self.log.info("  ⏳ Waiting 5min for Understat data...")
                time.sleep(300)

        self.log.warning("  Understat data not available after polling")
        return False

    def _refresh_views(self) -> None:
        """Refresh materialized views if they exist."""
        if self.dry_run:
            self.log.info("[DRY-RUN] Would refresh materialized views")
            return

        try:
            sys.path.insert(0, str(ROOT))
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
                    self.log.debug("  MV %s does not exist yet, skipping", mv)

            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("MV refresh error: %s", exc)


# ════════════════════════════════════════════════════════════
# DAILY MAINTENANCE
# ════════════════════════════════════════════════════════════

class DailyMaintenance:
    """Run once per day: FBref, Transfermarkt, cleanup, MV refresh."""

    def __init__(self, league: str, log: logging.Logger, notifier: Notifier,
                 *, dry_run: bool = False):
        self.league = league
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self.sources = LEAGUE_SOURCES.get(league, {})
        self.last_run_date: str | None = None

    def is_due(self) -> bool:
        """Check if daily maintenance should run (once per calendar day UTC)."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return self.last_run_date != today

    def run(self) -> bool:
        """Execute daily maintenance tasks."""
        self.log.info("═" * 50)
        self.log.info("DAILY MAINTENANCE")
        self.log.info("═" * 50)

        ok = True

        # 1. FBref
        if self.sources.get("fbref"):
            ok &= run_with_retry(
                [PYTHON, "fbref_scraper.py", "--league", self.league],
                ROOT / "fbref",
                f"Daily/FBref [{self.league}]",
                self.log, dry_run=self.dry_run,
            )

        # 2. Transfermarkt
        if self.sources.get("transfermarkt"):
            ok &= run_with_retry(
                [PYTHON, "tm_scraper.py", "--league", self.league],
                ROOT / "transfermarkt",
                f"Daily/Transfermarkt [{self.league}]",
                self.log, dry_run=self.dry_run,
            )

        # 3. DB Load
        ok &= run_with_retry(
            [PYTHON, "-m", "db.loader", "--league", self.league],
            ROOT,
            f"Daily/DBLoad [{self.league}]",
            self.log, dry_run=self.dry_run,
        )

        # 4. Cleanup live data (> 7 days old)
        self._cleanup_live_data()

        # 5. Refresh materialized views
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
            sys.path.insert(0, str(ROOT))
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
            sys.path.insert(0, str(ROOT))
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
                    self.log.debug("  MV %s does not exist yet, skipping", mv)
            cur.close()
            conn.close()
        except Exception as exc:
            self.log.warning("MV refresh error: %s", exc)


# ════════════════════════════════════════════════════════════
# MAIN SCHEDULER LOOP
# ════════════════════════════════════════════════════════════

class Scheduler:
    """Match-schedule-aware daemon. One league, runs forever."""

    def __init__(self, league: str, *, dry_run: bool = False):
        self.league = league.upper()
        self.dry_run = dry_run
        self.log = setup_logging(self.league)
        self.notifier = Notifier(self.league, self.log)

        tournament_id = TOURNAMENT_IDS.get(self.league)
        if not tournament_id:
            raise ValueError(f"Unknown league: {self.league}")

        self.schedule = ScheduleManager(tournament_id, self.log)
        self.tracker = LiveTracker(self.league, self.log, self.notifier,
                                   dry_run=dry_run)
        self.post_match = PostMatchWorker(self.league, self.log, self.notifier,
                                          dry_run=dry_run)
        self.daily = DailyMaintenance(self.league, self.log, self.notifier,
                                      dry_run=dry_run)
        self._shutdown = asyncio.Event()

    def _install_signals(self) -> None:
        """Graceful shutdown on SIGINT / SIGTERM."""
        def handler(*_):
            self.log.info("Shutdown signal received...")
            self._shutdown.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)
        if hasattr(signal, "SIGBREAK"):
            signal.signal(signal.SIGBREAK, handler)

    def _sleep_until(self, target_utc: datetime, reason: str) -> bool:
        """
        Sleep until target time (interruptible by shutdown signal).
        Returns False if shutdown was requested.
        """
        now = datetime.now(timezone.utc)
        delta = (target_utc - now).total_seconds()
        if delta <= 0:
            return True

        self.log.info("💤 Sleeping until %s UTC (%s) — %s",
                      target_utc.strftime("%Y-%m-%d %H:%M"),
                      self._fmt_duration(delta), reason)

        # Sleep in 30-second chunks for responsiveness
        deadline = time.monotonic() + delta
        while time.monotonic() < deadline:
            if self._shutdown.is_set():
                return False
            remaining = deadline - time.monotonic()
            time.sleep(min(30, max(0, remaining)))

        return not self._shutdown.is_set()

    def _next_morning_utc(self) -> datetime:
        """Return next 06:00 UTC + Jitter offset."""
        now = datetime.now(timezone.utc)
        jitter_minute = TOURNAMENT_IDS[self.league] % 60
        morning = now.replace(hour=6, minute=jitter_minute, second=0, microsecond=0)
        if morning <= now:
            morning += timedelta(days=1)
        return morning

    @staticmethod
    def _fmt_duration(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds / 60:.0f}m"
        else:
            h = int(seconds // 3600)
            m = int((seconds % 3600) // 60)
            return f"{h}h{m:02d}m"

    async def run(self) -> None:
        """Main scheduler loop — runs forever."""
        self._install_signals()

        self.log.info("═" * 60)
        self.log.info("SCHEDULER STARTED — %s", self.league)
        self.log.info("═" * 60)
        self.log.info("  League:       %s", self.league)
        self.log.info("  Tournament:   %d", TOURNAMENT_IDS[self.league])
        self.log.info("  Dry run:      %s", self.dry_run)
        self.log.info("  PID:          %d", os.getpid())
        self.log.info("  Log:          logs/scheduler_%s.log", self.league.lower())
        self.log.info("  Discord:      %s", "YES" if self.notifier.webhook_url else "NO")
        self.log.info("═" * 60)

        self.notifier.send("info", f"Scheduler started (PID={os.getpid()})")

        while not self._shutdown.is_set():
            try:
                await self._cycle()
            except Exception as exc:
                self.log.exception("Unexpected error in main cycle: %s", exc)
                self.notifier.send("error", f"Cycle error: {exc}")
                # Don't crash — wait 60s and retry
                self.log.info("Retrying in 60s...")
                for _ in range(60):
                    if self._shutdown.is_set():
                        break
                    time.sleep(1)

        self.log.info("═" * 60)
        self.log.info("SCHEDULER STOPPED — %s", self.league)
        self.log.info("═" * 60)
        self.notifier.send("info", "Scheduler stopped")

    async def _cycle(self) -> None:
        """One full cycle: check schedule → track matches → post-match."""
        now = datetime.now(timezone.utc)
        self.log.info("─" * 50)
        self.log.info("CYCLE START — %s", now.strftime("%Y-%m-%d %H:%M UTC"))

        # ── Daily maintenance (with Jitter) ──
        if self.daily.is_due():
            jitter_minute = TOURNAMENT_IDS[self.league] % 60
            if now.hour > 6 or (now.hour == 6 and now.minute >= jitter_minute):
                self.log.info("⏰ Jitter offset met: %02d minutes past 06:00", jitter_minute)
                self.daily.run()

        # ── Fetch schedule ──
        upcoming = self.schedule.get_upcoming()

        if not upcoming:
            self.log.info("No upcoming matches found")
            # Sleep until tomorrow 06:00 UTC
            morning = self._next_morning_utc()
            if not self._sleep_until(morning, "no matches today"):
                return
            return

        # ── Process each match in chronological order ──
        for match in upcoming:
            if self._shutdown.is_set():
                return

            kickoff = match.get("kickoff_utc")
            home = match["home_team"]
            away = match["away_team"]
            event_id = match["event_id"]

            if not kickoff:
                self.log.warning("Match %s vs %s has no kickoff time, skipping",
                                 home, away)
                continue

            now = datetime.now(timezone.utc)

            # Already in progress → start tracking immediately
            if match["status"] == "inprogress":
                self.log.info("Match %s vs %s is already in progress!", home, away)
            else:
                # Sleep until 15 min before kickoff
                wake_time = kickoff - timedelta(minutes=15)
                if wake_time > now:
                    if not self._sleep_until(wake_time,
                                             f"waiting for {home} vs {away}"):
                        return

            # ── Live tracking ──
            await self.tracker.track(match, self._shutdown)

            if self._shutdown.is_set():
                return

            # ── Post-match pipeline ──
            self.post_match.run(match)

        # ── All matches done — check if more coming later today ──
        self.log.info("All scheduled matches processed for this cycle")

        # Brief pause before next cycle check
        for _ in range(60):
            if self._shutdown.is_set():
                return
            time.sleep(1)


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
        description="Vertex Football Scheduler — Match-aware 24/7 daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scheduler.py --league EPL
  python scheduler.py --league LALIGA
  python scheduler.py --league EPL --dry-run
  python scheduler.py --league EPL --test-notify

Supported leagues:
  EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1, UCL,
  EREDIVISIE, LIGA_PORTUGAL, RFPL
        """,
    )
    parser.add_argument(
        "--league", required=True,
        help="League ID (e.g. EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log schedule & sleep times without running scrapers",
    )
    parser.add_argument(
        "--test-notify", action="store_true",
        help="Send test Discord notification and exit",
    )

    args = parser.parse_args()
    league = args.league.upper()

    if league not in TOURNAMENT_IDS:
        print(f"Unknown league: {league}")
        print(f"Supported: {', '.join(sorted(TOURNAMENT_IDS.keys()))}")
        sys.exit(1)

    # ── Test notification mode ──
    if args.test_notify:
        log = setup_logging(league)
        notifier = Notifier(league, log)
        if not notifier.webhook_url:
            print("DISCORD_WEBHOOK not set in .env — notification test skipped")
            print("Set DISCORD_WEBHOOK=https://discord.com/api/webhooks/... in .env")
            sys.exit(1)
        notifier.send("info", "🧪 Test notification from scheduler.py — OK!")
        print("✓ Test notification sent to Discord")
        sys.exit(0)

    # ── Main scheduler ──
    scheduler = Scheduler(league, dry_run=args.dry_run)
    asyncio.run(scheduler.run())


if __name__ == "__main__":
    main()
