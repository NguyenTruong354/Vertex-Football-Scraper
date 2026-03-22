# ============================================================
# scheduler_master.py — Single-Browser Multi-League Daemon (Refactored)
# ============================================================
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from core.antiban import AntiBanStateMachine, LiveMetrics
from core.config import BROWSER_RECYCLE_EVERY, TOURNAMENT_IDS
from core.utils import Notifier, setup_logging
from network.http_client import CurlCffiClient
from scheduler.live_pool import LiveTrackingPool
from scheduler.maintenance import DailyMaintenance
from scheduler.manager import ScheduleManager
from scheduler.post_match import PostMatchWorker

from services import insight_worker

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "sofascore") not in sys.path:
    sys.path.insert(0, str(ROOT / "sofascore"))

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

        # Global Resource Governors
        self.scraper_sem = asyncio.Semaphore(4)
        self.browser_sem = asyncio.Semaphore(1)
        self.db_write_sem = asyncio.Semaphore(2)

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

    async def _sleep_interruptible(self, seconds: float) -> bool:
        """Sleep for N seconds, return False if shutdown requested."""
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=seconds)
            return False
        except asyncio.TimeoutError:
            return True

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
        
        # Init async db pool
        from db.config_db import get_async_pool
        self.log.info("🔌 Connecting to PostgreSQL with asyncpg...")
        self.db_pool = await get_async_pool()
        self.pool.db_pool = self.db_pool  # Pass down to LiveTrackingPool
        
        # Init AI Orchestrator Singleton
        from services import insight_worker
        self.log.info("🔌 Initializing AI Orchestrator...")
        insight_worker.init_orchestrator()

        self.notifier.send(
            "info", f"Master started — {', '.join(self.leagues)} (PID={os.getpid()})"
        )

        try:
            tasks = [
                asyncio.create_task(self._live_loop()),
                asyncio.create_task(self._maintenance_loop()),
                asyncio.create_task(self._news_loop()),
                asyncio.create_task(self._ai_loop())
            ]
            await asyncio.gather(*tasks)
        except Exception as exc:
            self.log.exception("Master scheduler fatal error: %s", exc)
            self.notifier.send("error", f"Master scheduler fatal error: {exc}")
        finally:
            self.log.info("Cleaning up resources...")
            
            # Close AI Orchestrator Connections
            from services import insight_worker
            try:
                await insight_worker.shutdown_orchestrator()
            except Exception as e:
                self.log.warning("Error shutting down orchestrator: %s", e)

            # 1. Close DB Pool
            if hasattr(self, 'db_pool') and self.db_pool is not None:
                await self.db_pool.close()
                
            # 2. Stop Browser
            await self.browser.stop()
            
            # 3. Shutdown ThreadPoolExecutor (Crucial for process exit)
            if hasattr(self, '_post_match_executor'):
                self.log.info("Shutting down thread executor...")
                self._post_match_executor.shutdown(wait=False, cancel_futures=True)

            self.log.info("MASTER SCHEDULER STOPPED")
            self.notifier.send("info", "Master stopped")
            
            # Unregister signals to prevent redundant logs
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)

    async def _live_loop(self) -> None:
        self.log.info("Started Live Match Orchestration Loop")
        while not self._shutdown.is_set():
            now = datetime.now(timezone.utc)
            self.log.info("─" * 50)
            self.log.info("LIVE CYCLE START — %s", now.strftime("%Y-%m-%d %H:%M UTC"))

            # 1. Browser recycle (when pool is idle)
            if self.browser.needs_recycle() and self.pool.is_empty:
                await self.browser.recycle()
                self.notifier.send("recycle", "Browser recycled (memory cleanup)")

            # 2. Fetch schedule
            try:
                upcoming = await self.schedule.get_upcoming()
            except Exception as e:
                self.log.error("Live loop upcoming matches error: %s", e)
                if not await self._sleep_interruptible(60): return
                continue

            if not upcoming and self.pool.is_empty:
                next_check = now + timedelta(minutes=5)
                self.log.info("💤 No matches — sleeping 5m until %s UTC", next_check.strftime("%H:%M"))
                if not await self._sleep_interruptible(300): return
                continue

            # 3. Check lineups ~60min before kickoff
            await self._check_lineups(upcoming)

            # 4. Add upcoming matches to pool when ready
            for match in upcoming:
                kickoff = match.get("kickoff_utc")
                if not kickoff:
                    continue
                time_to_kickoff = (kickoff - datetime.now(timezone.utc)).total_seconds()

                if time_to_kickoff <= 900 or match["status"] == "inprogress":
                    self.pool.add_match(match)
                    await self._fetch_and_save_lineup(match)

            # 5. Poll active matches
            if not self.pool.is_empty:
                finished_matches = await self.pool.poll_all()
                for fm in finished_matches:
                    asyncio.get_event_loop().run_in_executor(
                        self._post_match_executor, self.post_match.run, fm
                    )
                self.log.info("⏳ Cycle complete for %d active matches. Next cycle shortly...", self.pool.active_count)
                if not await self._sleep_interruptible(5): return
                continue

            # 6. Sleep before next kickoff if pool is empty
            if upcoming:
                next_kickoff = min(m["kickoff_utc"] for m in upcoming if m.get("kickoff_utc"))
                wake_time = next_kickoff - timedelta(minutes=60)
                now = datetime.now(timezone.utc)
                if wake_time > now:
                    delta = (wake_time - now).total_seconds()
                    self.log.info("💤 Next match in %s — sleeping until %s UTC", self._fmt_duration(delta), wake_time.strftime("%H:%M"))
                    if not await self._sleep_interruptible(min(300, delta)): return
                else:
                    if not await self._sleep_interruptible(60): return
            else:
                if not await self._sleep_interruptible(60): return

    async def _maintenance_loop(self) -> None:
        self.log.info("Started Daily Maintenance Task Loop")
        while not self._shutdown.is_set():
            now = datetime.now(timezone.utc)
            if self.daily.is_due() and (now.hour >= 6 or self.force_daily_now):
                if self.pool.is_empty:
                    self.log.info("🚧 Running daily maintenance...")
                    self.notifier.send("info", "⚙️ Bắt đầu Daily Maintenance...")
                    t0 = time.monotonic()
                    self._lineup_fetched.clear()
                    try:
                        await self.daily.run(self.scraper_sem, self.browser_sem, self.db_write_sem)
                        if self.daily.last_run_date == now.strftime("%Y-%m-%d"):
                            duration = self._fmt_duration(time.monotonic() - t0)
                            self.notifier.send("info", f"✅ Hoàn thành Daily Maintenance! (Thời gian: {duration})")
                        elif self.daily._daily_attempt_count >= 3:
                            self.notifier.send("error", f"🚨 Daily Maintenance failed 3 lần hôm nay. Bỏ qua cho tới ngày mai.")
                    except Exception as e:
                        self.log.error("Maintenance failed: %s", e)
                else:
                    self.log.info("⏸️ Pool has %d active matches — deferring maintenance", self.pool.active_count)
            # Check every 5 minutes if due
            if not await self._sleep_interruptible(300):
                break

    async def _news_loop(self) -> None:
        self.log.info("Started News Radar Task Loop")
        while not self._shutdown.is_set():
            now = datetime.now(timezone.utc)
            if self.last_news_fetch is None or (now - self.last_news_fetch).total_seconds() >= 1800:
                if self.dry_run:
                    self.log.info("[DRY-RUN] Would fetch RSS news")
                else:
                    try:
                        from services import news_radar
                        self.log.info("📰 Starting RSS news radar fetch...")
                        saved = await asyncio.to_thread(news_radar.run_and_save)
                        if saved > 0:
                            self.log.info("✓ RSS news radar fetch complete: %d items", saved)
                        else:
                            self.log.info("✓ RSS news radar fetch complete: 0 new items")
                    except Exception as exc:
                        self.log.error("News radar error: %s", exc)
                self.last_news_fetch = datetime.now(timezone.utc)
            
            # Check shutdown every minute
            if not await self._sleep_interruptible(60):
                break

    async def _ai_loop(self) -> None:
        self.log.info("Started AI Worker Task Loop")
        while not self._shutdown.is_set():
            try:
                # FIX: Pass shutdown event to allow immediate break in worker loop
                processed = await insight_worker.run_worker_cycle(
                    shadow_mode=self.insight_shadow_mode,
                    max_jobs=15,
                    shutdown_event=self._shutdown
                )
                if processed > 0:
                    self.log.info("✓ AI cycle: processed %d jobs", processed)
                
            except Exception as e:
                self.log.error("AI worker loop error: %s", e)
            
            if not await self._sleep_interruptible(60):
                break

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
                            int(pi.get("jerseyNumber")) if pi.get("jerseyNumber") and str(pi.get("jerseyNumber")).isdigit() else 0,
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
            sql = """
                INSERT INTO match_lineups
                    (event_id, player_id, player_name, team_side, team_name,
                     position, jersey_number, is_substitute, minutes_played,
                     rating, formation, status, league_id, season)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
            
            try:
                await self.db_pool.executemany(sql, rows)
                self.log.info(
                    "  📋 Lineup saved: %s vs %s — %d players (event=%d)",
                    home,
                    away,
                    len(rows),
                    event_id,
                )
            except Exception as e:
                self.log.error("Error saving lineup with asyncpg: %s", e)


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
    try:
        asyncio.run(scheduler.run())
    finally:
        # Hard exit to ensure no zombie threads keep the process alive on Windows
        os._exit(0)


if __name__ == "__main__":
    main()
