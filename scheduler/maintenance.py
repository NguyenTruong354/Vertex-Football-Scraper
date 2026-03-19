import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

from core.config import LEAGUE_SOURCES, ROOT, PYTHON
from core.runner import run_with_retry
from core.utils import Notifier
from services import insight_producer

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
        ai_stats = {"total": 0, "succeeded": 0, "queued": 0, "dropped": 0, "failed": 0}
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
                f"⏳ Queued: **{ai_stats['queued']}**\n"
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
