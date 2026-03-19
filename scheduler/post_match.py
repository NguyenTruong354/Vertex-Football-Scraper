import asyncio
import json
import logging
import re
import sys

from core.config import LEAGUE_SOURCES, TOURNAMENT_IDS
from core.runner import run_with_retry
from core.utils import Notifier
from network.http_client import CurlCffiClient

# Since ROOT and PYTHON were defined globally in scheduler_master
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

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
            from services import insight_producer

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
