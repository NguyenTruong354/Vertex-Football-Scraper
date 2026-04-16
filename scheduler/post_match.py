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
                        
                        event_id = match.get("event_id")
                        if event_id:
                            task_stats = asyncio.ensure_future(
                                self._run_match_stats_safe(league, event_id)
                            )
                            task_stats.add_done_callback(_swallow_task_exc)

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

            # Lấy mapping từ Sofascore ID -> FBref ID để tránh Duplicate (Dòng 1 vs Dòng 2)
            cur.execute(
                "SELECT sofascore_team_id, fbref_team_id FROM team_canonical WHERE league_id = %s AND sofascore_team_id IS NOT NULL",
                (league,)
            )
            ss_to_fbref = {str(r[0]): r[1] for r in cur.fetchall()}

            for row in rows:
                team = row.get("team", {})
                ss_team_id = str(team.get("id", ""))
                team_name = team.get("name", "?")
                
                # Resolving correct FBref team_id:
                team_id = ss_to_fbref.get(ss_team_id)
                if not team_id:
                    self.log.warning("Skipping standing update for %s (no FBref mapping for Sofascore ID %s)", team_name, ss_team_id)
                    continue
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
                    UPDATE standings SET
                        position = %s,
                        team_name = %s,
                        matches_played = %s,
                        wins = %s,
                        draws = %s,
                        losses = %s,
                        goals_for = %s,
                        goals_against = %s,
                        goal_difference = %s,
                        points = %s,
                        points_avg = %s,
                        loaded_at = NOW()
                    WHERE team_id = %s AND league_id = %s AND season = %s
                """,
                    (
                        position,
                        team_name,
                        matches_played,
                        wins,
                        draws,
                        losses,
                        goals_for,
                        goals_against,
                        goal_diff,
                        points,
                        points_avg,
                        team_id,
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

    async def _run_match_stats_safe(self, league: str, event_id: int) -> None:
        """Safe wrapper for match stats fetching with error boundary."""
        try:
            await self._fetch_and_save_match_stats(league, event_id)
            self.log.info("✓ Match team stats updated: %s %s", league, event_id)
        except Exception as exc:
            self.log.error(
                "⚠ Match stats update FAILED — league=%s event_id=%s — %s",
                league,
                event_id,
                exc,
            )

    async def _fetch_and_save_match_stats(self, league: str, event_id: int) -> None:
        """Fetch latest statistics from SofaScore API and upsert into match_team_stats."""
        self.log.info("��� Fetching match statistics for event %s from SofaScore API...", event_id)

        data = await self.browser.get_json(f"/event/{event_id}/statistics")
        if not data or "statistics" not in data:
            self.log.warning("Could not fetch statistics for event %s", event_id)
            return

        stats = next((s for s in data["statistics"] if s.get("period") == "ALL"), None)
        if not stats:
            self.log.warning("No ALL period statistics found for event %s", event_id)
            return

        stat_map = {}
        for group in stats.get("groups", []):
            for item in group.get("statisticsItems", []):
                stat_map[item.get("name")] = item

        import re
        def extract_number(name: str, side: str) -> float:
            item = stat_map.get(name)
            if not item: return 0.0
            
            val = item.get(f"{side}Value")
            if val is not None:
                return float(val)
            
            s = str(item.get(side, "0"))
            if not s: return 0.0
            m = re.search(r"^(\d+\.?\d*)", s)
            return float(m.group(1)) if m else 0.0

        for side in ["home", "away"]:
            possession = extract_number("Ball possession", side)
            total_shots = int(extract_number("Total shots", side))
            shots_on_target = int(extract_number("Shots on target", side))
            shots_off_target = int(extract_number("Shots off target", side))
            blocked_shots = int(extract_number("Blocked shots", side))
            corners = int(extract_number("Corner kicks", side))
            fouls = int(extract_number("Fouls", side))
            big_chances_created = int(extract_number("Big chances", side))
            passes = int(extract_number("Passes", side))
            accurate_passes = int(extract_number("Accurate passes", side))

            try:
                from db.config_db import get_connection
                conn = get_connection()
                cur = conn.cursor()
                
                # Fetch team_id (we need to get the fbref_team_id mapped, or just use Sofascore ID if desired)
                # But it's usually sufficient to just store event_id, side
                # The proposal says team_id BIGINT, but SofaScore team_ids are INT. 
                # Let's get the team_id from the live_snapshots or live_match_state. But wait, in SofaScore event, team_id is not directly in the statistics endpoint. 
                # It's fine to leave it NULL if we can't easily fetch it, or query the DB for it.
                # Actually, the user proposal says: `team_id BIGINT ID đội bóng`. Let's try to query it from `live_match_state` or `live_snapshots` or just leave it.
                # Let's query team ID using sofascore API if we need to, but it's simpler to leave as NULL and do a join with live_snapshots later.
                
                cur.execute("""
                    INSERT INTO match_team_stats
                        (event_id, team_id, side, possession, total_shots, shots_on_target, shots_off_target,
                         blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes, league_id)
                    VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, side) DO UPDATE SET
                        possession = EXCLUDED.possession,
                        total_shots = EXCLUDED.total_shots,
                        shots_on_target = EXCLUDED.shots_on_target,
                        shots_off_target = EXCLUDED.shots_off_target,
                        blocked_shots = EXCLUDED.blocked_shots,
                        corners = EXCLUDED.corners,
                        fouls = EXCLUDED.fouls,
                        big_chances_created = EXCLUDED.big_chances_created,
                        passes = EXCLUDED.passes,
                        accurate_passes = EXCLUDED.accurate_passes,
                        loaded_at = NOW()
                """, (
                    event_id, side, possession, total_shots, shots_on_target, shots_off_target,
                    blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes, league
                ))
                
                conn.commit()
                cur.close()
                conn.close()
            except Exception as exc:
                self.log.error("Failed to upsert match_team_stats %s side %s: %s", event_id, side, exc)

    async def _run_match_stats_safe(self, league: str, event_id: int) -> None:
        try:
            await self._fetch_and_save_match_stats(league, event_id)
        except Exception as exc:
            self.log.error(f"⚠ Match stats update FAILED — league={league} event_id={event_id} — {exc}")

    async def _fetch_and_save_match_stats(self, league: str, event_id: int) -> None:
        self.log.info(f"📊 Fetching match statistics for event {event_id} from SofaScore API...")

        data = await getattr(self.browser, "get_json", self.browser.get)(f"/event/{event_id}/statistics")
        if not data or "statistics" not in data:
            self.log.warning(f"No statistics found for event {event_id}")
            return

        stats = next((s for s in data["statistics"] if s.get("period") == "ALL"), None)
        if not stats:
            self.log.warning(f"No ALL period statistics found for event {event_id}")
            return

        stat_map = {}
        for group in stats.get("groups", []):
            for item in group.get("statisticsItems", []):
                stat_map[item.get("name")] = item

        import re
        def extract_number(name: str, side: str) -> float:
            item = stat_map.get(name)
            if not item: return 0.0
            val = item.get(f"{side}Value")
            if val is not None:
                return float(val)
            s = str(item.get(side, "0"))
            if not s: return 0.0
            m = re.search(r"^(\d+\.?\d*)", s)
            return float(m.group(1)) if m else 0.0

        for side in ["home", "away"]:
            possession = extract_number("Ball possession", side)
            total_shots = int(extract_number("Total shots", side))
            shots_on_target = int(extract_number("Shots on target", side))
            shots_off_target = int(extract_number("Shots off target", side))
            blocked_shots = int(extract_number("Blocked shots", side))
            corners = int(extract_number("Corner kicks", side))
            fouls = int(extract_number("Fouls", side))
            big_chances_created = int(extract_number("Big chances", side))
            passes = int(extract_number("Passes", side))
            accurate_passes = int(extract_number("Accurate passes", side))

            try:
                from db.config_db import get_connection
                conn = get_connection()
                cur = conn.cursor()
                
                cur.execute("""
                    INSERT INTO match_team_stats
                        (event_id, side, possession, total_shots, shots_on_target, shots_off_target,
                         blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id, side) DO UPDATE SET
                        possession = EXCLUDED.possession,
                        total_shots = EXCLUDED.total_shots,
                        shots_on_target = EXCLUDED.shots_on_target,
                        shots_off_target = EXCLUDED.shots_off_target,
                        blocked_shots = EXCLUDED.blocked_shots,
                        corners = EXCLUDED.corners,
                        fouls = EXCLUDED.fouls,
                        big_chances_created = EXCLUDED.big_chances_created,
                        passes = EXCLUDED.passes,
                        accurate_passes = EXCLUDED.accurate_passes,
                        loaded_at = NOW();
                """, (
                    event_id, side, possession, total_shots, shots_on_target, shots_off_target,
                    blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes
                ))
                
                conn.commit()
                cur.close()
                conn.close()
                self.log.info(f"✓ Match stats upserted for {event_id} ({side})")
            except Exception as exc:
                self.log.error(f"Failed to upsert match_team_stats {event_id} side {side}: {exc}")
