#!/usr/bin/env python3
"""
Patch script to add auto-standings feature to scheduler_master.py on e2-micro.
Run this ONCE on the server, then restart the service.

Usage:
    python3 tools/maintenance/patch_standings.py
    sudo systemctl restart scheduler-master.service
"""
import re

TARGET = "/opt/vertex-football-scraper/scheduler_master.py"

# Read the file
with open(TARGET, "r", encoding="utf-8") as f:
    code = f.read()

changes = 0

# ── PATCH 1: Add browser parameter to PostMatchWorker.__init__ ──
old1 = '''class PostMatchWorker:
    def __init__(self, log: logging.Logger, notifier: Notifier,
                 *, dry_run: bool = False,
                 shutdown_event: asyncio.Event | None = None):
        self.log = log
        self.notifier = notifier
        self.dry_run = dry_run
        self._shutdown = shutdown_event'''

new1 = '''class PostMatchWorker:
    def __init__(self, log: logging.Logger, notifier: Notifier,
                 browser: CurlCffiClient | None = None,
                 *, dry_run: bool = False,
                 shutdown_event: asyncio.Event | None = None):
        self.log = log
        self.notifier = notifier
        self.browser = browser
        self.dry_run = dry_run
        self._shutdown = shutdown_event'''

if old1 in code:
    code = code.replace(old1, new1, 1)
    changes += 1
    print("✓ Patch 1: Added browser param to PostMatchWorker.__init__")
else:
    print("⚠ Patch 1: Already applied or not found")

# ── PATCH 2: Add standings update call + method after run() ──
old2 = '''        if ok:
            self.notifier.send("post_match_done",
                               f"[{league}] Post-match done: {home} vs {away}")
        else:
            self.notifier.send("error",
                               f"[{league}] Post-match errors: {home} vs {away}")
        return ok


# ════════════════════════════════════════════════════════════
# DAILY MAINTENANCE'''

new2 = '''        # Update standings from SofaScore API (no Chrome needed!)
        if self.browser:
            tournament_id = TOURNAMENT_IDS.get(league)
            if tournament_id:
                try:
                    import asyncio
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        asyncio.ensure_future(
                            self._update_standings_from_sofascore(league, tournament_id)
                        )
                    else:
                        loop.run_until_complete(
                            self._update_standings_from_sofascore(league, tournament_id)
                        )
                except Exception as exc:
                    self.log.warning("Standings update failed: %s", exc)

        if ok:
            self.notifier.send("post_match_done",
                               f"[{league}] Post-match done: {home} vs {away}")
        else:
            self.notifier.send("error",
                               f"[{league}] Post-match errors: {home} vs {away}")
        return ok

    async def _update_standings_from_sofascore(self, league: str, tournament_id: int) -> None:
        """Fetch latest standings from SofaScore API and upsert into DB."""
        self.log.info("📊 Fetching standings for %s from SofaScore API...", league)

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

        season_str = season_name
        m = re.search(r"(\\d{2})/(\\d{2})$", season_name)
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

                cur.execute("""
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
                """, (position, team_name, team_id, matches_played, wins, draws, losses,
                      goals_for, goals_against, goal_diff, points, points_avg,
                      league, season_str))

            conn.commit()
            cur.close()
            conn.close()

            self.log.info("✅ Standings updated: %s — %d teams (%s)", league, len(rows), season_str)

            top5 = sorted(rows, key=lambda r: r.get("position", 99))[:5]
            lines = [f"📊 **BXH {league}** (sau vòng đấu):"]
            for r in top5:
                t = r.get("team", {}).get("name", "?")
                p = r.get("position", 0)
                pts = r.get("points", 0)
                w, d, l = r.get("wins", 0), r.get("draws", 0), r.get("losses", 0)
                lines.append(f"  #{p} {t} — {pts}pts ({w}W {d}D {l}L)")
            self.notifier.send("info", "\\n".join(lines))

        except Exception as exc:
            self.log.error("Failed to upsert standings for %s: %s", league, exc)


# ════════════════════════════════════════════════════════════
# DAILY MAINTENANCE'''

if old2 in code:
    code = code.replace(old2, new2, 1)
    changes += 1
    print("✓ Patch 2: Added _update_standings_from_sofascore method")
else:
    print("⚠ Patch 2: Already applied or not found")

# ── PATCH 3: Wire browser into PostMatchWorker in MasterScheduler.__init__ ──
old3 = 'self.post_match = PostMatchWorker(self.log, self.notifier, dry_run=dry_run,'
new3 = 'self.post_match = PostMatchWorker(self.log, self.notifier,\n                                           browser=self.browser, dry_run=dry_run,'

if old3 in code:
    code = code.replace(old3, new3, 1)
    changes += 1
    print("✓ Patch 3: Wired browser into PostMatchWorker constructor")
else:
    print("⚠ Patch 3: Already applied or not found")

# Write back
if changes > 0:
    with open(TARGET, "w", encoding="utf-8") as f:
        f.write(code)
    print(f"\n🎉 Applied {changes} patches successfully!")
    print("Now run: sudo systemctl restart scheduler-master.service")
else:
    print("\n⚠ No changes made — patches may have already been applied.")
