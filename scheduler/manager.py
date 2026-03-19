import json
import logging
from datetime import datetime, timedelta, timezone

from network.http_client import CurlCffiClient


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
