import argparse
import sys
import asyncio
import logging
import re
from pathlib import Path

# Add project root to sys.path so we can import from config, db, etc.
sys.path.append(str(Path(__file__).resolve().parent.parent))

from curl_cffi.requests import AsyncSession
import sofascore.config_sofascore as cfg
from db.config_db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("backfill_stats")

async def fetch_and_save_match_stats(session, event_id: int) -> bool:
    url = f"{cfg.SS_API_BASE}/event/{event_id}/statistics"
    
    try:
        resp = await session.get(url, timeout=15)
        if resp.status_code == 429:
            logger.warning(f"429 Rate Limit for event {event_id}. Need cooling.")
            return False

        if resp.status_code != 200:
            logger.warning(f"API error {resp.status_code}: {url}")
            return True # Not retriable
            
        data = resp.json()
        if not data or "statistics" not in data:
            logger.warning(f"No statistics found for event {event_id}")
            return True

        stats = next((s for s in data["statistics"] if s.get("period") == "ALL"), None)
        if not stats:
            logger.warning(f"No ALL period statistics found for event {event_id}")
            return True

        stat_map = {}
        for group in stats.get("groups", []):
            for item in group.get("statisticsItems", []):
                stat_map[item.get("name")] = item

        def extract_number(name: str, side: str) -> float:
            item = stat_map.get(name)
            if not item: return 0.0
            val = item.get(f"{side}Value")
            if val is not None: return float(val)
            s = str(item.get(side, "0"))
            if not s: return 0.0
            m = re.search(r"^(\d+\.?\d*)", s)
            return float(m.group(1)) if m else 0.0

        conn = get_connection()
        cur = conn.cursor()
        
        try:
            # Look up league id if possible to store correctly
            cur.execute("SELECT league_id FROM ss_events WHERE event_id = %s LIMIT 1", (event_id,))
            row = cur.fetchone()
            league_id = row[0] if row else None

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

                cur.execute("""
                    INSERT INTO match_team_stats
                        (event_id, side, possession, total_shots, shots_on_target, shots_off_target,
                         blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes, league_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        league_id = EXCLUDED.league_id,
                        loaded_at = NOW();
                """, (
                    event_id, side, possession, total_shots, shots_on_target, shots_off_target,
                    blocked_shots, corners, fouls, big_chances_created, passes, accurate_passes, league_id
                ))
            conn.commit()
            logger.info("✓ Inserted match_team_stats %s", event_id)
            return True
        except Exception as e:
            logger.error(f"DB Error for event {event_id}: {e}")
            conn.rollback()
            return False
        finally:
            cur.close()
            conn.close()
    except Exception as e:
        logger.error(f"Req error for {event_id}: {e}")
        return False


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, default="ALL")
    parser.add_argument("--season", type=str, default=None, help="Season year (e.g. 2024 or 2025)")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    
    query = """
        SELECT event_id
        FROM ss_events 
        WHERE status IN ('finished', '100', '106')
          AND NOT EXISTS (
              SELECT 1 FROM match_team_stats WHERE match_team_stats.event_id = ss_events.event_id
          )
    """
    
    params = []
    
    if args.league != "ALL":
        query += " AND league_id = %s"
        params.append(args.league)
        
        if args.season:
            from sofascore.config_sofascore import get_ss_config
            logger.info("Resolving SofaScore season ID for %s, season %s", args.league, args.season)
            try:
                ss_cfg = get_ss_config(args.league, season_override=args.season)
                query += " AND season_id = %s"
                params.append(ss_cfg.season_id)
                logger.info("  Season ID matched: %s", ss_cfg.season_id)
            except Exception as exc:
                logger.error("Failed to resolve season ID: %s. Reverting to all seasons for this run.", exc)
                
        cur.execute(query, tuple(params))
    else:
        if args.season:
            logger.warning("Filtering by season is only supported when a specific --league is provided. Running for ALL seasons.")
        cur.execute(query)
        
    rows = cur.fetchall()
    cur.close()
    conn.close()

    event_ids = [r[0] for r in rows]
    logger.info(f"��� Found {len(event_ids)} finished events missing match team stats.")

    if not event_ids:
        logger.info("No missing match stats to fetch!")
        return

    session = AsyncSession(impersonate="chrome120", headers=cfg.SS_HEADERS)
    batch_size = 10
    
    for i in range(0, len(event_ids), batch_size):
        batch = event_ids[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(event_ids)+batch_size-1)//batch_size}, events: {batch}")
        
        for eid in batch:
            res = await fetch_and_save_match_stats(session, eid)
            if not res: 
                logger.warning("  Sleeping 10s due to rate limit/error...")
                await asyncio.sleep(10)
            await asyncio.sleep(2.5) # Gentle request spacing

    await session.close()
    logger.info("��� Done backfilling match stats.")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
