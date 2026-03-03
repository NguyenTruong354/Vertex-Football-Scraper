import asyncio
import sys
import os
import time
from datetime import datetime, timezone

# Load environment variables
for line in open(".env", encoding="utf-8").read().splitlines():
    if line.strip() and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

import scheduler_master
from scheduler_master import setup_logging, Notifier, CurlCffiClient, ScheduleManager, LiveTrackingPool

async def run_simulation():
    log = setup_logging()
    log.info("🚀 STARTING EPL LIVE MATCH SIMULATOR")
    
    notifier = Notifier(log)
    client = CurlCffiClient(log, notifier, dry_run=False) # Dry run false to actually fetch data
    await client.start()

    try:
        # Create ScheduleManager & LiveTrackingPool
        tournaments = {"EPL": 17}
        schedule = ScheduleManager(tournaments, client, log)
        pool = LiveTrackingPool(client, log, notifier, dry_run=False) # Dry-run False: real tracking

        log.info("Fetching today's schedule...")
        matches = await schedule.get_upcoming()
        
        # Filter for Brentford and Burnley matches
        targets = []
        for m in matches:
            home = m["home_team"].lower()
            away = m["away_team"].lower()
            if "brentford" in home or "brentford" in away or "burnley" in home or "burnley" in away:
                targets.append(m)

        if not targets:
            log.error("Could not find Brentford or Burnley in today's schedule.")
            return

        log.info("Found %d target matches. Adding to Live Tracking Pool...", len(targets))
        for m in targets:
            pool.add_match(m)

        log.info("Starting forced poll loop (Ctrl+C to stop)...")
        # Run exactly 5 loops to prove it works without getting banned
        for cycle in range(1, 6):
            log.info("--- Cycle %d ---", cycle)
            
            # Patch SQL save temporarily so we don't write to DB during simulation
            # (Just doing empty mock function)
            old_save = pool._save_to_db
            pool._save_to_db = lambda state: log.info("[DB-MOCK] Would save state: %d-%d %s'", state.home_score, state.away_score, state.minute)
            
            await pool.poll_all()
            
            # Restore 
            pool._save_to_db = old_save
            
            log.info("Cycle %d done. Sleeping 15s...", cycle)
            await asyncio.sleep(15)
            
        log.info("Simulation finished 5 cycles successfully.")

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
    finally:
        await client.stop()
        log.info("Simulator shutdown.")


if __name__ == "__main__":
    asyncio.run(run_simulation())
