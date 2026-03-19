# Project Findings

## API & Database Integration
- Currently using `psycopg2` which handles queries synchronously.
- Goal is to use `asyncpg` to take advantage of native asyncio loops in the master scheduler.
- In `scheduler_master.py` lines 431-465, we use `psycopg2.extras.execute_batch` to insert lineups.
- In `scheduler/live_pool.py` lines 649-688, we use `psycopg2.extras.execute_batch` to insert `live_matches_state`.

## Dependencies
- Need to install `asyncpg`.
- Need to keep `psycopg2-binary` for non-async modules like `db/loader.py` and `db/setup_db.py` because they are synchronous.
