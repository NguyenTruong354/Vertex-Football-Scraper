# Task Plan: Optimize Event Loop (Replace psycopg2 with asyncpg) (P1)

## Goal
Replace the synchronous `psycopg2` database driver with the asynchronous `asyncpg` driver in `scheduler_master.py` and `scheduler/live_pool.py`. This will prevent database operations from blocking the event loop and causing the scheduler to hang.

## Phases

### Phase 1: Setup and Dependencies (Status: COMPLETE)
- [x] Add `asyncpg` to `requirements.txt`.
- [x] Install `asyncpg` in the virtual environment.

### Phase 2: Async Database Configuration (Status: IN_PROGRESS)
- [ ] Update `db/config_db.py` to support `asyncpg` connection pools.
- [ ] Implement an asynchronous equivalent of `get_connection()` using an `asyncpg.Pool` or `asyncpg.connect`.


### Phase 3: Refactor `scheduler_master.py` (Status: COMPLETE)
- [x] Convert `_db_upsert` in `scheduler_master.py` (line ~430-480) from `psycopg2.extras.execute_batch` to `asyncpg` executemany.
- [x] Apply `await` to the new async database insertion logic.

### Phase 4: Refactor `scheduler/live_pool.py` (Status: COMPLETE)
- [x] Convert `_save_to_db` in `scheduler/live_pool.py` (line ~640-700) to use `asyncpg` async connections and `executemany`.
- [x] Replace `ThreadPoolExecutor` or `to_thread`/`run_in_executor` wrap with native `await pool.execute()`.

### Phase 5: Verification (Status: COMPLETE)
- [x] Verify `scheduler_master.py --dry-run` works without errors.
- [x] Ensure database insertions logic works without blocking the event loop.

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| | | |
