# Project Findings

## API & Database Integration
- Currently using `psycopg2` which handles queries synchronously.
- Goal is to use `asyncpg` to take advantage of native asyncio loops in the master scheduler.
- In `scheduler_master.py` lines 431-465, we use `psycopg2.extras.execute_batch` to insert lineups.
- In `scheduler/live_pool.py` lines 649-688, we use `psycopg2.extras.execute_batch` to insert `live_matches_state`.

## Dependencies
- Need to install `asyncpg`.
## 📊 Database Performance Insights (2026-03-20)
- **Vấn đề**: Database Load tăng đột biến (>3000%) do Scraper chạy cường độ cao.
- **Tối ưu Index**: Aiven phát hiện 2 Index dư thừa cần xóa:
    - `idx_live_inc_event` (public.live_incidents)
    - `idx_crossref_fb` (public.player_crossref)
- **Hành động sau này**:
    - Thực hiện `DROP INDEX` cho 2 index trên.
    - Tạo Composite Index cho bảng `heatmaps` trên 2 cột `(event_id, player_id)`.
    - Kiểm tra các Index trên bảng `shots` vì tốc độ ghi đang rất chậm (6s+).
