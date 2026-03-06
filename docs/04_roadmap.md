# Vertex Football Scraper — Roadmap
> Cập nhật: 01/03/2026

---

## Tổng quan tiến độ

```
PHASE 1 ─────────── PHASE 2 ──────────── PHASE 3 ──────────── PHASE 4
EPL Only             Top 5 EU + UCL       Job Queue + API       Global Scale
CSV Output           PostgreSQL           Celery + Redis        Kubernetes
1 scraper            4 scrapers           FastAPI               Data Lake
[✅ XONG]            [✅ XONG]             [🔲 Tiếp theo]        [🔲 Dài hạn]
```

---

## ✅ PHASE 1 — Foundation (HOÀN THÀNH)

> EPL only, CSV output, 1 scraper pipeline.

- [x] Understat pipeline — Shot xG, Player xG/xA/xGChain, Match aggregates
- [x] FBref pipeline — Standings, Squad stats, Player profiles, Player season stats
- [x] Cloudflare bypass với `nodriver` (headed Chrome)
- [x] Pydantic v2 validation cho tất cả data
- [x] CSV output (7 files)
- [x] Documentation đầy đủ

---

## ✅ PHASE 2 — Multi-League + PostgreSQL (HOÀN THÀNH)

> 4 nguồn scraping, multi-league, PostgreSQL, live tracking.

### 2.1 League Registry ✅
- [x] `league_registry.py` — Single source of truth cho tất cả giải đấu
- [x] 9 leagues: EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1, RFPL, UCL, EREDIVISIE, LIGA_PORTUGAL
- [x] Thêm giải mới chỉ cần 1 entry

### 2.2 Multi-Source Scrapers ✅
- [x] **Understat** — refactor nhận `--league`, `--season` param
- [x] **FBref** — dynamic URL theo `comp_id`; 6 loại data: standings, squad_stats, rosters, player_season_stats, fixtures, gk_stats
- [x] **SofaScore** — nodriver; events, heatmaps, avg_positions, lineups, ratings
- [x] **Transfermarkt** — nodriver; team_metadata, market_values

### 2.3 PostgreSQL Database ✅
- [x] `db/schema.sql` — 17 bảng + indexes + FK constraints
- [x] `db/config_db.py` — psycopg2 connection via dotenv
- [x] `db/loader.py` — 15 loaders + crossref builder; upsert ON CONFLICT
- [x] `db/setup_db.py` — tạo DB + schema trong 1 lệnh
- [x] `db/queries.py` — query helpers
- [x] Tested: 628 rows loaded, cross-source JOIN hoạt động

### 2.4 Player ID Mapping ✅
- [x] `player_crossref` table — Understat INTEGER ↔ FBref TEXT slug
- [x] Auto-build bằng fuzzy name matching trong `db/loader.py`
- [x] Query mẫu join xG + player stats đã verify

### 2.5 Pipeline Orchestrator ✅
- [x] `run_pipeline.py` — chạy 4 scrapers + load DB trong 1 lệnh
- [x] `autorun.bat` (Windows) — bat file cho Task Scheduler
- [x] `run_daemon.py` — 24/7 daemon với 3-tier scheduling
- [x] `daemon.bat` — Windows bat file cho daemon

### 2.6 Live Match Tracker ✅
- [x] `live_match.py` (~1,039 lines) — real-time live match tracking
- [x] Terminal dashboard: score banner, incidents, bar chart stats, lineups
- [x] Poll SofaScore API: score, incidents, statistics (40+ stats), lineups
- [x] `--save-db`: persist vào `live_snapshots` + `live_incidents` (JSONB)
- [x] `--query`: view live data từ DB "như xem thật sự"
- [x] Tested: Wolves 0-0 Villa, 3 polls (30'→33'), DB verified

---

## 🔲 PHASE 3 — Automation + API

> Celery task queue, monitoring, FastAPI.

### 3.1 Celery Task Queue
- [ ] Setup Redis server
- [ ] `tasks/understat_tasks.py` — per-league, per-match tasks
- [ ] `tasks/fbref_tasks.py` — per-league, per-squad tasks
- [ ] Dead letter queue — retry 3 lần, Telegram alert nếu fail

### 3.2 Scheduler (APScheduler)
- [ ] `scheduler.py` thay `run_daemon.py`
- [ ] Persistence: state survive restart
- [ ] Web UI admin cho scheduler

### 3.3 Monitoring & Alerting
- [ ] Grafana + Prometheus dashboard
# Roadmap — Vertex Football Scraper
> Cập nhật: 06/03/2026 | Phiên bản: 1.5.0

Hệ thống đang tiến gần tới trạng thái hoàn thiện (Full-stack ready). Các module core đã ổn định, tập trung vào tối ưu hóa performance và mở rộng analytics.

---

## 🚩 Lộ trình phát triển

### ✅ Phase 1: Core Scrapers (Completed)
- [x] **Understat:** Async xG pipeline.
- [x] **FBref:** Standings & Squad stats.
- [x] **SofaScore API:** Events polling.
- [x] **Transfermarkt:** Market values.

### ✅ Phase 2: Database Infrastructure (Completed)
- [x] **Schema Design:** 20+ tables với FK & Indexes.
- [x] **Loader Logic:** CSV to PostgreSQL với UPSERT support.
- [x] **PostgreSQL 18.2:** Quy trình setup chuẩn hóa.

### ✅ Phase 3: Automation & Live Tracking (Completed)
- [x] **Scheduler Master:** Điều phối viên 24/7 (PID management, single browser).
- [x] **Live Polling:** Polling xG/Incidents thời gian thực.
- [x] **Post-Match Worker:** Tự động hóa Heatmaps & Standings update sau trận.
- [x] **Lineup Fetching:** 3-phase matching (60m, 15m, post-match).

### 🚧 Phase 4: Data Enrichment & AI (In-Progress)
- [x] **FBref Deep Stats:** Defensive & Possession stats (League-wide).
- [x] **News Radar:** Tự động quét RSS tin tức & chấn thương (Discord notification).
- [x] **AI Match Story:** Tự động tóm tắt diễn biến trận đấu bằng AI.
- [x] **Materialized Views:** Tối ưu hóa query ảnh cầu thủ/logo đội.
- [/] **Player Trend Analysis:** Phân tích phong độ cầu thủ hàng tuần (Nightly task).

---

## 🚀 Sắp tới (Next Steps)

### Phase 5: Advanced Analytics & Frontend Ready
- [ ] **Match Passing Stats:** Phân tích mạng lưới chuyền bóng (Passing network).
  - *Ghi chú: Đang tạm hoãn do độ phức tạp cao trong việc parse match report pages.*
- [ ] **Player Crossref refinement:** Tự động hóa việc map Transfermarkt ID cho toàn bộ database.
- [ ] **REST API:** Xây dựng Spring Boot/Python API để phục vụ Frontend.

### Phase 6: Scaling & Reliability
- [ ] **Dockerization:** Containerize toàn bộ scheduler & DB.
- [ ] **Alert System:** Dashboard theo dõi sức khỏe scraper (Uptime, error rate).
- [ ] **Proxy Rotation:** Hỗ trợ proxy để scale lên 20+ giải đấu đồng thời.

---

## 📈 Chỉ số hiện tại
| Chỉ số | Giá trị |
|--------|---------|
| Số giải đấu | 5 (Big Five) |
| Tổng số bảng | 22 |
| Tần suất poll | 60 - 90s |
| Độ trễ xG | < 120s |
| Thời gian backup | Hàng ngày 06:00 UTC |
| Coverage | 95% SofaScore Match Details |
| AI Story | ~30s sau khi trận đấu kết thúc |
