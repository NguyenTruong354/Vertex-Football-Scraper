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
  - Tasks/giờ, success rate, scrape latency
  - DB size alert, Cloudflare block rate
- [ ] Telegram bot alert khi task failure > 5%
- [ ] `health_check.py` — kiểm tra toàn bộ pipeline < 30s

### 3.4 FastAPI Backend
- [ ] `api/` folder với FastAPI
  - `GET /leagues` — danh sách giải
  - `GET /standings/{league_id}` — BXH hiện tại
  - `GET /players/{player_id}/stats` — thống kê cầu thủ
  - `GET /matches/{match_id}` — thông tin trận
  - `GET /live` — danh sách trận đang diễn ra
  - `GET /live/{event_id}` — live data 1 trận
  - `GET /admin/status` — pipeline health
- [ ] API key authentication cho admin endpoints
- [ ] Rate limiting — 100 req/phút per IP
- [ ] Swagger docs tự động

### 3.5 Caching Layer
- [ ] Redis cache: standings (1h), fixtures (6h), player profiles (24h)
- [ ] Cache invalidation khi có dữ liệu mới

---

## 🔲 PHASE 4 — Global Scale

> 50+ giải, cloud-native, ML pipeline.

### 4.1 Mở rộng nguồn dữ liệu
- [ ] **StatsBomb Open Data** — event data chi tiết (pass coordinates)
- [ ] Thêm 20+ giải: Championship, MLS, J-League, A-League…

### 4.2 Distributed Scraping
- [ ] Docker container cho mỗi browser worker
- [ ] Docker Compose cho local development
- [ ] Kubernetes cho production (HPA theo queue length)

### 4.3 Data Lake Architecture
```
Raw Layer (MinIO/S3)       → raw/{source}/{league}/{season}/{id}.json
Staging Layer (PostgreSQL) → Cleaned, validated, deduplicated
Analytics Layer (DuckDB)   → Materialized aggregates, pre-computed metrics
```

### 4.4 Machine Learning Pipeline
- [ ] **xG Model tự train** từ shot data (features: x, y, angle, distance, situation)
- [ ] **Match outcome prediction** (team xG form, H2H, lineup)
- [ ] **Player performance scoring** — composite score by position

---

## Metrics

| Phase | Status | Giải | Sources | DB | Live |
|-------|--------|------|---------|----|----|
| Phase 1 | ✅ Done | EPL only | 2 (US+FB) | ❌ CSV | ❌ |
| Phase 2 | ✅ Done | 9 leagues | 4 sources | ✅ 17 tables | ✅ |
| Phase 3 | 🔲 Next | 9 leagues | 4 sources | ✅ | ✅ API |
| Phase 4 | 🔲 Future | 50+ | 6+ | ✅ Data Lake | ✅ |
