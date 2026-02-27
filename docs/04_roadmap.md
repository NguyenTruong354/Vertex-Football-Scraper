# Vertex Football Scraper — Roadmap
> Cập nhật: 28/02/2026

---

## Tổng quan

```
PHASE 1 ──────────── PHASE 2 ──────────── PHASE 3 ──────────── PHASE 4
EPL Only             Top 5 EU + UCL       15 Giải + Queue      50+ Giải Global
CSV Output           PostgreSQL           Celery Workers        Kubernetes
1 scraper            LeagueRegistry       Redis Cache           Data Lake
[✅ XONG]            [🟡 Đang làm]        [🔲 Tương lai]        [🔲 Dài hạn]
```

---

## ✅ PHASE 1 — Foundation (HOÀN THÀNH)
> **Mục tiêu:** Pipeline hoàn chỉnh cho EPL, đủ dùng để phát triển frontend.

### Deliverables
- [x] Understat pipeline — Shot xG, Player xG/xA/xGChain, Match aggregates
- [x] FBref pipeline — Standings, Squad stats, Player profiles, Player season stats
- [x] Cloudflare bypass với `nodriver` (headed Chrome)
- [x] Pydantic v2 validation cho tất cả data
- [x] CSV output (7 files)
- [x] Documentation (checklist, DB tables, usage guide)

### Dữ liệu có được sau Phase 1
| File | Rows (full EPL season) |
|---|---|
| `dataset_epl_xg.csv` | ~11,500 shots |
| `dataset_epl_player_stats.csv` | ~10,000 records |
| `dataset_epl_match_stats.csv` | 380 matches |
| `dataset_epl_standings.csv` | 20 rows |
| `dataset_epl_squad_stats.csv` | 20 rows |
| `dataset_epl_squad_rosters.csv` | ~580 players |
| `dataset_epl_player_season_stats.csv` | ~580 records |

### Giới hạn của Phase 1
- ~~Hard-code toàn bộ config cho EPL~~ (đã xử lý ở Phase 2.1/2.2)
- Không có database — chỉ CSV
- Không có deduplication
- Không có job scheduling
- Không có monitoring
- ~~Thêm giải mới = sửa code nhiều nơi~~ (đã giảm mạnh, chủ yếu qua `league_registry.py`)

---

## 🟡 PHASE 2 — Multi-League + Database (ĐANG THỰC HIỆN)
> **Mục tiêu:** Hỗ trợ Top 5 EU + UCL, chuyển sang PostgreSQL, kiến trúc sạch có thể mở rộng.
> **Ước tính thời gian còn lại:** 2–3 tuần

### Trạng thái nhanh
- ✅ 2.1 League Registry — hoàn thành
- ✅ 2.2 Refactor Scrapers nhận `league_id` — hoàn thành
- 🔲 2.3 PostgreSQL Database — chưa bắt đầu
- 🔲 2.4 Player ID Mapping — chưa bắt đầu
- 🔲 2.5 Data Pipeline Orchestrator — chưa bắt đầu

### 2.1 League Registry
**Mục tiêu:** Thêm giải mới chỉ cần thêm 1 entry — không sửa scraper.

- [x] Tạo `league_registry.py` — catalogue tất cả giải đấu
  ```
  LeagueConfig(
      league_id, display_name, country, confederation,
      understat_name,      # None nếu Understat không hỗ trợ
      fbref_comp_id,       # None nếu FBref không hỗ trợ
      fbref_comp_slug,
      seasons_available,
      has_xg_data,
      priority,
      active
  )
  ```
- [x] Thêm config cho:
  - EPL (`understat_name="EPL"`, `fbref_comp_id=9`)
  - La Liga (`understat_name="La_liga"`, `fbref_comp_id=12`)
  - Bundesliga (`understat_name="Bundesliga"`, `fbref_comp_id=20`)
  - Serie A (`understat_name="Serie_A"`, `fbref_comp_id=11`)
  - Ligue 1 (`understat_name="Ligue_1"`, `fbref_comp_id=13`)
  - UCL (`understat_name=None`, `fbref_comp_id=8`)
  - RFPL, Eredivisie, Primeira Liga (mở rộng thêm)

---

### 2.2 Refactor Scrapers — Nhận `league_id` làm tham số

**Understat:**
- [x] Refactor `config.py` → output dynamic theo `league_id`
- [x] Cập nhật `async_scraper.py` → nhận `--league`, `--season`, `--list-leagues`
- [x] Validate: báo lỗi nếu league không có Understat support

**FBref:**
- [x] Refactor `config_fbref.py` → `get_fbref_config(league_id: str)`
- [x] Build URL động: `f"/en/comps/{fbref_comp_id}/{fbref_comp_slug}-Stats"`
- [x] Cập nhật `fbref_scraper.py` → nhận `--league`, `--list-leagues`

**CLI mới sau refactor:**
```bash
# Chạy Understat cho La Liga 2024-2025
python understat/async_scraper.py --league LALIGA --season 2024

# Chạy FBref cho Bundesliga
python fbref/fbref_scraper.py --league BUNDESLIGA

# Chạy cả 2 cho từng giải (script custom)
python run_all.py
```

---

### 2.3 PostgreSQL Database

**Setup:**
- [ ] Cài PostgreSQL 16 (local) hoặc Supabase (cloud free tier)
- [ ] Tạo schema từ `database/schema.sql`:
  - `dim_leagues` — Catalogue giải đấu
  - `dim_seasons` — Mùa giải
  - `dim_teams` — Đội bóng
  - `dim_players` — Cầu thủ
  - `dim_matches` — Trận đấu
  - `fact_shots` — Từng cú sút
  - `fact_player_match` — Stats cầu thủ/trận
  - `fact_player_season` — Stats cầu thủ toàn mùa
  - `fact_standings` — BXH snapshot theo vòng đấu

**DB Writer module:**
- [ ] Tạo `database/db_writer.py`
  - `upsert_teams()` — INSERT OR UPDATE, không duplicate
  - `upsert_players()` — Dedup theo `player_id`
  - `upsert_shots()` — Dedup theo `(match_id, player_id, minute, x, y)`
  - `upsert_player_season()` — Dedup theo `(player_id, league_id, season_id)`
  - `upsert_standings()` — Snapshot theo `(league_id, season_id, matchweek)`
- [ ] Tạo `database/db_reader.py` — Query helpers cho API
- [ ] Connection pooling với `asyncpg`

**Migration:**
- [ ] Tạo `database/migrations/` folder
- [ ] Dùng `alembic` để quản lý schema changes

---

### 2.4 Player ID Mapping

Understat và FBref dùng 2 bộ `player_id` khác nhau. Cần bảng mapping để join data xG với data stats.

- [ ] Tạo `database/player_id_mapping.py`
  ```python
  # Match bằng tên cầu thủ (fuzzy matching)
  # Understat: player_id = int (e.g. 882)
  # FBref: player_id = str (e.g. "bc7dc64d")
  
  table player_id_map:
      understat_id  INTEGER
      fbref_id      VARCHAR(50)
      player_name   VARCHAR(100)
      confidence    FLOAT  -- 1.0 = exact match, 0.8+ = fuzzy
  ```
- [ ] Script `build_player_mapping.py` — tự động match bằng `rapidfuzz`
- [ ] Manual review report cho các trường hợp confidence < 0.9

---

### 2.5 Data Pipeline Orchestrator

- [ ] Tạo `run_pipeline.py` — script tổng hợp cả 2 pipelines
  ```bash
  # Full pipeline cho 1 giải
  python run_pipeline.py --league EPL --season 2025
  
  # Chỉ update matchweek mới nhất (nhanh)
  python run_pipeline.py --league EPL --season 2025 --mode incremental
  
  # Full historical backfill
  python run_pipeline.py --league EPL --seasons 2017 2018 2019 2020 2021 2022 2023 2024 2025
  ```
- [ ] Modes:
  - `full` — Scrape toàn bộ season
  - `incremental` — Chỉ scrape matches chưa có trong DB
  - `standings-only` — Chỉ update BXH (chạy hàng ngày)

---

### Phase 2 — Success Metrics
| Metric | Target |
|---|---|
| Số giải hỗ trợ | 6 (Top 5 EU + UCL) |
| Thêm giải mới | Chỉ cần thêm 1 `LeagueConfig` entry |
| Data deduplication | 100% — không có duplicate rows trong DB |
| Time scrape 1 giải | < 25 phút |
| DB size (6 giải × 5 mùa) | < 1GB |

**Tiến độ hiện tại (28/02/2026):**
- League registry + scraper refactor: **đạt**
- Dynamic output structure theo giải: **đạt**
- PostgreSQL + dedup + orchestrator: **chưa đạt (next focus)**

---

## 🔲 PHASE 3 — Job Queue + Monitoring
> **Mục tiêu:** Tự động hóa hoàn toàn, xử lý song song nhiều giải, có alerting khi lỗi.
> **Ước tính thời gian:** 6–8 tuần

### 3.1 Celery Task Queue

Mỗi giải / mỗi trận = 1 Celery task → chạy song song trên nhiều worker.

```
Scheduler (APScheduler)
    │
    ▼
Task Queue (Redis + Celery)
    │
    ├─── understat_worker × 3 (async)
    │        Xử lý 3 giải Understat cùng lúc
    │
    └─── fbref_worker × 2 (browser headed)
             Xử lý 2 giải FBref cùng lúc
             (mỗi worker 1 Chrome instance)
```

- [ ] Setup Redis server
- [ ] Cài `celery[redis]`
- [ ] Tạo `tasks/understat_tasks.py`
  - `task_scrape_understat_league(league_id, season)`
  - `task_scrape_understat_match(match_id, league_id)`
- [ ] Tạo `tasks/fbref_tasks.py`
  - `task_scrape_fbref_league(league_id, season)`
  - `task_scrape_fbref_squad(team_id, league_id)`
- [ ] Dead letter queue — retry failed tasks, alert sau 3 lần thất bại

---

### 3.2 Scheduler

- [ ] Tạo `scheduler.py` dùng `APScheduler`
  ```
  Hàng ngày 06:00:
    → Scrape standings tất cả giải active
    → Scrape matches hoàn thành trong 24h qua

  Hàng tuần (Thứ 2, 07:00):
    → Full sync player season stats tất cả giải

  Hàng tháng (Mùng 1):
    → Full audit — check missing data, re-scrape nếu cần
  ```

---

### 3.3 Monitoring & Alerting

- [ ] Setup **Grafana + Prometheus**
  - Dashboard: số tasks/giờ, success rate, scrape latency
  - Dashboard: DB size, row counts per table
  - Dashboard: Cloudflare block rate
- [ ] Alerting rules:
  - Task failure rate > 5% trong 1 giờ → Telegram alert
  - Cloudflare block liên tiếp 3 lần → Tạm dừng, alert
  - DB write errors → PagerDuty (nếu production)
- [ ] `health_check.py` — script kiểm tra nhanh toàn bộ pipeline
  ```bash
  python health_check.py
  # → Output: DB connections OK, Redis OK, 
  #           Last scrape: 2h ago, Missing: 0 matches
  ```

---

### 3.4 Caching Layer

- [ ] Redis cache cho queries thường dùng:
  - Standings (cache 1 giờ)
  - Top scorers (cache 6 giờ)
  - Player profiles — ít thay đổi (cache 24 giờ)
- [ ] Raw HTML/JSON cache trên MinIO/S3:
  - Lưu raw response trước khi parse
  - Cho phép re-parse mà không cần scrape lại
  - Giữ 30 ngày

---

### 3.5 Admin Dashboard (Backend)

- [ ] Tạo `api/` folder với **FastAPI**
  - `GET /leagues` — Danh sách giải đấu
  - `GET /leagues/{league_id}/standings` — BXH
  - `GET /leagues/{league_id}/top-scorers` — Top ghi bàn
  - `GET /matches/{match_id}/shots` — Shot data cho 1 trận
  - `GET /players/{player_id}` — Player profile + stats
  - `POST /admin/scrape` — Trigger scrape thủ công
  - `GET /admin/status` — Pipeline health check
- [ ] Authentication — API key cho admin endpoints
- [ ] Rate limiting — 100 requests/phút per IP

---

### Phase 3 — Success Metrics
| Metric | Target |
|---|---|
| Số giải hỗ trợ | 15 |
| Automation | 100% — không cần chạy tay |
| Uptime | > 99% cho scheduler |
| Alert response | < 5 phút khi có lỗi nghiêm trọng |
| API latency | p95 < 200ms |
| Backfill 5 giải × 10 mùa | < 4 giờ |

---

## 🔲 PHASE 4 — Global Scale
> **Mục tiêu:** 50+ giải toàn thế giới, kiến trúc cloud-native.
> **Ước tính thời gian:** 3–6 tháng

### 4.1 Mở rộng nguồn dữ liệu

| Nguồn | Giải hỗ trợ | Dữ liệu thêm được |
|---|---|---|
| **Understat** (hiện có) | 6 giải EU | xG shot-level |
| **FBref** (hiện có) | ~50 giải | Stats toàn diện |
| **Sofascore** (mới) | 600+ giải | Lineups, events, ratings, heat maps |
| **Transfermarkt** (mới) | Toàn cầu | Giá trị chuyển nhượng, lịch sử CLB |
| **StatsBomb Open** (mới) | Giải chọn lọc | Event data chi tiết (pass coordinates) |

- [ ] Xây dựng `sofascore/` pipeline (dùng private mobile API)
- [ ] Xây dựng `transfermarkt/` pipeline
- [ ] Integrate StatsBomb Open Data (GitHub, free JSON)

---

### 4.2 Distributed Scraping

FBref cần Chrome headed — không thể chạy nhiều instance trên 1 máy hiệu quả.

- [ ] Docker container cho mỗi browser worker:
  ```dockerfile
  FROM python:3.11-slim
  RUN apt-get install -y chromium chromium-driver
  # 1 container = 1 Chrome instance = 1 FBref worker
  ```
- [ ] Docker Compose cho local:
  ```yaml
  services:
    fbref_worker_1:  # Xử lý EPL, La Liga
    fbref_worker_2:  # Xử lý Bundesliga, Serie A
    fbref_worker_3:  # Xử lý UCL, Europa League
    understat_worker: # Async, 1 worker xử lý tất cả
    redis:
    postgres:
    grafana:
  ```
- [ ] Kubernetes deployment cho production:
  - `fbref-worker` Deployment — scale 3–10 replicas
  - `understat-worker` Deployment — scale 1–3 replicas
  - HorizontalPodAutoscaler — scale theo queue length

---

### 4.3 Data Lake Architecture

Để hỗ trợ analytics phức tạp và machine learning:

```
Raw Layer (MinIO / S3)
  └─ raw/{source}/{league}/{season}/{match_id}.json
        Lưu toàn bộ raw response, không transform

Staging Layer (PostgreSQL)
  └─ Cleaned, validated, deduplicated
        Dùng cho API và web app

Analytics Layer (DuckDB / ClickHouse)
  └─ Materialized aggregates, pre-computed metrics
        Dùng cho dashboards nặng, ML features
```

- [ ] Setup MinIO (self-hosted S3) cho raw storage
- [ ] ETL pipeline: Raw → PostgreSQL (hàng giờ)
- [ ] ClickHouse cho OLAP queries (BXH lịch sử, trend analysis)

---

### 4.4 Machine Learning Pipeline

Sau khi có đủ data từ nhiều giải nhiều mùa:

- [ ] **xG Model tự train** — Tự xây mô hình xG từ shot data
  - Features: x, y, situation, shot_type, last_action, angle, distance
  - Model: XGBoost / LightGBM
  - Validation: Calibration curve, Brier score
- [ ] **Match outcome prediction** — Dự đoán kết quả trận
  - Features: team xG trung bình, form, H2H, lineup
- [ ] **Player performance scoring** — Chấm điểm cầu thủ
  - Composite score từ nhiều metrics
  - Normalized by position

---

### Phase 4 — Success Metrics
| Metric | Target |
|---|---|
| Số giải hỗ trợ | 50+ |
| Số cầu thủ trong DB | 50,000+ |
| Historical depth | 10+ mùa cho giải lớn |
| API availability | 99.9% |
| Query latency | p99 < 500ms |
| ML model xG accuracy | Brier score < 0.07 |

---

## Tech Stack Summary

| Component | Phase 1 | Phase 2 | Phase 3 | Phase 4 |
|---|---|---|---|---|
| **Storage** | CSV files | PostgreSQL | PostgreSQL + Redis + MinIO | PostgreSQL + ClickHouse + S3 |
| **Orchestration** | Manual CLI | `run_pipeline.py` | Celery + APScheduler | Kubernetes + Airflow |
| **Caching** | None | None | Redis | Redis + CDN |
| **Monitoring** | Logs only | Logs only | Grafana + Prometheus | Datadog |
| **API** | None | None | FastAPI | FastAPI + GraphQL |
| **Scraping** | aiohttp + nodriver | aiohttp + nodriver | Celery workers | Docker + K8s pods |
| **League support** | 1 (EPL) | 6 (Top 5 + UCL) | 15 | 50+ |
| **Cost/tháng** | $0 | ~$10 (DB) | ~$65 | ~$200–500 |

---

## Dependency Map

```
Phase 1 (DONE)
    │
    ├─── 2.1 League Registry ────────────────────────────────┐
    │                                                        │
    ├─── 2.2 Refactor Scrapers ◀─── phụ thuộc 2.1           │
    │                                                        │
    ├─── 2.3 PostgreSQL Schema                               │
    │         │                                              │
    │         └─── 2.4 Player ID Mapping                    │
    │                   │                                    │
    │                   └─── 2.5 Pipeline Orchestrator ◀────┘
    │                               │
    │                    [Phase 2 DONE]
    │                               │
    ├─── 3.1 Celery Queue ◀─────────┘
    │         │
    ├─── 3.2 Scheduler ◀─── phụ thuộc 3.1
    │
    ├─── 3.3 Monitoring (độc lập, có thể làm song song)
    │
    ├─── 3.4 Caching ◀─── phụ thuộc 3.1
    │
    └─── 3.5 FastAPI ◀─── phụ thuộc 2.3 + 3.4
                  │
        [Phase 3 DONE]
                  │
    ┌─── 4.1 New data sources
    ├─── 4.2 Docker + K8s ◀─── phụ thuộc 3.1
    ├─── 4.3 Data Lake ◀─── phụ thuộc 3.4
    └─── 4.4 ML Pipeline ◀─── phụ thuộc 4.3
```

---

## Immediate Next Steps (Phase 2 — Còn lại)

| # | Task | File | Ưu tiên |
|---|---|---|---|
| 1 | Setup PostgreSQL + tạo schema chuẩn | `database/schema.sql` | 🔴 Cao |
| 2 | Viết `db_writer.py` (upsert + dedup) | `database/db_writer.py` | 🔴 Cao |
| 3 | Viết `db_reader.py` query helpers | `database/db_reader.py` | 🟡 Vừa |
| 4 | Cài alembic + initial migration | `database/migrations/` | 🟡 Vừa |
| 5 | Build player ID mapping (rapidfuzz) | `database/player_id_mapping.py` | 🟡 Vừa |
| 6 | Viết `run_pipeline.py` (full/incremental) | `run_pipeline.py` | 🟡 Vừa |
| 7 | Test end-to-end: LALIGA + BUNDESLIGA vào DB | — | 🟡 Vừa |
| 8 | Update docs DB + vận hành | `docs/` | 🟢 Thấp |
