# Autorun — Hướng dẫn chạy tự động pipeline
> Cập nhật: 01/03/2026

Tài liệu này mô tả cách chạy toàn bộ pipeline **Scrape → CSV → PostgreSQL**
theo lịch tự động, 24/7 daemon mode, và Live Match Tracker.

---

## Mục lục

1. [Tổng quan pipeline](#1-tổng-quan-pipeline)
2. [Yêu cầu hệ thống](#2-yêu-cầu-hệ-thống)
3. [Cấu hình](#3-cấu-hình)
4. [run_pipeline.py — Chạy 1 lần](#4-run_pipelinepy--chạy-1-lần)
5. [Lên lịch chạy](#5-lên-lịch-chạy)
6. [Daemon mode — 24/7](#6-daemon-mode--247)
7. [Live Match Tracker](#7-live-match-tracker)
8. [Command reference](#8-command-reference)
9. [Leagues hỗ trợ](#9-leagues-hỗ-trợ)
10. [Thời gian ước tính](#10-thời-gian-ước-tính)
11. [Logs & troubleshooting](#11-logs--troubleshooting)

---

## 1. Tổng quan pipeline

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│  Understat   │  │    FBref     │  │  SofaScore   │  │ Transfermarkt│
│  aiohttp     │  │  nodriver    │  │  nodriver    │  │  nodriver    │
│  async JSON  │  │  Cloudflare  │  │  SS API      │  │  TM HTML     │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │                 │
       └─────────────────┴─────────────────┴─────────────────┘
                                   │
                              CSV Output
                                   │
                     ┌─────────────▼─────────────┐
                     │  db/loader.py (upsert)     │
                     │  PostgreSQL 17 tables      │
                     └───────────────────────────┘
```

### 4 bước chính

| Bước | Script | Dữ liệu | DB tables |
|------|--------|---------|-----------|
| 1 | `understat/async_scraper.py` | xG, shots, player stats | `match_stats`, `shots`, `player_match_stats` |
| 2 | `fbref/fbref_scraper.py` | Standings, squad, players, fixtures, GK | 6 tables |
| 3 | `sofascore/sofascore_client.py` | Events, heatmaps, avg positions | 3 tables |
| 4 | `transfermarkt/tm_scraper.py` | Team info, market values | 2 tables |
| 5 | `db/loader.py` | CSV → PostgreSQL + crossref | `player_crossref` |

---

## 2. Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|-----------|-----------|---------|
| Python | 3.11+ | Đã test 3.11.9 |
| PostgreSQL | 15+ | Đã test 18.2 |
| Chrome | Mới nhất | Cho FBref, SofaScore, Transfermarkt, Live Tracker |
| RAM | ≥ 4 GB | Chrome dùng ~500 MB |
| Disk | ~200 MB | CSV output + venv |

```bash
# Cài dependencies
pip install -r requirements.txt
pip install nodriver
```

---

## 3. Cấu hình

### 3.1 File `.env`

```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=vertex_football
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
```

### 3.2 Khởi tạo database (chạy 1 lần)

```bash
python db/setup_db.py --schema-only
```

Tạo database `vertex_football` nếu chưa có + chạy `db/schema.sql` (17 bảng + indexes + FK).

---

## 4. `run_pipeline.py` — Chạy 1 lần

```bash
[Unit]
Description=Vertex Football Master Scheduler
After=network.target postgresql.service

[Service]
User=youruser
WorkingDirectory=/path/to/Vertex_Football_Scraper2
Environment=PYTHONPATH=.
ExecStart=/path/to/venv/bin/python scheduler_master.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Kích hoạt:
```bash
sudo systemctl enable vertex-master
sudo systemctl start vertex-master
sudo journalctl -u vertex-master -f  # Xem log thời gian thực
```

### 3. Triển khai trên Windows (Task Scheduler / Batch)
Tạo file `daemon_master.bat`:
```batch
@echo off
│                                                         │
│  TIER 3 — Transfermarkt                                │
│    • Mỗi 24 giờ                                        │
│                                                         │
│  DB Load — sau mỗi lần scrape thành công               │
└─────────────────────────────────────────────────────────┘
```

| Tầng | Nguồn | Interval (match hours) | Interval (off hours) |
|------|-------|----------------------|---------------------|
| T1 | Understat + SofaScore | 30 phút | 2 giờ |
| T2 | FBref | 4 giờ | 4 giờ |
| T3 | Transfermarkt | 24 giờ | 24 giờ |

### Chạy daemon

```bash
# Windows
daemon.bat

# Trực tiếp
python run_daemon.py

# Nhiều league, match hours tùy chỉnh
python run_daemon.py --league EPL LALIGA --match-hours 11-23

# Interval tùy chỉnh
python run_daemon.py --tier1-interval 900 --tier2-interval 7200

# Test (không chạy thật)
python run_daemon.py --dry-run

# Linux background
nohup python run_daemon.py > /dev/null 2>&1 &
```

### Tham số daemon

| Flag | Default | Mô tả |
|------|---------|-------|
| `--league` | `EPL` | 1 hoặc nhiều league IDs |
| `--match-hours` | `11-23` | Giờ thi đấu UTC format `START-END` |
| `--tier1-interval` | `1800` | T1 giây (match hours) |
| `--tier1-off-interval` | `7200` | T1 giây (off hours) |
| `--tier2-interval` | `14400` | T2 giây |
| `--tier3-interval` | `86400` | T3 giây |
| `--ss-match-limit` | `5` | SofaScore: trận mỗi cycle |
| `--dry-run` | False | Chỉ log, không chạy |

### State persistence

Daemon lưu state vào `logs/daemon_state.json` — survive khi restart:
```json
{
  "tier1_last_run": "2026-03-01T06:30:00",
  "tier2_last_run": "2026-03-01T04:00:00",
  "tier3_last_run": "2026-03-01T00:00:00",
  "cycle_count": 47
}
```

### Log rotation

`logs/daemon.log` — tự động rotate: 10 MB × 5 files = tối đa 50 MB logs.

---

## 7. Live Match Tracker

Theo dõi trận đấu đang diễn ra với terminal dashboard realtime.

### Cách hoạt động

1. Poll SofaScore API mỗi 60–90 giây
2. Fetch 3–4 endpoints/poll: score, incidents, statistics, lineups (mỗi 3 polls)
3. Render terminal dashboard với ANSI colors
4. Upsert vào PostgreSQL (nếu `--save-db`)

### Sử dụng

```bash
# Bước 1: Tìm event_id
python live_match.py --today          # Xem tất cả trận hôm nay
python live_match.py --team Arsenal   # Tìm trận của Arsenal

# Bước 2: Theo dõi trận
python live_match.py 14023979                        # Poll mặc định 90s
python live_match.py 14023979 --interval 60          # Poll mỗi 60s
python live_match.py 14023979 --save-db              # Lưu vào PostgreSQL
python live_match.py 14023979 --save-db --save-csv   # Lưu cả DB + CSV

# Bước 3: Query dữ liệu đã lưu
python live_match.py --query                         # Tất cả matches trong DB
python live_match.py --query 14023979               # Chi tiết 1 match

# Dừng: Ctrl+C (graceful shutdown)
```

### Dashboard

```
╔══════════════════════════════════════════════════════╗
║  ⚽  LIVE MATCH TRACKER  |  Poll #3  |  33'        ║
╠══════════════════════════════════════════════════════╣
║    Wolverhampton  0 – 0  Aston Villa                ║
║    Premier League | Round 28                        ║
║    DB SAVED: 1 rows upserted                        ║
╚══════════════════════════════════════════════════════╝

📊 MATCH STATISTICS
  Ball possession  │ Wolves 40% ████████░░░░░░ Villa 60%
  Expected goals   │ Wolves 0.26 █████░░░░░░░░ Villa 0.39
  Total shots      │ Wolves 2 ███░░░░░░░░░░░░░ Villa 4
  Shots on target  │ Wolves 0 ░░░░░░░░░░░░░░░░ Villa 2
  Passes           │ Wolves 139 █████░░░░░░░░░ Villa 204
  Corner kicks     │ Wolves 0 ░░░░░░░░░░░░░░░░ Villa 2
  ...40 stats total

🎯 INCIDENTS
  (No goals or cards in this period)

👥 LINEUPS — Wolverhampton
  GK  #1  Sa, Jose (8.2) ★
  ...
```

### JSONB queries sau khi save-db

```sql
-- Xem live match đang diễn ra
SELECT home_team, home_score, away_score, away_team, minute,
       statistics_json->'Ball possession'->>'home' AS poss_home,
       statistics_json->'Expected goals'->>'away' AS xg_away
FROM live_snapshots WHERE status = 'inprogress';

-- So sánh stats theo thời gian (nhiều polls)
SELECT minute, poll_count,
       statistics_json->'Total shots'->>'home' AS shots_home,
       statistics_json->'Total shots'->>'away' AS shots_away
FROM live_snapshots WHERE event_id = 14023979;
```

### DB tables (live)

| Bảng | Nội dung | PK |
|------|----------|-----|
| `live_snapshots` | 1 row/match, upsert mỗi poll, JSONB 40 stats | `event_id` |
| `live_incidents` | 1 row/incident, upsert theo unique index | `id` |

---

## 8. Command reference

```bash
# ── Khởi tạo ──────────────────────────────────────────────
python db/setup_db.py --schema-only        # Tạo 17 bảng

# ── Scraping thủ công ─────────────────────────────────────
python understat/async_scraper.py --limit 5
python fbref/fbref_scraper.py --standings-only
python sofascore/sofascore_client.py --limit 10
python transfermarkt/tm_scraper.py

# ── Load DB ───────────────────────────────────────────────
python db/loader.py                        # Load EPL
python db/loader.py --league LALIGA        # Load La Liga

# ── Pipeline orchestrator ─────────────────────────────────
python run_pipeline.py                     # Full EPL
python run_pipeline.py --quick-test        # Test nhanh
python run_pipeline.py --load-only         # Chỉ load DB

# ── Automation ────────────────────────────────────────────
autorun.bat                                # Windows: 1 lần
python run_daemon.py                       # 24/7 daemon
daemon.bat                                 # Windows daemon

# ── Live tracking ─────────────────────────────────────────
python live_match.py --today
python live_match.py 14023979 --save-db
python live_match.py --query
python live_match.py --query 14023979
```

---

## 9. Leagues hỗ trợ

| League ID | Tên | Understat | FBref | SS | TM |
|-----------|-----|-----------|-------|----|----|
| `EPL` | Premier League | ✅ | ✅ | ✅ | ✅ |
| `LALIGA` | La Liga | ✅ | ✅ | ✅ | ✅ |
| `BUNDESLIGA` | Bundesliga | ✅ | ✅ | ✅ | ✅ |
| `SERIEA` | Serie A | ✅ | ✅ | ✅ | ✅ |
| `LIGUE1` | Ligue 1 | ✅ | ✅ | ✅ | ✅ |
| `RFPL` | Russian PL | ✅ | ❌ | ❌ | ❌ |
| `UCL` | Champions League | ❌ | ✅ | ❌ | ❌ |
| `EREDIVISIE` | Eredivisie | ❌ | ✅ | ❌ | ❌ |
| `LIGA_PORTUGAL` | Primeira Liga | ❌ | ✅ | ❌ | ❌ |

---

## 10. Thời gian ước tính

| Task | Thời gian |
|------|-----------|
| Understat `--limit 5` | ~7–10 giây |
| Understat EPL full season | ~8–15 phút |
| FBref `--standings-only` | ~15 giây |
| FBref EPL full (20 squads) | ~2–3 phút |
| SofaScore `--limit 10` | ~2–5 phút |
| Transfermarkt EPL | ~3–5 phút |
| `run_pipeline.py --quick-test` | ~2 phút |
| `run_pipeline.py` EPL full | ~25–35 phút |
| Live poll (1 cycle) | ~5–10 giây |

---

## 11. Logs & troubleshooting

### Log files

| File | Nội dung |
|------|---------|
| `logs/daemon.log` | Daemon: 10 MB × 5 rotation |
| `logs/daemon_state.json` | Daemon state (last run times) |
| `logs/autorun_YYYYMMDD.log` | Task Scheduler run log |

### Vấn đề thường gặp

| Triệu chứng | Nguyên nhân | Giải pháp |
|------------|------------|---------|
| Chrome bật lên | nodriver cần headed mode | Bình thường, không đóng |
| `RuntimeError: Event loop is closed` | Windows asyncio cleanup | Harmless warning, bỏ qua |
| FBref trả về 403 | Cloudflare block | Thử lại sau 5 phút |
| Understat rate limit | Request quá nhanh | Tăng `POLITENESS_DELAY_SECONDS` lên 2.0 |
| Live tracker không tìm trận | Event chưa bắt đầu | Dùng `--today` để lấy event_id chuẩn |
| DB timeout | PostgreSQL không chạy | Kiểm tra `pg_ctl status` |
| `psycopg2.OperationalError` | Sai credentials | Kiểm tra `.env` |
