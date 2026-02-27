# Autorun — Hướng dẫn chạy tự động pipeline

> Tài liệu này mô tả cách chạy toàn bộ pipeline **Scrape → CSV → PostgreSQL**
> theo lịch tự động (hàng ngày / hàng tuần).

---

## Mục lục

1. [Tổng quan pipeline](#1-tổng-quan-pipeline)
2. [Yêu cầu hệ thống](#2-yêu-cầu-hệ-thống)
3. [Cấu hình](#3-cấu-hình)
4. [Script tự động — `run_pipeline.py`](#4-script-tự-động--run_pipelinepy)
5. [Lên lịch chạy](#5-lên-lịch-chạy)
   - [Windows Task Scheduler](#51-windows-task-scheduler)
   - [Linux cron](#52-linux-cron)
6. [Command reference — chạy thủ công](#6-command-reference--chạy-thủ-công)
7. [Bảng league được hỗ trợ](#7-bảng-league-được-hỗ-trợ)
8. [Thời gian chạy ước tính](#8-thời-gian-chạy-ước-tính)
9. [Xử lý lỗi & logs](#9-xử-lý-lỗi--logs)
10. [FAQ](#10-faq)

---

## 1. Tổng quan pipeline

```
┌──────────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│  Understat   │   │    FBref     │   │  SofaScore   │   │ Transfermarkt│
│  (aiohttp)   │   │  (nodriver)  │   │  (nodriver)  │   │  (nodriver)  │
└──────┬───────┘   └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
       │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼
   output/             output/           output/            output/
   understat/          fbref/            sofascore/         transfermarkt/
   {league}/           {league}/         {league}/          {league}/
   *.csv               *.csv             *.csv              *.csv
       │                  │                  │                  │
       └──────────────────┴──────────────────┴──────────────────┘
                                    │
                                    ▼
                          ┌─────────────────┐
                          │  db/loader.py   │
                          │  (bulk upsert)  │
                          └────────┬────────┘
                                   │
                                   ▼
                          ┌─────────────────┐
                          │   PostgreSQL    │
                          │  15 tables +    │
                          │  player_crossref│
                          └─────────────────┘
```

**4 bước chính:**

| Bước | Mô tả | Script |
|------|--------|--------|
| 1 | Scrape Understat (xG, shots, player stats) | `understat/async_scraper.py` |
| 2 | Scrape FBref (standings, squads, fixtures, GK) | `fbref/fbref_scraper.py` |
| 3 | Scrape SofaScore (events, heatmaps, avg positions) | `sofascore/sofascore_client.py` |
| 4 | Scrape Transfermarkt (team metadata, market values) | `transfermarkt/tm_scraper.py` |
| 5 | Load CSV → PostgreSQL + build crossref | `db/setup_db.py` hoặc `db/loader.py` |

---

## 2. Yêu cầu hệ thống

| Thành phần | Phiên bản | Ghi chú |
|-----------|-----------|---------|
| Python | 3.11+ | Đã test trên 3.11.9 |
| PostgreSQL | 15+ | Đã test trên 18.2 |
| Google Chrome | Mới nhất | Cần cho FBref, SofaScore, Transfermarkt (nodriver) |
| RAM | ≥ 4 GB | Chrome headless dùng ~500 MB |
| Disk | ~200 MB | Cho CSV output + venv |

### Cài đặt dependencies

```bash
# Tạo venv
python -m venv .venv

# Activate (Windows PowerShell)
.venv\Scripts\Activate.ps1

# Activate (Windows Git Bash / Linux)
source .venv/Scripts/activate   # Windows
source .venv/bin/activate       # Linux

# Cài packages
pip install -r requirements.txt
pip install nodriver
```

---

## 3. Cấu hình

### 3.1 File `.env`

Copy `.env.example` → `.env` rồi sửa:

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

Lệnh này sẽ:
- Tạo database `vertex_football` nếu chưa có
- Chạy `db/schema.sql` — tạo 15 bảng + indexes + FK constraints

---

## 4. Script tự động — `run_pipeline.py`

File `run_pipeline.py` ở thư mục gốc chạy toàn bộ pipeline tự động:

```bash
# Chạy full pipeline cho EPL (mặc định)
python run_pipeline.py

# Chạy cho league cụ thể
python run_pipeline.py --league LALIGA

# Chạy nhiều league
python run_pipeline.py --league EPL LALIGA BUNDESLIGA

# Chỉ scrape (không load DB)
python run_pipeline.py --scrape-only

# Chỉ load DB (dùng CSV đã có)
python run_pipeline.py --load-only

# Giới hạn số trận SofaScore (tiết kiệm thời gian)
python run_pipeline.py --ss-match-limit 10

# Chạy nhanh để test (limit tất cả)
python run_pipeline.py --quick-test
```

### Tham số đầy đủ

| Flag | Default | Mô tả |
|------|---------|--------|
| `--league` | `EPL` | 1 hoặc nhiều league IDs |
| `--scrape-only` | `false` | Chỉ scrape, không load vào DB |
| `--load-only` | `false` | Chỉ load CSV có sẵn vào DB |
| `--skip-understat` | `false` | Bỏ qua Understat |
| `--skip-fbref` | `false` | Bỏ qua FBref |
| `--skip-sofascore` | `false` | Bỏ qua SofaScore |
| `--skip-transfermarkt` | `false` | Bỏ qua Transfermarkt |
| `--ss-match-limit` | `0` | Giới hạn trận SofaScore (0 = tất cả) |
| `--quick-test` | `false` | Test nhanh: limit 2 teams, 1 match |

---

## 5. Lên lịch chạy

### 5.1 Windows Task Scheduler

**Bước 1:** Tạo file `autorun.bat`:

```bat
@echo off
:: ============================================================
:: Vertex Football Scraper — Daily Auto Run
:: ============================================================

set PROJECT_DIR=D:\Vertex_Football_Scraper2
set PYTHON=%PROJECT_DIR%\.venv\Scripts\python.exe
set LOG_DIR=%PROJECT_DIR%\logs

:: Tạo thư mục log nếu chưa có
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: Timestamp cho log file
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set DATESTAMP=%%c-%%a-%%b
for /f "tokens=1-2 delims=: " %%a in ('time /t') do set TIMESTAMP=%%a%%b
set LOGFILE=%LOG_DIR%\autorun_%DATESTAMP%_%TIMESTAMP%.log

echo [%date% %time%] Pipeline started >> "%LOGFILE%"

cd /d "%PROJECT_DIR%"

:: Chạy pipeline
"%PYTHON%" run_pipeline.py --league EPL >> "%LOGFILE%" 2>&1

echo [%date% %time%] Pipeline finished (exit code: %ERRORLEVEL%) >> "%LOGFILE%"
```

**Bước 2:** Mở Task Scheduler:

1. Win + R → `taskschd.msc` → Enter
2. **Create Task** (không phải Create Basic Task)
3. Tab **General**:
   - Name: `Vertex Football Scraper`
   - Run whether user is logged on or not: ✅
   - Run with highest privileges: ✅
4. Tab **Triggers**:
   - New → Daily → Start at `06:00` (hoặc giờ muốn chạy)
5. Tab **Actions**:
   - New → Start a program
   - Program: `D:\Vertex_Football_Scraper2\autorun.bat`
   - Start in: `D:\Vertex_Football_Scraper2`
6. Tab **Settings**:
   - Stop the task if it runs longer than: `4 hours`
   - If the task fails, restart every: `30 minutes`, up to `3 times`

### 5.2 Linux cron

```bash
# Mở crontab
crontab -e

# Chạy hàng ngày lúc 6:00 sáng
0 6 * * * cd /path/to/Vertex_Football_Scraper2 && .venv/bin/python run_pipeline.py --league EPL >> logs/autorun_$(date +\%Y\%m\%d).log 2>&1

# Chạy hàng tuần (Chủ nhật 3:00 sáng) cho nhiều league
0 3 * * 0 cd /path/to/Vertex_Football_Scraper2 && .venv/bin/python run_pipeline.py --league EPL LALIGA BUNDESLIGA SERIEA LIGUE1 >> logs/autorun_weekly_$(date +\%Y\%m\%d).log 2>&1
```

---

## 6. Command reference — chạy thủ công

Nếu muốn chạy từng bước riêng:

### 6.1 Understat

```bash
cd understat
python async_scraper.py --league EPL
# Tùy chọn:
#   --season 2025            Mùa giải
#   --limit 5                Giới hạn 5 trận
#   --no-csv                 Không xuất CSV
cd ..
```

**Output:** `output/understat/epl/`
- `dataset_epl_xg.csv` — dữ liệu shots/xG
- `dataset_epl_player_stats.csv` — thống kê cầu thủ từng trận
- `dataset_epl_match_stats.csv` — thống kê tổng hợp từng trận

### 6.2 FBref

```bash
cd fbref
python fbref_scraper.py --league EPL
# Tùy chọn:
#   --standings-only          Chỉ BXH + lịch đấu
#   --limit 5                 Giới hạn 5 đội
#   --match-limit 3           Giới hạn 3 match reports
#   --no-match-passing        Bỏ qua passing data
cd ..
```

**Output:** `output/fbref/epl/`
- `dataset_epl_standings.csv` — bảng xếp hạng
- `dataset_epl_squad_rosters.csv` — danh sách cầu thủ
- `dataset_epl_squad_stats.csv` — thống kê đội
- `dataset_epl_player_season_stats.csv` — thống kê cầu thủ mùa giải
- `dataset_epl_gk_stats.csv` — thống kê thủ môn
- `dataset_epl_fixtures.csv` — lịch thi đấu + kết quả

### 6.3 SofaScore

```bash
cd sofascore
python sofascore_client.py --league EPL --match-limit 10
# Tùy chọn:
#   --match-limit 0          Tất cả trận (rất lâu!)
#   --skip-heatmaps          Chỉ lấy avg positions
cd ..
```

**Output:** `output/sofascore/epl/`
- `dataset_epl_ss_events.csv` — danh sách trận đấu
- `dataset_epl_player_avg_positions.csv` — vị trí trung bình cầu thủ
- `dataset_epl_heatmaps.csv` — heatmap (có JSON points)

### 6.4 Transfermarkt

```bash
cd transfermarkt
python tm_scraper.py --league EPL
# Tùy chọn:
#   --limit 5                Giới hạn 5 đội
#   --metadata-only          Chỉ metadata đội
cd ..
```

**Output:** `output/transfermarkt/epl/`
- `dataset_epl_team_metadata.csv` — thông tin đội (sân, HLV, đội hình)
- `dataset_epl_market_values.csv` — giá trị chuyển nhượng cầu thủ

### 6.5 Load vào PostgreSQL

```bash
# Full setup (tạo DB + schema + load + demo queries)
python db/setup_db.py --league EPL

# Chỉ load CSV (DB đã có sẵn)
python db/loader.py --league EPL

# Load 1 bảng cụ thể
python db/loader.py --league EPL --table shots standings

# Chạy demo queries
python db/queries.py --league EPL
python db/queries.py --league EPL --query top_xg standings
```

---

## 7. Bảng league được hỗ trợ

| League ID | Tên giải | Understat | FBref | SofaScore | Transfermarkt |
|-----------|----------|:---------:|:-----:|:---------:|:------------:|
| `EPL` | Premier League | ✅ | ✅ | ✅ | ✅ |
| `LALIGA` | La Liga | ✅ | ✅ | ✅ | ✅ |
| `BUNDESLIGA` | Bundesliga | ✅ | ✅ | ✅ | ✅ |
| `SERIEA` | Serie A | ✅ | ✅ | ✅ | ✅ |
| `LIGUE1` | Ligue 1 | ✅ | ✅ | ✅ | ✅ |
| `RFPL` | Russian Premier Liga | ✅ | ❌ | ✅ | ✅ |
| `UCL` | Champions League | ❌ | ✅ | ✅ | ✅ |
| `EREDIVISIE` | Eredivisie | ❌ | ✅ | ✅ | ✅ |
| `LIGA_PORTUGAL` | Primeira Liga | ❌ | ✅ | ✅ | ✅ |

> **Lưu ý:** Khi chạy league không hỗ trợ ở 1 nguồn, scraper đó sẽ tự bỏ qua.

---

## 8. Thời gian chạy ước tính

Thời gian phụ thuộc vào tốc độ mạng và rate-limit của từng nguồn.

### Một league (EPL)

| Bước | Thời gian ước tính | Ghi chú |
|------|-------------------|---------|
| Understat | 1–3 phút | 6 concurrent requests |
| FBref | 15–30 phút | 5s delay / page, ~20 đội |
| SofaScore | 2–5 giờ | 2s delay / request, ~380 trận × 22 cầu thủ |
| SofaScore (limit 10) | 5–10 phút | Chỉ 10 trận gần nhất |
| Transfermarkt | 10–20 phút | 6s delay / page, ~20 đội |
| DB Load | < 1 phút | Bulk upsert |
| **Tổng (full)** | **~3–6 giờ** | Tùy SofaScore match-limit |
| **Tổng (limit SS=10)** | **~30–60 phút** | Recommended cho daily run |

### Nhiều league (5 league lớn)

| Chế độ | Thời gian |
|--------|-----------|
| Full (SS limit=0) | 15–30 giờ |
| SS limit=10 | 2.5–5 giờ |
| SS limit=5 | 2–4 giờ |

### Khuyến nghị lịch chạy

| Tần suất | Config | Mục đích |
|----------|--------|----------|
| Hàng ngày | `--ss-match-limit 5` | Cập nhật trận mới nhất |
| Hàng tuần | `--ss-match-limit 0` | Full refresh heatmaps |
| Hàng tháng | Nhiều league, full | Sync toàn bộ dữ liệu |

---

## 9. Xử lý lỗi & logs

### 9.1 Cấu trúc log

Pipeline log ra stdout/stderr với format:

```
HH:MM:SS | LEVEL    | message
```

Khi chạy qua `autorun.bat` hoặc cron, log được redirect vào file:

```
logs/
├── autorun_2026-02-28_0600.log
├── autorun_2026-03-01_0600.log
└── ...
```

### 9.2 Lỗi thường gặp

| Lỗi | Nguyên nhân | Cách sửa |
|-----|------------|----------|
| `ConnectionRefusedError` (DB) | PostgreSQL chưa chạy | Khởi động PostgreSQL service |
| `Chrome not found` (nodriver) | Chrome chưa cài | Cài Google Chrome |
| `403 Forbidden` (SofaScore) | Cloudflare block | Đã xử lý bằng nodriver — nếu vẫn lỗi, thử cập nhật Chrome |
| `429 Too Many Requests` | Rate limit | Tăng delay hoặc giảm `--limit` |
| `NotNullViolation` (fixtures) | Trận chưa diễn ra | Đã xử lý — loader tự filter NULL match_id |
| `I/O operation on closed pipe` | Windows asyncio cleanup | Vô hại — bỏ qua |

### 9.3 Retry tự động

- **Understat:** `tenacity` retry 3 lần, exponential backoff
- **FBref:** Retry 2 lần nếu lỗi navigate
- **SofaScore:** Retry trên từng request
- **Transfermarkt:** Retry 2 lần nếu lỗi navigate
- **DB Loader:** Upsert (ON CONFLICT) → an toàn khi chạy lại

---

## 10. FAQ

### Q: Chạy lại có bị duplicate data không?

**Không.** Tất cả bảng dùng `ON CONFLICT DO UPDATE` (upsert). Chạy lại sẽ cập nhật
data mới, không tạo bản ghi trùng.

### Q: SofaScore chạy quá lâu, làm sao?

Dùng `--ss-match-limit 10` để chỉ lấy 10 trận gần nhất. Hoặc `--skip-sofascore`
nếu không cần heatmap data.

### Q: Cần Chrome chạy ở chế độ nào?

- **SofaScore:** headless (tự động)
- **FBref & Transfermarkt:** headed (mở cửa sổ Chrome) — cần display/desktop
- Trên server Linux headless, cần `Xvfb` hoặc sửa code để chạy headless

### Q: Muốn thêm league mới?

Thêm config vào `league_registry.py` với các ID tương ứng từ Understat, FBref,
SofaScore, Transfermarkt. Xem `docs/02_fbref.md` để biết cách tìm league URL.

### Q: Database schema thay đổi thì sao?

```bash
# Drop tất cả bảng và tạo lại
python -c "
from db.config_db import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute('DROP SCHEMA public CASCADE; CREATE SCHEMA public;')
conn.commit()
"
python db/setup_db.py --league EPL
```

### Q: Muốn chạy trên Docker?

Hiện chưa có Docker support. Có thể tự build Dockerfile dựa trên:

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y chromium chromium-driver
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt && pip install nodriver
CMD ["python", "run_pipeline.py", "--league", "EPL"]
```

> **Lưu ý:** nodriver cần Chrome thật (không phải Chromium trong mọi trường hợp).
> Cần test kỹ trên Docker environment.
