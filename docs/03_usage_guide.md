# Usage Guide — Vertex Football Scraper
> Hướng dẫn cài đặt, cấu hình và vận hành hệ thống cào dữ liệu bóng đá 24/7.

---

## 📂 Cấu trúc dự án (Mới)

```text
/
├── scheduler_master.py   # [CHÍNH] Điều phối viên 24/7 (Multi-league)
├── run_pipeline.py       # Chạy pipeline thủ công cho từng giải
├── live_match.py         # Module polling live data (SofaScore)
├── async_scraper.py      # Module cào Understat (Async)
├── news_radar.py         # Tự động quét tin tức & chấn thương (RSS)
├── db/
│   ├── schema.sql        # Định nghĩa 20+ bảng & Materialized Views
│   ├── loader.py         # Logic chuẩn hóa & đẩy CSV vào PostgreSQL
│   └── utils.py          # Tiện ích DB (fuzzy match, safe_float)
├── fbref/
│   ├── fbref_scraper.py  # Scraper stats chính (Defensive, Possession, GK...)
│   └── config_fbref.py   # Mapping URL giải đấu FBref
├── sofascore/
│   ├── sofascore_client.py # Client lấy heatmaps, avg positions, lineups
│   └── config_sofascore.py # Mapping Tournament IDs
├── transfermarkt/
│   ├── tm_scraper.py     # Lấy market values & player/team photos
│   └── config_tm.py      # Mapping URL Transfermarkt
├── output/               # Chứa file CSV theo league/source
└── logs/                 # Log vận hành chi tiết
```

---

## ⚡ Bắt đầu nhanh (Quick Start)

### 1. Cài đặt môi trường
(Giữ nguyên)

### 2. Cấu hình Database & Webhook
Tạo file `.env` tại thư mục gốc:
```bash
POSTGRES_HOST=your_host
POSTGRES_PORT=5432
POSTGRES_DB=vertex_football
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Discord Webhooks (Optional)
DISCORD_WEBHOOK=...        # Thông báo chung / Bảo trì
DISCORD_WEBHOOK_LIVE=...   # Thông báo bàn thắng / VAR live
DISCORD_WEBHOOK_NEWS=...   # Thông báo tin tức / Chấn thương
```

---

## 🚀 Cách vận hành

### A. Chạy Master Scheduler (Khuyên dùng)
Đây là chế độ vận hành "Set & Forget", tự động quản lý toàn bộ logic cào dữ liệu, live tracking và bảo trì cho tất cả các giải đấu.

```bash
# Chạy cho cả 5 giải (EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1)
python scheduler_master.py

# Chỉ chạy cho 1 hoặc vài giải cụ thể
python scheduler_master.py --leagues EPL LALIGA
```
**Cơ chế của Master:**
1. **Daily Maintenance (06:00 UTC):** Cào FBref, Transfermarkt, refresh Materialized Views, dọn dẹp data cũ.
2. **Upcoming Check:** Quét lịch thi đấu SofaScore để lên lịch Live Tracking.
3. **Lineup Fetch:** Lấy đội hình 60p và 15p trước giờ bóng lăn.
4. **Live Polling:** Poll xG, sút, bàn thắng mỗi 60s khi có trận đấu.
5. **Post-Match:** Cào Understat, Heatmaps ngay sau khi kết thúc.

### B. Chạy Pipeline thủ công
(Giữ nguyên)

---

## 🛠 Features nâng cao

### 1. Cross-Source Fuzzy Matching
Hệ thống tự động ánh xạ cầu thủ giữa Understat (ID số) và FBref (ID slug) thông qua tên. Nếu cần map thủ công hoặc cập nhật hàng loạt Transfermarkt ID:
```bash
python -m db.loader --league EPL --refresh-crossref
```

### 2. Materialized Views Refresh
Để cập nhật các bảng Profile (ảnh, logo) cho Frontend:
```bash
# Thường đã được scheduler_master chạy hàng ngày
# Có thể chạy tay trong psql:
# REFRESH MATERIALIZED VIEW mv_player_profiles;
```

---

## 🛡 Xử lý lỗi (Troubleshooting)
(Giữ nguyên)
| OS | Windows / macOS / Linux |

---

## Cài đặt

```bash
# 1. Clone
git clone https://github.com/NguyenTruong354/Vertex-Football-Scraper.git
cd Vertex-Football-Scraper

# 2. Tạo virtual environment
python -m venv .venv

# 3. Kích hoạt
.venv\Scripts\activate          # Windows CMD/PowerShell
source .venv/Scripts/activate   # Windows Git Bash
source .venv/bin/activate       # Linux/macOS

# 4. Cài dependencies
pip install -r requirements.txt
pip install nodriver

# 5. Tạo .env từ example
copy .env.example .env          # Windows
cp .env.example .env            # Linux/macOS
# Sửa .env: điền POSTGRES_PASSWORD

# 6. Khởi tạo database (chạy 1 lần)
python db/setup_db.py --schema-only
```

### `.env` template

```ini
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=vertex_football
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
```

---

## League Registry

`league_registry.py` là single source of truth. Thêm giải mới chỉ cần 1 entry.

### Giải đấu hỗ trợ hiện tại

| League ID | Tên giải | Understat | FBref | Transfermarkt |
|-----------|----------|-----------|-------|---------------|
| `EPL` | Premier League | ✅ | ✅ comp 9 | ✅ GB1 |
| `LALIGA` | La Liga | ✅ | ✅ comp 12 | ✅ ES1 |
| `BUNDESLIGA` | Bundesliga | ✅ | ✅ comp 20 | ✅ L1 |
| `SERIEA` | Serie A | ✅ | ✅ comp 11 | ✅ IT1 |
| `LIGUE1` | Ligue 1 | ✅ | ✅ comp 13 | ✅ FR1 |
| `RFPL` | Russian Premier Liga | ✅ | ❌ | ❌ |
| `UCL` | Champions League | ❌ | ✅ comp 8 | ❌ |
| `EREDIVISIE` | Eredivisie | ❌ | ✅ comp 23 | ❌ |
| `LIGA_PORTUGAL` | Primeira Liga | ❌ | ✅ comp 32 | ❌ |

### Thêm giải mới

```python
# Trong league_registry.py, thêm vào LEAGUES dict:
"SCOTTISH_PREM": LeagueConfig(
    league_id="SCOTTISH_PREM",
    display_name="Scottish Premiership",
    country="Scotland",
    understat_name=None,
    fbref_comp_id=40,
    fbref_slug="Scottish-Premiership",
    tm_comp_id="SC1",
    tm_slug="scottish-premiership",
    priority=10,
),
```

**Không cần sửa file nào khác.**

---

## Pipeline 1: Understat

Dữ liệu xG shot-level. API JSON, không cần browser.

```bash
# EPL mặc định — toàn bộ mùa 2025
python understat/async_scraper.py

# Test nhanh 5 trận
python understat/async_scraper.py --limit 5

# La Liga mùa 2024
python understat/async_scraper.py --league LALIGA --season 2024

# Bundesliga, không xuất CSV
python understat/async_scraper.py --league BUNDESLIGA --no-csv

# Xem danh sách giải hỗ trợ
python understat/async_scraper.py --list-leagues
```

| Flag | Default | Mô tả |
|------|---------|-------|
| `--league` | `EPL` | League ID |
| `--season` | `2025` | Mùa giải (năm bắt đầu) |
| `--limit` | `0` (tất cả) | Giới hạn số trận |
| `--no-csv` | False | Không xuất CSV |
| `--list-leagues` | — | In danh sách và thoát |

**Output:** `output/understat/{league}/dataset_{league}_{xg|player_stats|match_stats}.csv`

**Thời gian:** ~8–15 phút/mùa EPL đầy đủ | ~7–10s với `--limit 5`

---

## Pipeline 2: FBref

Full stats: standings, squad, players, fixtures, GK. Dùng nodriver bypass Cloudflare.

> ⚠️ **Cửa sổ Chrome sẽ tự mở** — KHÔNG đóng tay trong khi đang chạy.

```bash
# EPL mặc định — đầy đủ
python fbref/fbref_scraper.py

# Chỉ lấy standings (~15 giây)
python fbref/fbref_scraper.py --standings-only

# Test 2 đội đầu tiên (~30 giây)
python fbref/fbref_scraper.py --limit 2

# La Liga đầy đủ
python fbref/fbref_scraper.py --league LALIGA

# UCL, 5 đội
python fbref/fbref_scraper.py --league UCL --limit 5

python fbref/fbref_scraper.py --list-leagues
```

| Flag | Default | Mô tả |
|------|---------|-------|
| `--league` | `EPL` | League ID |
| `--standings-only` | False | Chỉ scrape standings |
| `--limit` | `0` (tất cả) | Giới hạn số đội |
| `--list-leagues` | — | In danh sách và thoát |

**Output:** `output/fbref/{league}/dataset_{league}_{standings|squad_stats|squad_rosters|player_season_stats|fixtures|gk_stats}.csv`

**Thời gian:** ~2–3 phút/mùa EPL đầy đủ (20 squad pages × 5s/page)

---

## Pipeline 3: SofaScore

Match events, heatmaps, avg positions, player ratings.

```bash
# EPL mặc định
python sofascore/sofascore_client.py

# Giới hạn 10 trận (tiết kiệm thời gian)
python sofascore/sofascore_client.py --limit 10

# La Liga
python sofascore/sofascore_client.py --league LALIGA
```

**Output:** `output/sofascore/{league}/dataset_{league}_{ss_events|player_avg_positions|heatmaps}.csv`

---

## Pipeline 4: Transfermarkt

Team info, manager, stadium, player market values.

```bash
# EPL mặc định
python transfermarkt/tm_scraper.py

# La Liga
python transfermarkt/tm_scraper.py --league LALIGA
```

**Output:** `output/transfermarkt/{league}/dataset_{league}_{team_metadata|market_values}.csv`

---

## Load vào PostgreSQL

```bash
# Load tất cả CSVs của EPL vào DB
python db/loader.py

# Chỉ định league
python db/loader.py --league LALIGA

# Chỉ load 1 bảng
python db/loader.py --table shots
```

---

## Live Match Tracker

Theo dõi trận đấu trực tiếp qua SofaScore API. Poll mỗi 60-90s.

```bash
# Tìm trận hôm nay
python live_match.py --today

# Theo dõi bằng event_id
python live_match.py 14023979

# Tìm trận của đội cụ thể
python live_match.py --team Arsenal

# Poll mỗi 60 giây, lưu DB
python live_match.py 14023979 --interval 60 --save-db

# Lưu CSV snapshot mỗi poll
python live_match.py 14023979 --save-csv

# Xem dữ liệu đã lưu trong DB
python live_match.py --query           # Tất cả matches đã track
python live_match.py --query 14023979  # Chi tiết 1 match
```

| Flag | Default | Mô tả |
|------|---------|-------|
| `event_id` | — | SofaScore event ID (positional) |
| `--today` | — | Hiển thị tất cả trận hôm nay |
| `--team TEAM` | — | Tìm trận theo tên đội |
| `--interval N` | `90` | Giây giữa các poll |
| `--save-db` | False | Lưu vào PostgreSQL |
| `--save-csv` | False | Xuất CSV mỗi poll |
| `--query [ID]` | — | Xem dữ liệu từ DB |

**Dashboard terminal:**
```
╔══════════════════════════════════════════╗
║  ⚽  LIVE MATCH TRACKER                 ║
╠══════════════════════════════════════════╣
║  Wolverhampton  0 – 0  Aston Villa      ║
║  Premier League | Round 28 | 33'        ║
╚══════════════════════════════════════════╝

📊 MATCH STATISTICS  [Poll #3 | 33']
  Ball possession │ Wolves 40% ████████░░░░░░░░░░░░ Villa 60%
  Expected goals  │ Wolves 0.26 ████░░░░░░░░░░░░░░░░ Villa 0.39
  Total shots     │ Wolves 2 ███░░░░░░░░░░░░░░░░░░░░ Villa 4
  Passes          │ Wolves 139 █████░░░░░░░░░░░░░░░░ Villa 204
```

---

## Chạy toàn bộ Pipeline (`run_pipeline.py`)

```bash
# Full pipeline EPL
python run_pipeline.py

# Nhiều league
python run_pipeline.py --league EPL LALIGA BUNDESLIGA

# Test nhanh
python run_pipeline.py --quick-test

# Chỉ scrape (không load DB)
python run_pipeline.py --scrape-only

# Chỉ load DB (dùng CSV đã có)
python run_pipeline.py --load-only

# Bỏ qua SofaScore
python run_pipeline.py --skip-sofascore

# Giới hạn SofaScore 10 trận
python run_pipeline.py --ss-match-limit 10
```

| Flag | Default | Mô tả |
|------|---------|-------|
| `--league` | `EPL` | 1 hoặc nhiều league IDs |
| `--scrape-only` | False | Chỉ scrape, không load DB |
| `--load-only` | False | Chỉ load CSV có sẵn |
| `--skip-understat` | False | Bỏ qua Understat |
| `--skip-fbref` | False | Bỏ qua FBref |
| `--skip-sofascore` | False | Bỏ qua SofaScore |
| `--skip-transfermarkt` | False | Bỏ qua Transfermarkt |
| `--ss-match-limit` | `0` | Giới hạn trận SS (0=tất cả) |
| `--quick-test` | False | Test nhanh: limit 2 teams, 1 match |

---

## Lưu ý quan trọng

| Vấn đề | Giải pháp |
|--------|-----------|
| Chrome bật lên khi chạy FBref/SofaScore/Transfermarkt | Bình thường — nodriver cần headed mode để bypass Cloudflare |
| Bị rate limit Understat | Tăng `POLITENESS_DELAY_SECONDS` lên `2.0` trong `understat/config.py` |
| FBref scrape chậm | Mỗi squad page mất 5s để bypass Cloudflare — bình thường |
| `RuntimeError: Event loop is closed` | Cảnh báo harmless của Windows khi nodriver cleanup |
| Live tracker không tìm thấy trận | Dùng `--today` để lấy event_id hiện tại |
