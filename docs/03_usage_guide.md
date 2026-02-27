# Usage Guide — Vertex Football Scraper
> Hướng dẫn sử dụng pipeline Understat và FBref

---

## Cấu trúc thư mục

```
Vertex_Football_Scraper2/
│
├── understat/                  ← Understat pipeline
│   ├── config.py               — Cấu hình API, headers, rate limit
│   ├── schemas.py              — Pydantic models (ShotData, PlayerMatchStats…)
│   ├── extractor.py            — Parse JSON từ API response
│   └── async_scraper.py        — Script chạy chính ⭐
│
├── fbref/                      ← FBref pipeline
│   ├── config_fbref.py         — Cấu hình URLs, table IDs, rate limit
│   ├── schemas_fbref.py        — Pydantic models (StandingsRow, PlayerProfile…)
│   └── fbref_scraper.py        — Script chạy chính ⭐
│
├── output/                     ← Tất cả CSV output
│   ├── dataset_epl_xg.csv
│   ├── dataset_epl_player_stats.csv
│   ├── dataset_epl_match_stats.csv
│   ├── dataset_epl_standings.csv
│   ├── dataset_epl_squad_stats.csv
│   ├── dataset_epl_squad_rosters.csv
│   └── dataset_epl_player_season_stats.csv
│
├── docs/                       ← Tài liệu
├── requirements.txt
└── .venv/                      ← Virtual environment
```

---

## Yêu cầu hệ thống

| Yêu cầu | Chi tiết |
|---|---|
| Python | 3.11+ |
| Chrome browser | Cài sẵn trên máy (cho FBref) |
| OS | Windows / macOS / Linux |
| RAM | Tối thiểu 4GB (Chrome + pipeline) |
| Disk | ~100MB cho output CSV |

---

## Cài đặt môi trường

```bash
# 1. Clone repo
git clone https://github.com/NguyenTruong354/Vertex-Football-Scraper.git
cd Vertex-Football-Scraper

# 2. Tạo virtual environment
python -m venv .venv

# 3. Kích hoạt (Windows)
.venv\Scripts\activate

# 4. Cài dependencies
pip install -r requirements.txt
```

---

## ═══════════════════════════════
## PIPELINE 1: UNDERSTAT
## ═══════════════════════════════

### Tổng quan

Understat cung cấp dữ liệu **xG (Expected Goals)** chi tiết nhất miễn phí.

**Dữ liệu thu thập:**
- Shot-level xG (tọa độ x/y, loại tình huống, kết quả)
- Player xG/xA/xGChain/xGBuildup per match
- Match aggregates (tổng xG home/away)

**Kỹ thuật:** Gọi JSON API nội bộ của Understat (không cần browser).
API trả về `text/javascript`, parse bằng `json.loads()`.

---

### Cách chạy

```bash
# Từ thư mục gốc của project
cd understat/

# Chạy tất cả 380 trận EPL 2025-2026
python async_scraper.py

# Chạy thử 5 trận đầu tiên
python async_scraper.py --limit 5

# Chạy 20 trận
python async_scraper.py --limit 20
```

---

### Luồng xử lý

```
1. async_scraper.py load config
         │
         ▼
2. Fetch league data từ Understat API
   GET /getLeagueData/EPL/2025
   → Nhận list 380 matches (match_id, teams, scores, dates)
         │
         ▼
3. Async fetch từng match (concurrency = 5)
   GET /getMatchData/{match_id}
   → Trả về JSON: { "shotsData": {...}, "rostersData": {...} }
         │
         ▼
4. extractor.py parse JSON
   → flatten_shots()           → list[ShotData]
   → flatten_player_stats()    → list[PlayerMatchStats]
   → flatten_match_stats()     → list[MatchInfo]
         │
         ▼
5. schemas.py validate với Pydantic
   → Báo lỗi nếu data invalid, bỏ qua row lỗi
         │
         ▼
6. Export to CSV
   → output/dataset_epl_xg.csv
   → output/dataset_epl_player_stats.csv
   → output/dataset_epl_match_stats.csv
```

---

### Cấu hình (`understat/config.py`)

```python
# Các setting quan trọng có thể chỉnh:

LEAGUE = "EPL"              # Giải đấu
SEASON = 2025               # Mùa giải (dùng năm bắt đầu)

CONCURRENCY_LIMIT = 5       # Số request đồng thời (khuyến nghị ≤ 5)
REQUEST_DELAY = 1.0         # Delay giữa các batch (giây)

# Retry settings
MAX_RETRIES = 3
RETRY_WAIT_MIN = 2.0
RETRY_WAIT_MAX = 10.0
```

---

### Output mẫu

```
23:10:01 | INFO | UNDERSTAT DATA PIPELINE – BẮT ĐẦU
23:10:01 | INFO | Season: EPL/2025 | Limit: 5
23:10:03 | INFO | [1/5] match_id=21234 — Arsenal vs Chelsea
23:10:04 | INFO | [2/5] match_id=21235 — Liverpool vs Man City
...
23:10:08 | INFO | ═══ DATA QUALITY REPORT ═══
23:10:08 | INFO |   Shots validated   : 127 / 127
23:10:08 | INFO |   Player stats      : 147 / 147
23:10:08 | INFO |   Match aggregates  : 5 / 5
23:10:08 | INFO | ═══ XUẤT FILE ═══
23:10:08 | INFO |   → output/dataset_epl_xg.csv
23:10:08 | INFO |   → output/dataset_epl_player_stats.csv
23:10:08 | INFO |   → output/dataset_epl_match_stats.csv
23:10:08 | INFO | Thời gian: 7.3 giây
```

---

### Thời gian ước tính

| Lệnh | Số trận | Thời gian |
|---|---|---|
| `--limit 5` | 5 | ~7–10 giây |
| `--limit 50` | 50 | ~1–2 phút |
| (không limit) | 380 | ~8–15 phút |

---

### Lưu ý

- Understat có rate limiting nhẹ. Nếu bị block, tăng `REQUEST_DELAY` lên `2.0`.
- Các trận chưa diễn ra (future matches) sẽ có `h_xg` / `a_xg` = `null` — đây là bình thường.
- `CONCURRENCY_LIMIT = 5` là ngưỡng an toàn. Không nên tăng quá 10.

---

## ═══════════════════════════════
## PIPELINE 2: FBREF
## ═══════════════════════════════

### Tổng quan

FBref (powered by StatsBomb) cung cấp dữ liệu **thống kê đầy đủ nhất** miễn phí.

**Dữ liệu thu thập:**
- Bảng xếp hạng (standings)
- Thống kê tổng hợp đội bóng (squad stats)
- Hồ sơ cầu thủ (player profiles)
- Thống kê cầu thủ toàn mùa (player season stats)

**Kỹ thuật:** FBref dùng **Cloudflare JS Challenge** — mọi HTTP client
thông thường đều bị chặn (HTTP 403). Giải pháp duy nhất: dùng
**`nodriver`** (Undetected Chromium) ở chế độ **headed** (có cửa sổ trình duyệt).

> ⚠️ **Cửa sổ Chrome sẽ tự mở** khi chạy FBref pipeline — đây là bình thường,
> KHÔNG đóng tay cửa sổ đó trong lúc pipeline đang chạy.

---

### Cách chạy

```bash
# Từ thư mục gốc của project
cd fbref/

# Chạy đầy đủ: standings + 20 squad pages (~3–4 phút)
python fbref_scraper.py

# Chỉ lấy standings (~15 giây)
python fbref_scraper.py --standings-only

# Test với 2 đội đầu tiên (~30 giây)
python fbref_scraper.py --limit 2

# Test với 5 đội đầu tiên (~1 phút)
python fbref_scraper.py --limit 5
```

---

### Luồng xử lý

```
1. fbref_scraper.py khởi động
         │
         ▼
2. FBrefBrowser mở Chrome (headed)
   → nodriver.start(headless=False)
         │
         ▼
3. STEP 1: Fetch League Overview Page
   GET https://fbref.com/en/comps/9/Premier-League-Stats
   → Chờ Cloudflare JS challenge pass (~6–15 giây)
   → Chờ <table> elements xuất hiện
   → Lấy full HTML (outerHTML)
         │
         ▼
4. parse_league_page(html)
   → Parse standings table    → list[StandingsRow]
   → Parse squad stats table  → list[SquadStats]
   → Extract 20 team links    → [{"name", "team_id", "url"}]
         │
         ▼
5. STEP 2: Lần lượt fetch 20 Squad Pages
   (mỗi trang cách nhau 5 giây)
   GET /en/squads/{team_id}/{team_slug}-Stats
   → HTML ~600KB / trang
         │
         ▼
6. parse_squad_page(html, team_name, team_id)
   → Parse stats_standard_9 table  → profiles + season stats
   → Parse stats_shooting_9 table  → shooting data
   → Merge shooting vào player stats
         │
         ▼
7. schemas_fbref.py validate với Pydantic
   → PlayerProfile, PlayerSeasonStats
         │
         ▼
8. Export to CSV
   → output/dataset_epl_standings.csv
   → output/dataset_epl_squad_stats.csv
   → output/dataset_epl_squad_rosters.csv
   → output/dataset_epl_player_season_stats.csv
```

---

### Cấu hình (`fbref/config_fbref.py`)

```python
# Các setting quan trọng có thể chỉnh:

FBREF_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
# Đổi /9/ thành số comp khác để scrape giải khác:
# La Liga = /12/, Bundesliga = /20/, Serie A = /11/, Ligue 1 = /13/

FBREF_DELAY_BETWEEN_PAGES = 5.0   # Giây delay giữa các page (KHÔNG giảm xuống dưới 3.0)
FBREF_CF_WAIT_MAX = 45            # Giây tối đa chờ Cloudflare pass

# Table IDs (nếu FBref đổi cấu trúc)
SQUAD_STANDARD_TABLE_ID = "stats_standard_9"    # 9 = EPL comp ID
SQUAD_SHOOTING_TABLE_ID = "stats_shooting_9"
```

---

### Output mẫu

```
23:47:28 | INFO | FBREF DATA PIPELINE – BẮT ĐẦU
23:47:28 | INFO | ▶ Khởi động Chrome browser (headed mode)…
23:47:29 | INFO | ━━ STEP 1: Scrape League Page ━━
23:47:29 | INFO | 📥 Fetching: https://fbref.com/en/comps/9/Premier-League-Stats
23:47:35 | INFO |   ✓ Cloudflare passed sau 6s – Premier League Stats | FBref.com
23:47:38 | INFO |   ✓ HTML: 968199 bytes, tables=True
23:47:38 | INFO |   Standings: 20 teams parsed
23:47:38 | INFO |   Squad stats: 20 teams parsed
23:47:38 | INFO |   Team links: 20 teams found
23:47:38 | INFO | ━━ STEP 2: Scrape 20 Squad Pages ━━
23:47:38 | INFO | ── [1/20] Arsenal ──
23:47:38 | INFO | 📥 Fetching: https://fbref.com/en/squads/18bb7c10/Arsenal-Stats
23:47:43 | INFO |   ✓ HTML: 610844 bytes, tables=True
23:47:43 | INFO |   Arsenal: 28 players, 24 with shooting data
...
23:49:45 | INFO | ━━ STEP 3: Export CSV ━━
23:49:45 | INFO | Exported standings → output/dataset_epl_standings.csv (20 rows)
23:49:45 | INFO | Exported squad stats → output/dataset_epl_squad_stats.csv (20 rows)
23:49:45 | INFO | Exported player profiles → output/dataset_epl_squad_rosters.csv (583 rows)
23:49:45 | INFO | Exported player stats → output/dataset_epl_player_season_stats.csv (583 rows)
23:49:45 | INFO | ═══ FBREF PIPELINE SUMMARY ═══
23:49:45 | INFO |   Standings rows  : 20
23:49:45 | INFO |   Squad stats     : 20
23:49:45 | INFO |   Player profiles : 583
23:49:45 | INFO |   Player stats    : 583
23:49:45 | INFO |   CSV files       : 4
23:49:45 | INFO |   Thời gian       : 137.2 giây
```

---

### Thời gian ước tính

| Lệnh | Nội dung | Thời gian |
|---|---|---|
| `--standings-only` | 1 page (standings + squad stats) | ~15 giây |
| `--limit 2` | 1 league page + 2 squad pages | ~30 giây |
| `--limit 5` | 1 league page + 5 squad pages | ~55 giây |
| (không limit) | 1 league page + 20 squad pages | ~2.5–4 phút |

---

### Lưu ý quan trọng

**1. KHÔNG đóng cửa sổ Chrome thủ công**
> Chrome mở do pipeline mở — nếu đóng tay, pipeline sẽ crash.
> Cửa sổ tự đóng khi pipeline hoàn thành.

**2. KHÔNG giảm delay xuống dưới 3 giây**
> FBref rate limit nghiêm ngặt. Nếu scrape quá nhanh, IP có thể bị block 24h.

**3. Nếu Cloudflare không pass**
> Đôi khi CF thách thức lâu hơn bình thường (CAPTCHA). Nếu thấy log:
> `⚠ Cloudflare timeout sau 45s` → Chờ vài phút và chạy lại.

**4. FBref cập nhật dữ liệu hàng ngày**
> Nên scrape sau 12:00 trưa (UK time) để có dữ liệu mới nhất sau vòng đấu cuối tuần.

---

## Chạy cả 2 pipelines

```bash
# Chạy Understat trước (nhanh, không cần browser)
cd understat/
python async_scraper.py
cd ..

# Rồi mới chạy FBref (cần browser, lâu hơn)
cd fbref/
python fbref_scraper.py
cd ..
```

Hoặc tạo một script tổng hợp:

```python
# run_all.py
import subprocess, sys

python = sys.executable
print("=== STEP 1: Understat ===")
subprocess.run([python, "understat/async_scraper.py"], check=True)

print("\n=== STEP 2: FBref ===")
subprocess.run([python, "fbref/fbref_scraper.py"], check=True)

print("\n✅ DONE — Tất cả CSV đã xuất vào output/")
```

```bash
python run_all.py
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `HTTP 403` | Cloudflare block (Understat API) | Tăng delay, đổi User-Agent trong `config.py` |
| `⚠ Cloudflare timeout` | FBref CF challenge quá lâu | Chờ vài phút và chạy lại |
| `No tables found` | FBref thay đổi cấu trúc trang | Kiểm tra table ID trong `config_fbref.py` |
| `0 matches found` | Understat đổi API endpoint | Kiểm tra `LEAGUE_API_URL` trong `config.py` |
| `ValidationError` | Schema không match data mới | Chỉnh model trong `schemas.py` hoặc `schemas_fbref.py` |
| Chrome crash | RAM thiếu | Đóng bớt ứng dụng, chạy lại |
| `nodriver` error | Chrome chưa cài hoặc sai version | Cài Chrome từ google.com/chrome |

---

## Lịch chạy khuyến nghị (Production)

| Tần suất | Pipeline | Ghi chú |
|---|---|---|
| Sau mỗi vòng đấu | Understat `--limit 10` | Cập nhật xG 10 trận mới nhất |
| Sau mỗi vòng đấu | FBref `--standings-only` | Cập nhật BXH |
| Mỗi tuần 1 lần | FBref (full) | Cập nhật stats cầu thủ toàn mùa |
| Đầu mùa giải | Cả 2 pipelines (full) | Khởi tạo dataset mới |
