# Usage Guide — Vertex Football Scraper
> Hướng dẫn sử dụng pipeline Understat và FBref (Multi-League)

---

## Cấu trúc thư mục

```
Vertex_Football_Scraper2/
│
├── league_registry.py          ← ⭐ Single source of truth cho giải đấu
│
├── understat/                  ← Understat pipeline
│   ├── config.py               — Cấu hình API, headers, rate limit, output helpers
│   ├── schemas.py              — Pydantic models (ShotData, PlayerMatchStats…)
│   ├── extractor.py            — Parse JSON từ API response
│   └── async_scraper.py        — Script chạy chính ⭐
│
├── fbref/                      ← FBref pipeline
│   ├── config_fbref.py         — Cấu hình URLs, table IDs (dynamic theo comp_id)
│   ├── schemas_fbref.py        — Pydantic models (StandingsRow, PlayerProfile…)
│   └── fbref_scraper.py        — Script chạy chính ⭐
│
├── output/                     ← Tất cả CSV output (phân theo pipeline / giải)
│   ├── understat/
│   │   ├── epl/
│   │   │   ├── dataset_epl_xg.csv
│   │   │   ├── dataset_epl_player_stats.csv
│   │   │   └── dataset_epl_match_stats.csv
│   │   ├── laliga/
│   │   │   ├── dataset_laliga_xg.csv
│   │   │   └── …
│   │   └── bundesliga/ …
│   └── fbref/
│       ├── epl/
│       │   ├── dataset_epl_standings.csv
│       │   ├── dataset_epl_squad_stats.csv
│       │   ├── dataset_epl_squad_rosters.csv
│       │   └── dataset_epl_player_season_stats.csv
│       ├── laliga/ …
│       └── bundesliga/ …
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

## League Registry — Trung tâm quản lý giải đấu

`league_registry.py` là file duy nhất cần sửa khi thêm giải mới.

### Giải đấu hỗ trợ

| League ID | Tên | Understat | FBref |
|---|---|---|---|
| `EPL` | Premier League | ✓ | ✓ (comp 9) |
| `LALIGA` | La Liga | ✓ | ✓ (comp 12) |
| `BUNDESLIGA` | Bundesliga | ✓ | ✓ (comp 20) |
| `SERIEA` | Serie A | ✓ | ✓ (comp 11) |
| `LIGUE1` | Ligue 1 | ✓ | ✓ (comp 13) |
| `RFPL` | Russian Premier Liga | ✓ | ✗ |
| `UCL` | Champions League | ✗ | ✓ (comp 8) |
| `EREDIVISIE` | Eredivisie | ✗ | ✓ (comp 23) |
| `LIGA_PORTUGAL` | Primeira Liga | ✗ | ✓ (comp 32) |

```bash
# Xem toàn bộ danh sách từ CLI
python understat/async_scraper.py --list-leagues
python fbref/fbref_scraper.py --list-leagues
```

### Thêm giải đấu mới

Chỉ cần thêm 1 entry vào `LEAGUES` dict trong `league_registry.py`:

```python
"SCOTTISH_PREM": LeagueConfig(
    league_id="SCOTTISH_PREM",
    display_name="Scottish Premiership",
    country="Scotland",
    understat_name=None,       # None = Understat không hỗ trợ
    fbref_comp_id=40,
    fbref_slug="Scottish-Premiership",
    priority=11,
),
```

**Không cần sửa file nào khác.** Pipelines tự nhận diện qua registry.

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

**Giải đấu hỗ trợ:** EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1, RFPL

---

### Cách chạy

```bash
cd understat/

# EPL (mặc định) — toàn bộ mùa 2025-2026
python async_scraper.py

# Giới hạn số trận (test nhanh)
python async_scraper.py --limit 5

# La Liga toàn mùa
python async_scraper.py --league LALIGA

# Bundesliga, 10 trận gần nhất
python async_scraper.py --league BUNDESLIGA --limit 10

# Serie A mùa trước
python async_scraper.py --league SERIEA --season 2024

# Không xuất CSV (chỉ log kết quả)
python async_scraper.py --limit 5 --no-csv

# Scrape trực tiếp một số trận theo match ID
python async_scraper.py 28778 28779 28780

# Xem danh sách giải đấu hỗ trợ
python async_scraper.py --list-leagues
```

---

### Tham số CLI

| Tham số | Mặc định | Mô tả |
|---|---|---|
| `--league` | `EPL` | League ID (xem `--list-leagues`) |
| `--season` | `2025` | Mùa giải (năm bắt đầu, VD: 2024) |
| `--limit` | `0` (tất cả) | Giới hạn số trận gần nhất |
| `--no-csv` | False | Không xuất CSV (chỉ log kết quả) |
| `--list-leagues` | — | In danh sách giải đấu và thoát |
| `[match_ids...]` | — | Scrape trực tiếp theo match ID |

---

### Luồng xử lý

```
1. CLI parse args → validate league qua registry
         │
         ▼
2. Fetch league data từ Understat API
   GET /getLeagueData/{understat_name}/{season}
   → Nhận list matches (match_id, teams, scores, dates)
         │
         ▼
3. Async fetch từng match (concurrency = 6)
   GET /getMatchData/{match_id}
   → JSON: { "shotsData": {...}, "rostersData": {...} }
         │
         ▼
4. extractor.py parse JSON
   → flatten_shots()        → list[ShotData]
   → flatten_rosters()      → list[PlayerMatchStats]
   → build_match_info()     → list[MatchInfo]
         │
         ▼
5. schemas.py validate với Pydantic
         │
         ▼
6. Export to output/understat/{league_id}/
   → dataset_{league}_xg.csv
   → dataset_{league}_player_stats.csv
   → dataset_{league}_match_stats.csv
```

---

### Output mẫu

```
23:10:01 | INFO | Vertex Football Scraper – Pipeline bắt đầu
23:10:01 | INFO |    League: LALIGA (La Liga) | Season: 2025
23:10:03 | INFO | ✓ Tìm thấy 311 trận đã diễn ra (tổng 380) trong mùa 2025/La_liga
...
23:10:08 | INFO | ═══ PIPELINE SUMMARY ═══
23:10:08 | INFO |   Trận đã xử lý   : 5 / 5
23:10:08 | INFO |   Tổng shots       : 134
23:10:08 | INFO |   Tổng player stats: 152
23:10:08 | INFO | Exported shots → output\understat\laliga\dataset_laliga_xg.csv (134 rows)
23:10:08 | INFO | Exported player stats → output\understat\laliga\dataset_laliga_player_stats.csv (152 rows)
23:10:08 | INFO | Pipeline hoàn thành trong 7.8 giây.
```

---

### Thời gian ước tính

| Lệnh | Số trận | Thời gian |
|---|---|---|
| `--limit 5` | 5 | ~7–10 giây |
| `--limit 50` | 50 | ~1–2 phút |
| (không limit) | ~310–380 | ~8–15 phút |

---

### Lưu ý

- Nếu bị rate limit, tăng `POLITENESS_DELAY_SECONDS` lên `2.0` trong `understat/config.py`.
- Các trận chưa diễn ra (future matches) có `h_xg`/`a_xg` = `null` — bình thường.
- `MAX_CONCURRENT_REQUESTS = 6` là ngưỡng an toàn. Không tăng quá 10.

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

**Giải đấu hỗ trợ:** EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1, UCL, EREDIVISIE, LIGA_PORTUGAL

> ⚠️ **Cửa sổ Chrome sẽ tự mở** khi chạy FBref pipeline — đây là bình thường,
> KHÔNG đóng tay cửa sổ đó trong lúc pipeline đang chạy.

---

### Cách chạy

```bash
cd fbref/

# EPL (mặc định) — standings + 20 squad pages
python fbref_scraper.py

# Chỉ lấy standings (~15 giây)
python fbref_scraper.py --standings-only

# Test với 2 đội đầu tiên (~30 giây)
python fbref_scraper.py --limit 2

# La Liga — đầy đủ
python fbref_scraper.py --league LALIGA

# Bundesliga — chỉ standings
python fbref_scraper.py --league BUNDESLIGA --standings-only

# Champions League — 5 đội
python fbref_scraper.py --league UCL --limit 5

# Xem danh sách giải đấu hỗ trợ
python fbref_scraper.py --list-leagues
```

---

### Tham số CLI

| Tham số | Mặc định | Mô tả |
|---|---|---|
| `--league` | `EPL` | League ID (xem `--list-leagues`) |
| `--standings-only` | False | Chỉ scrape standings, bỏ squad pages |
| `--limit` | `0` (tất cả) | Giới hạn số đội scrape |
| `--list-leagues` | — | In danh sách giải đấu và thoát |

---

### Luồng xử lý

```
1. CLI parse args → validate league qua registry
   → get_fbref_config(league_id) build URL + table IDs tự động
         │
         ▼
2. FBrefBrowser mở Chrome (headed)
         │
         ▼
3. STEP 1: Fetch League Overview Page
   GET https://fbref.com/en/comps/{comp_id}/{slug}-Stats
   → Chờ Cloudflare JS challenge pass (~6–15 giây)
   → Parse standings table    → list[StandingsRow]
   → Parse squad stats table  → list[SquadStats]
   → Extract team links       → [{"name", "team_id", "url"}]
         │
         ▼
4. STEP 2: Lần lượt fetch squad pages (delay 5s/page)
   GET /en/squads/{team_id}/{team_slug}-Stats
   → Parse stats_standard_{comp_id}  → profiles + season stats
   → Parse stats_shooting_{comp_id}  → shooting data (merged)
         │
         ▼
5. Export to output/fbref/{league_id}/
   → dataset_{league}_standings.csv
   → dataset_{league}_squad_stats.csv
   → dataset_{league}_squad_rosters.csv
   → dataset_{league}_player_season_stats.csv
```

---

### Output mẫu

```
23:47:28 | INFO | FBREF DATA PIPELINE – BẮT ĐẦU
23:47:28 | INFO |   League: LALIGA (comp_id=12)
23:47:28 | INFO |   Season: 2025-2026
23:47:28 | INFO |   URL:    https://fbref.com/en/comps/12/La-Liga-Stats
23:47:28 | INFO | ▶ Khởi động Chrome browser (headed mode)…
23:47:35 | INFO |   ✓ Cloudflare passed sau 7s – La Liga Stats | FBref.com
23:47:38 | INFO |   Standings: 20 teams parsed
23:47:38 | INFO |   Squad stats: 20 teams parsed
23:47:38 | INFO |   Team links: 20 teams found
23:47:38 | INFO | ━━ STEP 2: Scrape 20 Squad Pages ━━
23:47:38 | INFO | ── [1/20] Real Madrid ──
...
23:49:45 | INFO | Exported standings → output\fbref\laliga\dataset_laliga_standings.csv (20 rows)
23:49:45 | INFO | Exported player stats → output\fbref\laliga\dataset_laliga_player_season_stats.csv (571 rows)
23:49:45 | INFO |   Thời gian: 141.3 giây
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
> Chỉnh `FBREF_DELAY_BETWEEN_PAGES` trong `fbref/config_fbref.py`.

**3. Nếu Cloudflare không pass**
> Đôi khi CF thách thức lâu hơn bình thường (CAPTCHA). Nếu thấy log:
> `⚠ Cloudflare timeout sau 45s` → Chờ vài phút và chạy lại.

**4. FBref cập nhật dữ liệu hàng ngày**
> Nên scrape sau 12:00 trưa (UK time) để có dữ liệu mới nhất sau vòng đấu cuối tuần.

---

## Chạy cả 2 pipelines

```bash
# Understat trước (nhanh, không cần browser)
cd understat/
python async_scraper.py --league EPL
cd ..

# FBref sau (cần browser, lâu hơn)
cd fbref/
python fbref_scraper.py --league EPL
```

Hoặc dùng script tổng hợp `run_all.py`:

```python
# run_all.py
import subprocess, sys

LEAGUE = "EPL"    # Đổi tại đây để chạy giải khác
SEASON = "2025"   # Understat season

python = sys.executable

print(f"=== STEP 1: Understat ({LEAGUE}) ===")
subprocess.run(
    [python, "understat/async_scraper.py", "--league", LEAGUE, "--season", SEASON],
    check=True,
)

print(f"\n=== STEP 2: FBref ({LEAGUE}) ===")
subprocess.run(
    [python, "fbref/fbref_scraper.py", "--league", LEAGUE],
    check=True,
)

print(f"\n✅ DONE — CSV đã xuất vào output/understat/{LEAGUE.lower()}/ và output/fbref/{LEAGUE.lower()}/")
```

```bash
python run_all.py
```

---

## Cấu hình nâng cao

### Thay đổi mùa giải mặc định

Sửa trong `league_registry.py`:

```python
"EPL": LeagueConfig(
    ...
    understat_season="2025",    # ← Understat season (năm bắt đầu)
    fbref_season="2025-2026",   # ← FBref season (2 năm)
    fbref_season_short="2025",
),
```

### Tắt giải đấu tạm thời

```python
"RFPL": LeagueConfig(
    ...
    active=False,   # ← Không xuất hiện trong --list-leagues
),
```

### Chỉnh rate limit

```python
# understat/config.py
MAX_CONCURRENT_REQUESTS: int = 6         # Số request đồng thời (≤ 10)
POLITENESS_DELAY_SECONDS: float = 0.5    # Delay giữa các request

# fbref/config_fbref.py
FBREF_DELAY_BETWEEN_PAGES: float = 5.0   # Delay giữa squad pages (≥ 3.0)
FBREF_CF_WAIT_MAX: int = 45              # Timeout Cloudflare (giây)
```

---

## Troubleshooting

| Lỗi | Nguyên nhân | Cách fix |
|---|---|---|
| `KeyError: League 'XYZ'` | League ID không tồn tại | Chạy `--list-leagues` để xem ID đúng |
| `ValueError: không có FBref` | Giải chưa có FBref comp_id | Thêm `fbref_comp_id` vào registry |
| `HTTP 403` (Understat) | Rate limit hoặc IP block | Tăng `POLITENESS_DELAY_SECONDS` |
| `⚠ Cloudflare timeout` | FBref CF challenge quá lâu | Chờ vài phút và chạy lại |
| `No tables found` | FBref đổi table ID | Kiểm tra comp_id trong registry |
| `0 matches found` | Understat đổi tên giải | Kiểm tra `understat_name` trong registry |
| `ValidationError` | Schema không match data mới | Chỉnh model trong `schemas.py` / `schemas_fbref.py` |
| Chrome crash | RAM thiếu hoặc Chrome version cũ | Đóng bớt app, cập nhật Chrome |

---

## Lịch chạy khuyến nghị (Production)

| Tần suất | Lệnh | Ghi chú |
|---|---|---|
| Sau mỗi vòng đấu | `understat/async_scraper.py --league EPL --limit 10` | Cập nhật xG 10 trận mới nhất |
| Sau mỗi vòng đấu | `fbref/fbref_scraper.py --league EPL --standings-only` | Cập nhật BXH |
| Mỗi tuần | `fbref/fbref_scraper.py --league EPL` | Cập nhật player stats toàn mùa |
| Đầu mùa | Cả 2 pipelines (không `--limit`) | Khởi tạo dataset mới |
| Mở rộng giải | Thêm vào `league_registry.py` + chạy lại | Không cần sửa pipeline code |
