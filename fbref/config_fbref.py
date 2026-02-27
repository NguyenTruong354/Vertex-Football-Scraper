# ============================================================
# config_fbref.py – Cấu hình cho FBref Data Pipeline
# ============================================================
"""
FBref (powered by StatsBomb) cung cấp dữ liệu chiến thuật
phong phú nhất miễn phí cho bóng đá châu Âu.

Đặc điểm kỹ thuật:
  • FBref sử dụng Cloudflare protection → cần browser thật
    (nodriver) để bypass JS challenge.
  • Rate-limit nghiêm ngặt (~20 requests/phút) → delay 5s
    giữa các request.
  • Dữ liệu nằm trong HTML tables (<table>) với data-stat
    attributes cho mỗi cột.
  • Một số bảng bị ẩn trong HTML comments (<!-- ... -->),
    cần uncomment trước khi parse.

Cấu trúc URL:
  • League overview: /en/comps/9/Premier-League-Stats
  • Squad page:      /en/squads/{team_id}/{team_slug}
  • Match report:    /en/matches/{match_id}/{match_slug}
"""

from pathlib import Path

# ────────────────────────────────────────────────────────────
# 1. BASE URLs
# ────────────────────────────────────────────────────────────
FBREF_BASE: str = "https://fbref.com"

# League overview — chứa BXH + squad-level stats
FBREF_LEAGUE_URL: str = f"{FBREF_BASE}/en/comps/9/Premier-League-Stats"

# ────────────────────────────────────────────────────────────
# 2. TABLE IDs (HTML)
# ────────────────────────────────────────────────────────────
# FBref đặt id cho <table> elements:
#   • BXH tổng:  results{YYYY}-{YYYY2}{comp_id}_overall
#   • Squad page: stats_standard_{comp_id}  (comp_id = 9 cho EPL)
STANDINGS_TABLE_ID_PATTERN: str = "results{season_long}_overall"
# VD: results2025-202691_overall  (season_long = "2025-202691")
# Ta sẽ tìm bằng regex thay vì hardcode

# Squad page table IDs (comp_id = 9)
SQUAD_STANDARD_TABLE_ID: str = "stats_standard_9"
SQUAD_SHOOTING_TABLE_ID: str = "stats_shooting_9"
SQUAD_KEEPER_TABLE_ID: str = "stats_keeper_9"
SQUAD_PLAYING_TIME_TABLE_ID: str = "stats_playing_time_9"
SQUAD_MISC_TABLE_ID: str = "stats_misc_9"

# ────────────────────────────────────────────────────────────
# 3. RATE LIMITING
# ────────────────────────────────────────────────────────────
# FBref chặn IP rất nhanh.
# → delay 5 giây giữa mỗi page navigation
# → chỉ 1 request tại 1 thời điểm (browser chỉ có 1 tab)
FBREF_DELAY_BETWEEN_PAGES: float = 5.0

# Thời gian chờ Cloudflare challenge (giây)
FBREF_CF_WAIT_MAX: int = 45

# Thời gian chờ thêm sau khi tables xuất hiện (giây)
FBREF_TABLE_LOAD_WAIT: float = 2.0

# ────────────────────────────────────────────────────────────
# 4. SEASON
# ────────────────────────────────────────────────────────────
FBREF_SEASON: str = "2025-2026"
FBREF_SEASON_SHORT: str = "2025"

# ────────────────────────────────────────────────────────────
# 5. OUTPUT
# ────────────────────────────────────────────────────────────
OUTPUT_DIR: Path = Path(__file__).resolve().parent / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

STANDINGS_CSV: str = "dataset_epl_standings.csv"
SQUAD_ROSTER_CSV: str = "dataset_epl_squad_rosters.csv"
PLAYER_SEASON_STATS_CSV: str = "dataset_epl_player_season_stats.csv"
SQUAD_STATS_CSV: str = "dataset_epl_squad_stats.csv"

# ────────────────────────────────────────────────────────────
# 6. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"
