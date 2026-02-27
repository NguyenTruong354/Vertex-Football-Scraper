# ============================================================
# config.py – Cấu hình trung tâm cho toàn bộ pipeline
# ============================================================
"""
Chứa:
  • HTTP headers giả lập trình duyệt (tránh bị chặn)
  • URL templates cho Understat
  • Tham số điều khiển concurrency (Semaphore)
  • Cấu hình retry / rate-limit
  • Đường dẫn xuất dữ liệu

League/season defaults giờ được quản lý bởi league_registry.py.
Config này chỉ chứa các hằng số kỹ thuật (HTTP, retry, paths).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Import league registry (nằm ở thư mục cha)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from league_registry import LeagueConfig, get_league  # noqa: E402

# ────────────────────────────────────────────────────────────
# 1. HTTP HEADERS
# ────────────────────────────────────────────────────────────
# Giả lập một trình duyệt Chrome thật để tránh server từ chối request.
# User-Agent, Accept-Language, … giúp request trông giống traffic
# thông thường thay vì bot.

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*;q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    # ── BẮT BUỘC ── Understat kiểm tra header này để phân biệt
    # AJAX request (từ JS frontend) và request thông thường.
    # Thiếu header này → server trả HTML trống thay vì JSON data.
    "X-Requested-With": "XMLHttpRequest",
}

# ────────────────────────────────────────────────────────────
# 2. URL TEMPLATES – UNDERSTAT (JSON API)
# ────────────────────────────────────────────────────────────
# Understat sử dụng internal JSON API (AJAX endpoints).
# Frontend JS gọi các endpoint này để lấy dữ liệu sau khi
# trang HTML đã tải xong.
#
# Phát hiện bằng cách đọc js/match.min.js và js/league.min.js:
#   $.ajax({url: "getMatchData/" + match_info.id, ...})
#   $.ajax({url: "getLeagueData/" + league + "/" + season, ...})

BASE_URL: str = "https://understat.com"

# ── JSON API Endpoints ──
# Trả về JSON trực tiếp (content-type: text/javascript)
# với keys: {rosters, shots, tmpl}
MATCH_API_URL: str = f"{BASE_URL}/getMatchData/{{match_id}}"

# Trả về JSON với keys: {teams, players, dates}
LEAGUE_API_URL: str = f"{BASE_URL}/getLeagueData/{{league_name}}/{{season}}"

# ── HTML Pages (dùng cho Referer header) ──
MATCH_PAGE_URL: str = f"{BASE_URL}/match/{{match_id}}"
LEAGUE_PAGE_URL: str = f"{BASE_URL}/league/{{league_name}}/{{season}}"

# ────────────────────────────────────────────────────────────
# 3. CONCURRENCY – Semaphore
# ────────────────────────────────────────────────────────────
# asyncio.Semaphore giới hạn số coroutine chạy đồng thời.
#
# Cách hoạt động:
#   sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
#   async with sem:          # <— acquire slot (block nếu đã đầy)
#       await fetch(url)     # chỉ tối đa N request cùng lúc
#                            # <— tự động release khi ra khỏi with
#
# Chọn 5-10 để vừa nhanh, vừa không gây 429 Too Many Requests.

MAX_CONCURRENT_REQUESTS: int = 6

# Thời gian chờ tối đa cho mỗi HTTP request (giây)
REQUEST_TIMEOUT_SECONDS: int = 30

# Delay nhỏ giữa các request trong cùng batch (giây) – giảm tải server
POLITENESS_DELAY_SECONDS: float = 0.5

# ────────────────────────────────────────────────────────────
# 4. RETRY / TENACITY
# ────────────────────────────────────────────────────────────
# Exponential backoff: lần 1 chờ 2s, lần 2 chờ 4s, … tối đa 60s.
# Retry tối đa 5 lần.  Chỉ retry với status 429 (rate-limit) hoặc
# 5xx (server error).

RETRY_MAX_ATTEMPTS: int = 5
RETRY_WAIT_MIN_SECONDS: float = 2.0
RETRY_WAIT_MAX_SECONDS: float = 60.0
RETRY_MULTIPLIER: float = 2.0            # hệ số nhân mỗi lần

RETRYABLE_HTTP_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

# ────────────────────────────────────────────────────────────
# 5. OUTPUT / EXPORT
# ────────────────────────────────────────────────────────────
# Thư mục output tập trung (project root / output / understat / {league})
OUTPUT_BASE: Path = Path(__file__).resolve().parent.parent / "output"


def get_output_dir(league_id: str) -> Path:
    """Trả về thư mục output cho giải đấu cụ thể, tự tạo nếu chưa có."""
    out = OUTPUT_BASE / "understat" / league_id.lower()
    out.mkdir(parents=True, exist_ok=True)
    return out


def get_csv_filenames(league_id: str) -> dict[str, str]:
    """
    Trả về dict tên file CSV động theo league_id.

    VD: league_id="EPL" → {
        "shots":        "dataset_epl_xg.csv",
        "player_stats": "dataset_epl_player_stats.csv",
        "match_stats":  "dataset_epl_match_stats.csv",
    }
    """
    prefix = league_id.lower()
    return {
        "shots": f"dataset_{prefix}_xg.csv",
        "player_stats": f"dataset_{prefix}_player_stats.csv",
        "match_stats": f"dataset_{prefix}_match_stats.csv",
    }


# ── Legacy aliases (backward compatibility) ──
OUTPUT_DIR: Path = get_output_dir("EPL")
SHOTS_CSV_FILENAME: str = "dataset_epl_xg.csv"
PLAYER_STATS_CSV_FILENAME: str = "dataset_epl_player_stats.csv"
MATCH_STATS_CSV_FILENAME: str = "dataset_epl_match_stats.csv"

# ────────────────────────────────────────────────────────────
# 6. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"
LOG_FORMAT: str = (
    "%(log_color)s%(asctime)s | %(levelname)-8s | %(name)s | %(message)s%(reset)s"
)
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"

# ────────────────────────────────────────────────────────────
# 7. LEAGUE / SEASON DEFAULTS
# ────────────────────────────────────────────────────────────
# Defaults giờ lấy từ league_registry.py
# Giữ lại biến cũ để backward-compatible với CLI / scripts cũ
DEFAULT_LEAGUE: str = "EPL"          # English Premier League
DEFAULT_SEASON: str = "2025"         # Mùa 2025-2026
