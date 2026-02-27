# ============================================================
# config_tm.py – Cấu hình cho Transfermarkt Data Pipeline
# ============================================================
"""
Transfermarkt cung cấp team metadata (stadium, manager, formation)
và player market values — dữ liệu không có trên FBref/Understat.

Đặc điểm kỹ thuật:
  • Transfermarkt sử dụng Cloudflare protection → cần browser thật
    (nodriver) để bypass.
  • Rate-limit nghiêm ngặt → delay 5-8s giữa các request.
  • Dữ liệu nằm trong HTML tables + structured div elements.
  • Headers User-Agent BẮT BUỘC phải realistic.

Cấu trúc URL:
  • League overview: /premier-league/startseite/wettbewerb/GB1/saison_id/2025
  • Team page:       /{team-slug}/startseite/verein/{team_id}/saison_id/2025
  • Market values:   /{team-slug}/kader/verein/{team_id}/saison_id/2025/plus/1

Refactored: League config lấy từ league_registry.py (tm_comp_id, tm_slug).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Import league registry (nằm ở thư mục cha)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from league_registry import LeagueConfig, get_league  # noqa: E402

# ────────────────────────────────────────────────────────────
# 1. BASE URLs
# ────────────────────────────────────────────────────────────
TM_BASE: str = "https://www.transfermarkt.com"

# ────────────────────────────────────────────────────────────
# 2. RATE LIMITING & BROWSER
# ────────────────────────────────────────────────────────────
TM_DELAY_BETWEEN_PAGES: float = 6.0
TM_CF_WAIT_MAX: int = 45
TM_TABLE_LOAD_WAIT: float = 2.0

# ────────────────────────────────────────────────────────────
# 3. HEADERS (fallback cho direct HTTP — TM kiểm tra UA)
# ────────────────────────────────────────────────────────────
TM_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ────────────────────────────────────────────────────────────
# 4. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"

# ────────────────────────────────────────────────────────────
# 5. OUTPUT BASE
# ────────────────────────────────────────────────────────────
OUTPUT_BASE: Path = Path(__file__).resolve().parent.parent / "output"


# ────────────────────────────────────────────────────────────
# 6. DYNAMIC LEAGUE CONFIG
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class TMLeagueConfig:
    """Cấu hình Transfermarkt cho một giải đấu cụ thể."""

    league_id: str
    tm_comp_id: str
    tm_slug: str
    season: str
    league_url: str

    # Output
    output_dir: Path
    team_metadata_csv: str
    market_values_csv: str


def get_tm_config(league_id: str = "EPL") -> TMLeagueConfig:
    """
    Tạo TMLeagueConfig từ league_registry.

    Usage:
        cfg = get_tm_config("EPL")
        cfg = get_tm_config("LALIGA")

    Raises:
        KeyError: nếu league_id không tồn tại
        ValueError: nếu league không có TM comp_id
    """
    lg = get_league(league_id)
    if not lg.has_transfermarkt:
        raise ValueError(f"League '{league_id}' không có trên Transfermarkt (tm_comp_id=None)")

    prefix = lg.file_prefix()
    output_dir = OUTPUT_BASE / "transfermarkt" / prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    return TMLeagueConfig(
        league_id=lg.league_id,
        tm_comp_id=lg.tm_comp_id,
        tm_slug=lg.tm_slug,
        season=lg.tm_season,
        league_url=lg.tm_league_url,
        output_dir=output_dir,
        team_metadata_csv=f"dataset_{prefix}_team_metadata.csv",
        market_values_csv=f"dataset_{prefix}_market_values.csv",
    )
