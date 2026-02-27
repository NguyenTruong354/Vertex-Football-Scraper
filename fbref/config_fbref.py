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
  • League overview: /en/comps/{comp_id}/{slug}-Stats
  • Squad page:      /en/squads/{team_id}/{team_slug}
  • Match report:    /en/matches/{match_id}/{match_slug}

Refactored: Tất cả league-specific config giờ được tạo động
từ league_registry.py.  Thêm giải mới → chỉ cần thêm vào registry.
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
FBREF_BASE: str = "https://fbref.com"

# ────────────────────────────────────────────────────────────
# 2. RATE LIMITING
# ────────────────────────────────────────────────────────────
FBREF_DELAY_BETWEEN_PAGES: float = 5.0
FBREF_CF_WAIT_MAX: int = 45
FBREF_TABLE_LOAD_WAIT: float = 2.0

# ────────────────────────────────────────────────────────────
# 3. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"

# ────────────────────────────────────────────────────────────
# 4. OUTPUT BASE
# ────────────────────────────────────────────────────────────
OUTPUT_BASE: Path = Path(__file__).resolve().parent.parent / "output"


# ────────────────────────────────────────────────────────────
# 5. DYNAMIC LEAGUE CONFIG
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FBrefLeagueConfig:
    """Cấu hình FBref cho một giải đấu cụ thể — tạo từ LeagueConfig."""

    league_id: str
    comp_id: int
    league_url: str
    season: str
    season_short: str

    # Table IDs (dynamic theo comp_id)
    squad_standard_table_id: str
    squad_shooting_table_id: str
    squad_keeper_table_id: str
    squad_playing_time_table_id: str
    squad_misc_table_id: str
    squad_defense_table_id: str
    squad_possession_table_id: str
    squad_gk_table_id: str        # alias cho keeper (player-level)

    # Fixture schedule
    fixture_url: str
    fixture_table_id: str

    # Output
    output_dir: Path
    standings_csv: str
    squad_roster_csv: str
    player_season_stats_csv: str
    squad_stats_csv: str
    defensive_stats_csv: str
    possession_stats_csv: str
    gk_stats_csv: str
    fixture_csv: str
    match_passing_csv: str


def get_fbref_config(league_id: str = "EPL") -> FBrefLeagueConfig:
    """
    Tạo FBrefLeagueConfig từ league_registry.

    Usage:
        cfg = get_fbref_config("EPL")
        cfg = get_fbref_config("LALIGA")

    Raises:
        KeyError: nếu league_id không tồn tại
        ValueError: nếu league không có FBref comp_id
    """
    lg = get_league(league_id)
    if not lg.has_fbref:
        raise ValueError(f"League '{league_id}' không có trên FBref (comp_id=None)")

    comp_id = lg.fbref_comp_id
    prefix = lg.file_prefix()

    output_dir = OUTPUT_BASE / "fbref" / prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    return FBrefLeagueConfig(
        league_id=lg.league_id,
        comp_id=comp_id,
        league_url=lg.fbref_league_url,
        season=lg.fbref_season,
        season_short=lg.fbref_season_short,
        # Table IDs
        squad_standard_table_id=f"stats_standard_{comp_id}",
        squad_shooting_table_id=f"stats_shooting_{comp_id}",
        squad_keeper_table_id=f"stats_keeper_{comp_id}",
        squad_playing_time_table_id=f"stats_playing_time_{comp_id}",
        squad_misc_table_id=f"stats_misc_{comp_id}",
        squad_defense_table_id=f"stats_defense_{comp_id}",
        squad_possession_table_id=f"stats_possession_{comp_id}",
        squad_gk_table_id=f"stats_keeper_{comp_id}",
        # Fixture
        fixture_url=f"https://fbref.com/en/comps/{comp_id}/schedule/{lg.fbref_slug}-Scores-and-Fixtures",
        fixture_table_id=f"sched_{lg.fbref_season}_{comp_id}_1",
        # Output
        output_dir=output_dir,
        standings_csv=f"dataset_{prefix}_standings.csv",
        squad_roster_csv=f"dataset_{prefix}_squad_rosters.csv",
        player_season_stats_csv=f"dataset_{prefix}_player_season_stats.csv",
        squad_stats_csv=f"dataset_{prefix}_squad_stats.csv",
        defensive_stats_csv=f"dataset_{prefix}_defensive_stats.csv",
        possession_stats_csv=f"dataset_{prefix}_possession_stats.csv",
        gk_stats_csv=f"dataset_{prefix}_gk_stats.csv",
        fixture_csv=f"dataset_{prefix}_fixtures.csv",
        match_passing_csv=f"dataset_{prefix}_match_passing.csv",
    )


# ────────────────────────────────────────────────────────────
# 6. LEGACY ALIASES (backward compatibility)
# ────────────────────────────────────────────────────────────
# Giữ lại biến cũ để code gọi trực tiếp cfg.FBREF_LEAGUE_URL
# không bị lỗi.  Sẽ dần chuyển sang dùng FBrefLeagueConfig.

_DEFAULT = get_fbref_config("EPL")

FBREF_LEAGUE_URL: str = _DEFAULT.league_url
FBREF_SEASON: str = _DEFAULT.season
FBREF_SEASON_SHORT: str = _DEFAULT.season_short

SQUAD_STANDARD_TABLE_ID: str = _DEFAULT.squad_standard_table_id
SQUAD_SHOOTING_TABLE_ID: str = _DEFAULT.squad_shooting_table_id
SQUAD_KEEPER_TABLE_ID: str = _DEFAULT.squad_keeper_table_id
SQUAD_PLAYING_TIME_TABLE_ID: str = _DEFAULT.squad_playing_time_table_id
SQUAD_MISC_TABLE_ID: str = _DEFAULT.squad_misc_table_id
SQUAD_DEFENSE_TABLE_ID: str = _DEFAULT.squad_defense_table_id
SQUAD_POSSESSION_TABLE_ID: str = _DEFAULT.squad_possession_table_id

OUTPUT_DIR: Path = _DEFAULT.output_dir
STANDINGS_CSV: str = _DEFAULT.standings_csv
SQUAD_ROSTER_CSV: str = _DEFAULT.squad_roster_csv
PLAYER_SEASON_STATS_CSV: str = _DEFAULT.player_season_stats_csv
SQUAD_STATS_CSV: str = _DEFAULT.squad_stats_csv
DEFENSIVE_STATS_CSV: str = _DEFAULT.defensive_stats_csv
POSSESSION_STATS_CSV: str = _DEFAULT.possession_stats_csv
GK_STATS_CSV: str = _DEFAULT.gk_stats_csv
FIXTURE_CSV: str = _DEFAULT.fixture_csv
MATCH_PASSING_CSV: str = _DEFAULT.match_passing_csv
