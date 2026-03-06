# ============================================================
# config_sofascore.py – Cấu hình cho SofaScore Heatmap Pipeline
# ============================================================
"""
SofaScore cung cấp heatmaps / touch maps thông qua JSON API.

Đặc điểm kỹ thuật:
  • SofaScore API trả về JSON → không cần browser
  • Rate-limit: ~2s giữa các request
  • API base: https://api.sofascore.com/api/v1/
  • Heatmap endpoint: /event/{event_id}/heatmap/{player_id}
  • Cần match SofaScore event_id với FBref match → dùng scheduled-events API

Endpoints chính:
  • Lịch thi đấu theo ngày: /sport/football/scheduled-events/{YYYY-MM-DD}
  • Tournament seasons:     /unique-tournament/{id}/seasons
  • Event lineup:           /event/{event_id}/lineups
  • Event heatmap:          /event/{event_id}/heatmap/{player_id}
  • Event shotmap:          /event/{event_id}/shotmap

SofaScore Tournament IDs:
  • EPL:        17
  • La Liga:    8
  • Bundesliga: 35
  • Serie A:    23
  • Ligue 1:    34
  • UCL:        7
  • Eredivisie: 37
  • Liga Portugal: 238
  • RFPL:       203
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
# 1. API BASE
# ────────────────────────────────────────────────────────────
SS_API_BASE: str = "https://api.sofascore.com/api/v1"
SS_WEB_BASE: str = "https://www.sofascore.com"

# ────────────────────────────────────────────────────────────
# 2. RATE LIMITING
# ────────────────────────────────────────────────────────────
SS_DELAY_BETWEEN_REQUESTS: float = 2.0
SS_MAX_RETRIES: int = 3
SS_TIMEOUT: int = 30

# ────────────────────────────────────────────────────────────
# 3. HTTP HEADERS
# ────────────────────────────────────────────────────────────
SS_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.sofascore.com/",
    "Origin": "https://www.sofascore.com",
}

# ────────────────────────────────────────────────────────────
# 4. SOFASCORE TOURNAMENT IDs
# ────────────────────────────────────────────────────────────
SS_TOURNAMENT_IDS: dict[str, int] = {
    "EPL": 17,
    "LALIGA": 8,
    "BUNDESLIGA": 35,
    "SERIEA": 23,
    "LIGUE1": 34,
    "UCL": 7,
    "EREDIVISIE": 37,
    "LIGA_PORTUGAL": 238,
    "RFPL": 203,
}

# ────────────────────────────────────────────────────────────
# 5. SOFASCORE SEASON IDs (2024-2025 season)
# ────────────────────────────────────────────────────────────
# These need to be updated each season.
# Retrieve from: /unique-tournament/{tournament_id}/seasons
SS_SEASON_IDS: dict[str, int] = {
    "EPL": 61627,
    "LALIGA": 61643,
    "BUNDESLIGA": 61634,
    "SERIEA": 61639,
    "LIGUE1": 61736,
    "UCL": 62927,
    "EREDIVISIE": 61865,
    "LIGA_PORTUGAL": 61648,
    "RFPL": 62115,
}

# ────────────────────────────────────────────────────────────
# 6. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"

# ────────────────────────────────────────────────────────────
# 7. OUTPUT BASE
# ────────────────────────────────────────────────────────────
OUTPUT_BASE: Path = Path(__file__).resolve().parent.parent / "output"


# ────────────────────────────────────────────────────────────
# 8. DYNAMIC LEAGUE CONFIG
# ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SSLeagueConfig:
    """Cấu hình SofaScore cho một giải đấu cụ thể."""

    league_id: str
    tournament_id: int
    season_id: int
    season: str

    # Output
    output_dir: Path
    heatmaps_csv: str
    player_positions_csv: str
    match_passing_csv: str


def get_ss_config(league_id: str = "EPL") -> SSLeagueConfig:
    """
    Tạo SSLeagueConfig từ league_registry + SofaScore IDs.

    Usage:
        cfg = get_ss_config("EPL")
        cfg = get_ss_config("LALIGA")

    Raises:
        KeyError: nếu league_id không tồn tại
        ValueError: nếu league không có SofaScore mapping
    """
    lg = get_league(league_id)

    tournament_id = SS_TOURNAMENT_IDS.get(league_id)
    season_id = SS_SEASON_IDS.get(league_id)

    if not tournament_id:
        raise ValueError(
            f"League '{league_id}' không có SofaScore tournament mapping"
        )
    if not season_id:
        raise ValueError(
            f"League '{league_id}' không có SofaScore season_id. "
            f"Cần update SS_SEASON_IDS trong config_sofascore.py"
        )

    prefix = lg.file_prefix()
    output_dir = OUTPUT_BASE / "sofascore" / prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    return SSLeagueConfig(
        league_id=lg.league_id,
        tournament_id=tournament_id,
        season_id=season_id,
        season=lg.fbref_season_short,  # Use same season reference
        output_dir=output_dir,
        heatmaps_csv=f"dataset_{prefix}_heatmaps.csv",
        player_positions_csv=f"dataset_{prefix}_player_avg_positions.csv",
        match_passing_csv=f"dataset_{prefix}_match_passing.csv",
    )
