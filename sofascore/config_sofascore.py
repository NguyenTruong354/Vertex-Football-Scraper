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

import logging
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Import league registry (nằm ở thư mục cha)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.league_registry import LeagueConfig, get_league  # noqa: E402

logger = logging.getLogger(__name__)

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
    "UEL": 677,
    "EREDIVISIE": 37,
    "LIGA_PORTUGAL": 238,
    "RFPL": 203,
}

# ────────────────────────────────────────────────────────────
# 5. SOFASCORE SEASON IDs — FALLBACK CACHE
# ────────────────────────────────────────────────────────────
# Dùng làm fallback nếu Dynamic Lookup (API) thất bại.
# Tự động cập nhật khi resolve_season_id() tra cứu thành công.
SS_SEASON_IDS_CACHE: dict[str, int] = {
    "EPL": 76986,
    "LALIGA": 77559,
    "BUNDESLIGA": 77333,
    "SERIEA": 76457,
    "LIGUE1": 77356,
    "UCL": 76953,
    "UEL": 76954,
    "EREDIVISIE": 61865,
    "LIGA_PORTUGAL": 61648,
    "RFPL": 62115,
}

# In-memory cache: (league_id, season_year) → season_id
# Tránh gọi API nhiều lần trong cùng process.
_resolved_cache: dict[tuple[str, str], int] = {}

# ────────────────────────────────────────────────────────────
# 6. LOGGING
# ────────────────────────────────────────────────────────────
LOG_LEVEL: str = "INFO"

# ────────────────────────────────────────────────────────────
# 7. OUTPUT BASE
# ────────────────────────────────────────────────────────────
OUTPUT_BASE: Path = Path(__file__).resolve().parent.parent / "output"


# ────────────────────────────────────────────────────────────
# 8. DYNAMIC SEASON LOOKUP
# ────────────────────────────────────────────────────────────

def resolve_season_id(
    tournament_id: int,
    season_year: str,
    league_id: str = "",
) -> int | None:
    """
    Tự động tra cứu SofaScore season_id từ API.

    Gọi API: /unique-tournament/{tournament_id}/seasons
    Duyệt danh sách seasons, tìm mục có name/year khớp với season_year.

    Args:
        tournament_id: SofaScore tournament ID (VD: 17 cho EPL)
        season_year:   Năm mùa giải (VD: "2025" → match "25/26")
        league_id:     League key cho caching (VD: "EPL")

    Returns:
        season_id (int) nếu tìm thấy, None nếu không.

    Matching logic:
        season_year="2025" sẽ match với:
          - year="25/26"
          - name chứa "25/26" hoặc "2025/2026" hoặc "2025-2026"

    Ví dụ:
        resolve_season_id(17, "2025", "EPL")  # → 76986
        resolve_season_id(35, "2024", "BUNDESLIGA")  # → 63516
    """
    cache_key = (league_id or str(tournament_id), season_year)

    # Check in-memory cache first
    if cache_key in _resolved_cache:
        return _resolved_cache[cache_key]

    try:
        from curl_cffi import requests as cf_requests

        url = f"{SS_API_BASE}/unique-tournament/{tournament_id}/seasons"
        resp = cf_requests.get(
            url,
            headers=SS_HEADERS,
            impersonate="chrome",
            timeout=15,
        )

        if resp.status_code != 200:
            logger.warning(
                "SofaScore seasons API returned %d for tournament=%d",
                resp.status_code, tournament_id,
            )
            return None

        data = resp.json()
        seasons = data.get("seasons", [])

        if not seasons:
            logger.warning("SofaScore trả về 0 seasons cho tournament=%d", tournament_id)
            return None

        # Build search patterns from season_year
        # "2025" → short suffix "25/26", full forms "2025/2026", "2025-2026"
        year_int = int(season_year)
        short_suffix = f"{year_int % 100}/{(year_int + 1) % 100}"  # "25/26"
        full_slash = f"{year_int}/{year_int + 1}"                   # "2025/2026"
        full_dash = f"{year_int}-{year_int + 1}"                    # "2025-2026"

        for s in seasons:
            s_year = str(s.get("year", ""))
            s_name = str(s.get("name", ""))

            # Match year field: "25/26"
            if s_year == short_suffix:
                sid = s["id"]
                _resolved_cache[cache_key] = sid
                if league_id:
                    SS_SEASON_IDS_CACHE[league_id] = sid
                logger.info(
                    "✓ SofaScore season resolved: %s %s → id=%d (matched year='%s')",
                    league_id, season_year, sid, s_year,
                )
                return sid

            # Match name field: contains "25/26" or "2025/2026" or "2025-2026"
            if short_suffix in s_name or full_slash in s_name or full_dash in s_name:
                sid = s["id"]
                _resolved_cache[cache_key] = sid
                if league_id:
                    SS_SEASON_IDS_CACHE[league_id] = sid
                logger.info(
                    "✓ SofaScore season resolved: %s %s → id=%d (matched name='%s')",
                    league_id, season_year, sid, s_name,
                )
                return sid

        # Nếu không match, log toàn bộ seasons để debug
        available = [(s.get("id"), s.get("year"), s.get("name")) for s in seasons[:5]]
        logger.warning(
            "SofaScore: không tìm thấy season '%s' cho tournament=%d. "
            "Available (top 5): %s",
            season_year, tournament_id, available,
        )
        return None

    except ImportError:
        logger.warning("curl_cffi chưa cài đặt — không thể dynamic lookup season_id")
        return None
    except Exception as exc:
        logger.warning("SofaScore season lookup failed: %s", exc)
        return None


# ────────────────────────────────────────────────────────────
# 9. DYNAMIC LEAGUE CONFIG
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
    match_advanced_csv: str


def get_ss_config(
    league_id: str = "EPL",
    *,
    season_override: str | None = None,
) -> SSLeagueConfig:
    """
    Tạo SSLeagueConfig từ league_registry + Dynamic Season Lookup.

    Luồng xử lý season_id:
      1. Gọi resolve_season_id() → tra API SofaScore
      2. Nếu API fail → fallback về SS_SEASON_IDS_CACHE (hardcoded)
      3. Nếu cả hai đều không có → raise ValueError

    Args:
        league_id:       League ID (VD: "EPL", "BUNDESLIGA")
        season_override: Ghi đè mùa giải (VD: "2024" để cào mùa 24/25).
                         Nếu None → dùng mùa mặc định từ league_registry.

    Usage:
        cfg = get_ss_config("EPL")              # Mùa hiện tại (auto-detect)
        cfg = get_ss_config("EPL", season_override="2024")  # Mùa cũ 24/25

    Raises:
        KeyError: nếu league_id không tồn tại
        ValueError: nếu league không có SofaScore mapping
    """
    lg = get_league(league_id)

    tournament_id = SS_TOURNAMENT_IDS.get(league_id)
    if not tournament_id:
        raise ValueError(
            f"League '{league_id}' không có SofaScore tournament mapping"
        )

    # Determine target season year
    target_season = season_override or lg.fbref_season_short  # e.g. "2025"

    # Step 1: Dynamic lookup via API
    season_id = resolve_season_id(tournament_id, target_season, league_id)

    # Step 2: Fallback to cache (only if no season_override — override
    #         implies user wants a specific season, cache may be wrong)
    if season_id is None and season_override is None:
        season_id = SS_SEASON_IDS_CACHE.get(league_id)
        if season_id:
            logger.info(
                "⚠ SofaScore: dùng fallback cache season_id=%d cho %s "
                "(API lookup thất bại)",
                season_id, league_id,
            )

    if not season_id:
        raise ValueError(
            f"League '{league_id}' không tìm được SofaScore season_id "
            f"cho mùa '{target_season}'. API lookup thất bại và "
            f"không có fallback cache."
        )

    prefix = lg.file_prefix()
    output_dir = OUTPUT_BASE / "sofascore" / prefix
    output_dir.mkdir(parents=True, exist_ok=True)

    return SSLeagueConfig(
        league_id=lg.league_id,
        tournament_id=tournament_id,
        season_id=season_id,
        season=target_season,
        output_dir=output_dir,
        heatmaps_csv=f"dataset_{prefix}_heatmaps.csv",
        player_positions_csv=f"dataset_{prefix}_player_avg_positions.csv",
        match_passing_csv=f"dataset_{prefix}_match_passing.csv",
        match_advanced_csv=f"dataset_{prefix}_match_advanced_stats.csv",
    )
