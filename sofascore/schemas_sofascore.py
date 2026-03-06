# ============================================================
# schemas_sofascore.py – Pydantic v2 Models cho SofaScore Data
# ============================================================
"""
Models cho dữ liệu heatmap / touch map từ SofaScore API.

  1. HeatmapPoint      — Một điểm trên heatmap (x, y, value)
  2. PlayerHeatmap     — Tập hợp tất cả heatmap points cho 1 cầu thủ/trận
  3. PlayerAvgPosition — Vị trí trung bình của cầu thủ trên sân
  4. MatchEvent        — Thông tin trận đấu từ SofaScore

Tọa độ SofaScore:
  • x: 0–100 (trái → phải, hướng tấn công)
  • y: 0–100 (trên → dưới)
  • Khác Understat (0–1) → cần normalize nếu kết hợp
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

_BASE_CONFIG = {
    "populate_by_name": True,
    "str_strip_whitespace": True,
    "extra": "ignore",
}


# ────────────────────────────────────────────────────────────
# 1. HEATMAP POINT
# ────────────────────────────────────────────────────────────

class HeatmapPoint(BaseModel):
    """Một điểm trên heatmap — tọa độ (x, y) với cường độ."""
    model_config = _BASE_CONFIG

    x: float = Field(..., description="Tọa độ X (0–100)")
    y: float = Field(..., description="Tọa độ Y (0–100)")
    value: float = Field(default=1.0, description="Cường độ (trọng số)")


# ────────────────────────────────────────────────────────────
# 2. PLAYER HEATMAP
# ────────────────────────────────────────────────────────────

class PlayerHeatmap(BaseModel):
    """
    Heatmap data cho một cầu thủ trong một trận đấu.

    Từ SofaScore API: /event/{event_id}/heatmap/{player_id}
    """
    model_config = _BASE_CONFIG

    # ── Match context ──
    event_id: int = Field(..., description="SofaScore event ID")
    match_date: Optional[str] = Field(None, description="YYYY-MM-DD")
    home_team: Optional[str] = Field(None)
    away_team: Optional[str] = Field(None)
    score: Optional[str] = Field(None, description="VD: 2-1")

    # ── Player ──
    player_id: int = Field(..., description="SofaScore player ID")
    player_name: str = Field(...)
    team_name: Optional[str] = Field(None)
    position: Optional[str] = Field(None, description="VD: M, F, D, G")
    jersey_number: Optional[int] = Field(None)

    # ── Heatmap data ──
    heatmap_points: list[HeatmapPoint] = Field(
        default_factory=list,
        description="Danh sách điểm heatmap"
    )
    num_points: int = Field(default=0)

    # ── Derived summary ──
    avg_x: Optional[float] = Field(None, description="Vị trí X trung bình")
    avg_y: Optional[float] = Field(None, description="Vị trí Y trung bình")

    # ── League context ──
    league_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)


# ────────────────────────────────────────────────────────────
# 3. PLAYER AVERAGE POSITION
# ────────────────────────────────────────────────────────────

class PlayerAvgPosition(BaseModel):
    """
    Vị trí trung bình của cầu thủ trên sân trong 1 trận.

    Từ SofaScore event lineups API.
    """
    model_config = _BASE_CONFIG

    event_id: int
    match_date: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None

    player_id: int
    player_name: str
    team_name: Optional[str] = None
    position: Optional[str] = None
    jersey_number: Optional[int] = None

    avg_x: float = Field(..., description="Vị trí trung bình X (0–100)")
    avg_y: float = Field(..., description="Vị trí trung bình Y (0–100)")
    minutes_played: Optional[int] = None
    rating: Optional[float] = Field(None, description="SofaScore rating (0–10)")

    league_id: Optional[str] = None
    season: Optional[str] = None


# ────────────────────────────────────────────────────────────
# 4. MATCH EVENT
# ────────────────────────────────────────────────────────────

class MatchEvent(BaseModel):
    """Thông tin tóm tắt trận đấu từ SofaScore."""
    model_config = _BASE_CONFIG

    event_id: int = Field(..., description="SofaScore event ID")
    tournament_id: int = Field(...)
    season_id: int = Field(...)
    round_num: Optional[int] = Field(None, description="Vòng đấu")

    home_team: str
    home_team_id: int
    away_team: str
    away_team_id: int

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    status: Optional[str] = Field(None, description="finished, inprogress, notstarted")

    start_timestamp: Optional[int] = Field(None, description="Unix timestamp")
    match_date: Optional[str] = Field(None, description="YYYY-MM-DD")
    slug: Optional[str] = Field(None, description="URL slug")

    league_id: Optional[str] = None


# ────────────────────────────────────────────────────────────
# 5. PLAYER MATCH PASSING STATS
# ────────────────────────────────────────────────────────────

class PlayerMatchPassing(BaseModel):
    """
    Passing stats cho một cầu thủ trong một trận đấu.

    Từ SofaScore lineups API: /event/{event_id}/lineups
    → statistics dict chứa totalPass, accuratePass, keyPass, ...
    """
    model_config = _BASE_CONFIG

    # ── Match context ──
    event_id: int = Field(..., description="SofaScore event ID")
    match_date: Optional[str] = Field(None, description="YYYY-MM-DD")
    home_team: Optional[str] = Field(None)
    away_team: Optional[str] = Field(None)

    # ── Player ──
    player_id: int = Field(..., description="SofaScore player ID")
    player_name: str = Field(...)
    team_name: Optional[str] = Field(None)
    position: Optional[str] = Field(None, description="D, M, F, G")
    minutes_played: int = Field(default=0)

    # ── Total passes ──
    total_pass: int = Field(default=0)
    accurate_pass: int = Field(default=0)

    # ── Long balls ──
    total_long_balls: int = Field(default=0)
    accurate_long_balls: int = Field(default=0)

    # ── Crosses ──
    total_cross: int = Field(default=0)
    accurate_cross: int = Field(default=0)

    # ── Key passes ──
    key_pass: int = Field(default=0)

    # ── Pass distribution ──
    total_own_half_passes: int = Field(default=0)
    accurate_own_half_passes: int = Field(default=0)
    total_opp_half_passes: int = Field(default=0)
    accurate_opp_half_passes: int = Field(default=0)

    # ── Other ──
    touches: int = Field(default=0)
    expected_assists: Optional[float] = Field(None, description="xA")
    goal_assist: int = Field(default=0)
    possession_lost_ctrl: int = Field(default=0)

    # ── League context ──
    league_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)


# ────────────────────────────────────────────────────────────
# HELPER: Safe parse list
# ────────────────────────────────────────────────────────────
def safe_parse_list(
    model_class: type[BaseModel],
    raw_items: list[dict],
    *,
    context_label: str = "",
) -> list[BaseModel]:
    """Parse list[dict] thành Pydantic models, bỏ qua lỗi validation."""
    validated: list[BaseModel] = []
    for idx, item in enumerate(raw_items):
        try:
            validated.append(model_class.model_validate(item))
        except Exception as exc:
            logger.warning(
                "[%s] Bỏ qua item #%d – %s",
                context_label or model_class.__name__, idx, exc,
            )
    return validated
