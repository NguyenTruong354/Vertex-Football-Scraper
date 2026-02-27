# ============================================================
# schemas.py – Pydantic v2 Models cho Data Validation
# ============================================================
"""
Mỗi class kế thừa ``pydantic.BaseModel`` định nghĩa **schema** chặt chẽ
cho dữ liệu bóc tách từ Understat.

Tại sao dùng Pydantic?
  1. Tự động **ép kiểu** (coerce) – JSON trả string "0.34" → float 0.34
  2. **Validation** – nếu xG < 0 hoặc tọa độ > 1.0, Pydantic báo lỗi
  3. **Default values** – thiếu trường → gán None, pipeline không crash
  4. **Serialization** – .model_dump() → dict phẳng, sẵn sàng cho DataFrame

Quy ước tên trường:
  • snake_case cho Python
  • alias (nếu cần) khớp với key gốc trong JSON của Understat
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

# Base config cho tất cả models: ignore extra fields, strip whitespace
_BASE_CONFIG = {
    "populate_by_name": True,
    "str_strip_whitespace": True,
    "extra": "ignore",       # Bỏ qua fields thừa từ API thay vì raise error
}


# ────────────────────────────────────────────────────────────
# 1. SHOT DATA  (xG, Shot Map)
# ────────────────────────────────────────────────────────────
class ShotData(BaseModel):
    """
    Mô tả một pha dứt điểm (shot) trong trận đấu.

    Understat trả JSON dạng:
      {
        "id": "479352",
        "minute": "12",
        "result": "Goal",
        "X": "0.895",
        "Y": "0.432",
        "xG": "0.1245",
        "player": "Mohamed Salah",
        "h_a": "h",
        "player_id": "1250",
        "situation": "OpenPlay",
        "season": "2025",
        "shotType": "RightFoot",
        "match_id": "22301",
        "h_team": "Liverpool",
        "a_team": "Arsenal",
        "h_goals": "2",
        "a_goals": "1",
        "date": "2026-01-14 20:00:00",
        "player_assisted": "Trent Alexander-Arnold",
        "lastAction": "Pass"
      }

    Lưu ý: Tọa độ X, Y của Understat nằm trong khoảng [0, 1],
    biểu diễn % chiều dài / rộng sân.
    """

    # --- IDs ---
    id: int
    match_id: int
    player_id: int

    # --- Thông tin trận ---
    h_team: str = Field(..., description="Tên đội nhà")
    a_team: str = Field(..., description="Tên đội khách")
    h_goals: int = Field(0, description="Bàn thắng đội nhà")
    a_goals: int = Field(0, description="Bàn thắng đội khách")
    date: Optional[str] = None
    season: Optional[str] = None

    # --- Thông tin pha dứt điểm ---
    minute: int = Field(0, ge=0, description="Phút diễn ra shot")
    result: str = Field("Unknown", description="Kết quả: Goal / SavedShot / …")
    situation: str = Field("Unknown", description="Tình huống: OpenPlay / SetPiece / …")
    shot_type: Optional[str] = Field(None, alias="shotType", description="Chân / đầu")
    last_action: Optional[str] = Field(None, alias="lastAction")

    # --- Tọa độ & xG ---
    # X, Y: float trong khoảng [0, 1] (% chiều dài/rộng sân)
    # xG: float >= 0 (thường 0-1, nhưng penalty xG có thể cao hơn)
    x: float = Field(..., alias="X", ge=0.0, le=1.0, description="Tọa độ X (% chiều dài)")
    y: float = Field(..., alias="Y", ge=0.0, le=1.0, description="Tọa độ Y (% chiều rộng)")
    xg: float = Field(..., alias="xG", ge=0.0, description="Expected Goals")

    # --- Cầu thủ ---
    player: str = Field(..., description="Tên cầu thủ dứt điểm")
    h_a: str = Field("h", description="h = home, a = away")
    player_assisted: Optional[str] = Field(None, description="Cầu thủ kiến tạo")

    model_config = _BASE_CONFIG

    # ------ Validators ------

    @field_validator("minute", "h_goals", "a_goals", "id", "match_id", "player_id", mode="before")
    @classmethod
    def coerce_int(cls, v: object) -> int:
        """Understat trả số dưới dạng chuỗi → chuyển sang int."""
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @field_validator("x", "y", "xg", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float:
        """Chuyển chuỗi → float, gán 0.0 nếu không parse được."""
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0


# ────────────────────────────────────────────────────────────
# 2. MATCH INFO (thông tin tổng quan trận đấu)
# ────────────────────────────────────────────────────────────
class MatchInfo(BaseModel):
    """
    Metadata tổng hợp cho một trận đấu.
    """
    match_id: int
    h_team: str
    a_team: str
    h_goals: int = 0
    a_goals: int = 0
    h_xg: float = Field(0.0, description="Tổng xG đội nhà")
    a_xg: float = Field(0.0, description="Tổng xG đội khách")
    datetime_str: Optional[str] = Field(None, description="Thời gian thi đấu (ISO)")
    league: Optional[str] = Field(None, description="League ID (VD: EPL, LALIGA)")
    season: Optional[str] = None

    model_config = _BASE_CONFIG

    @field_validator("match_id", "h_goals", "a_goals", mode="before")
    @classmethod
    def coerce_int(cls, v: object) -> int:
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @field_validator("h_xg", "a_xg", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float:
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0


# ────────────────────────────────────────────────────────────
# 3. PLAYER MATCH STATS (Pass Networks / Performance)
# ────────────────────────────────────────────────────────────
class PlayerMatchStats(BaseModel):
    """
    Thống kê một cầu thủ trong trận đấu cụ thể.
    Dùng cho pass-network & xA.

    Dữ liệu gốc từ Understat "rostersData":
      {
        "id": "12345",
        "player_id": "1250",
        "goals": "1",
        "own_goals": "0",
        "shots": "3",
        "xG": "0.45",
        "xA": "0.12",
        "time": "90",
        "position": "FW S",
        "team_id": "89",
        "...": "..."
      }
    """

    id: Optional[int] = None
    match_id: Optional[int] = None
    player_id: int
    player: Optional[str] = None
    team_id: Optional[int] = None

    # Vị trí trên sân (ví dụ "FW S", "MC", "GK")
    position: Optional[str] = None

    # Thời gian thi đấu (phút)
    time: int = Field(0, ge=0)

    # Thống kê cơ bản
    goals: int = 0
    own_goals: int = 0
    shots: int = 0
    assists: int = 0
    key_passes: int = 0

    # Expected metrics
    xg: float = Field(0.0, alias="xG", ge=0.0)
    xa: float = Field(0.0, alias="xA", ge=0.0)
    xg_chain: float = Field(0.0, alias="xGChain", ge=0.0)
    xg_buildup: float = Field(0.0, alias="xGBuildup", ge=0.0)

    model_config = _BASE_CONFIG

    @field_validator(
        "id", "match_id", "player_id", "team_id",
        "time", "goals", "own_goals", "shots", "assists", "key_passes",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int | None:
        if v is None:
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @field_validator("xg", "xa", "xg_chain", "xg_buildup", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float:
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0


# ────────────────────────────────────────────────────────────
# 4. LEAGUE MATCH ENTRY (danh sách trận trong mùa giải)
# ────────────────────────────────────────────────────────────
class LeagueMatchEntry(BaseModel):
    """
    Một hàng trong danh sách trận đấu ở trang league overview.
    Dùng để thu thập toàn bộ match_id cho một mùa giải / vòng đấu.
    """

    id: int = Field(..., description="Understat match ID")
    is_result: bool = Field(True, alias="isResult")
    h_title: str = Field(..., alias="h", description="Tên viết tắt đội nhà")  # sẽ map qua nested
    a_title: str = Field(..., alias="a", description="Tên viết tắt đội khách")
    h_goals: int = Field(0, alias="goals_h")  # nested field → cần flattening trước
    a_goals: int = Field(0, alias="goals_a")
    h_xg: Optional[float] = Field(None, alias="xG_h", description="xG đội nhà (None nếu chưa đá)")
    a_xg: Optional[float] = Field(None, alias="xG_a", description="xG đội khách (None nếu chưa đá)")
    datetime_str: Optional[str] = Field(None, alias="datetime")
    forecast_w: Optional[float] = Field(None, alias="forecast_w")
    forecast_d: Optional[float] = Field(None, alias="forecast_d")
    forecast_l: Optional[float] = Field(None, alias="forecast_l")

    model_config = _BASE_CONFIG

    @field_validator("id", "h_goals", "a_goals", mode="before")
    @classmethod
    def coerce_int(cls, v: object) -> int:
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @field_validator("h_xg", "a_xg", "forecast_w", "forecast_d", "forecast_l", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        """Chuyển str → float. Giữ None cho trận chưa đá."""
        if v is None:
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


# ────────────────────────────────────────────────────────────
# HELPER: Safe parse với logging (dùng trong pipeline)
# ────────────────────────────────────────────────────────────
def safe_parse_list(
    model_class: type[BaseModel],
    raw_items: list[dict],
    *,
    context_label: str = "",
) -> list[BaseModel]:
    """
    Parse một danh sách dict thô thành list các Pydantic model.
    Nếu một item lỗi validation → log WARNING và bỏ qua, KHÔNG crash pipeline.

    Args:
        model_class: Pydantic model class (VD: ShotData)
        raw_items:   Danh sách dict thô từ JSON
        context_label: Nhãn để log (vd: "match_22301")

    Returns:
        Danh sách model instances đã validate thành công.
    """
    validated: list[BaseModel] = []
    for idx, item in enumerate(raw_items):
        try:
            validated.append(model_class.model_validate(item))
        except Exception as exc:
            logger.warning(
                "[%s] Bỏ qua item #%d – validation error: %s",
                context_label or model_class.__name__,
                idx,
                exc,
            )
    return validated
