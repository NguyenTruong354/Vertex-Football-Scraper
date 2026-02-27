# ============================================================
# schemas_tm.py – Pydantic v2 Models cho Transfermarkt Data
# ============================================================
"""
Models cho dữ liệu thu thập từ Transfermarkt.

  1. TeamMetadata   — Thông tin CLB (stadium, manager, formation, logo…)
  2. PlayerMarketValue — Giá trị chuyển nhượng cầu thủ

Transfermarkt data scrape từ HTML structure (không có data-stat).
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


def _safe_int(v: object) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(str(v).replace(",", "").replace(".", "").strip())
    except (ValueError, TypeError):
        return 0


def _safe_float(v: object) -> float | None:
    if v is None or v == "" or str(v).strip() == "":
        return None
    try:
        return float(str(v).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


# ────────────────────────────────────────────────────────────
# 1. TEAM METADATA
# ────────────────────────────────────────────────────────────
class TeamMetadata(BaseModel):
    """
    Metadata của một CLB từ Transfermarkt.

    Thu thập từ team overview page:
      /{team-slug}/startseite/verein/{team_id}/saison_id/{year}

    Bao gồm: tên, logo, sân, sức chứa, HLV, đội hình,
    phó chủ tịch, tổng giá trị đội hình…
    """
    model_config = _BASE_CONFIG

    # ── Identifiers ──
    team_name: str = Field(..., description="Tên CLB")
    team_id: str = Field(..., description="Transfermarkt team ID (number)")
    team_url: Optional[str] = Field(None, description="URL trang CLB")
    league_id: Optional[str] = Field(None, description="League ID từ registry")
    season: Optional[str] = Field(None, description="Mùa giải")

    # ── Visual ──
    logo_url: Optional[str] = Field(None, description="URL logo CLB")

    # ── Stadium ──
    stadium_name: Optional[str] = Field(None)
    stadium_capacity: Optional[int] = Field(None)
    stadium_url: Optional[str] = Field(None)

    # ── Staff ──
    manager_name: Optional[str] = Field(None, description="Tên HLV trưởng")
    manager_url: Optional[str] = Field(None)
    manager_since: Optional[str] = Field(None, description="Ngày bắt đầu HLV")
    manager_contract_until: Optional[str] = Field(None)

    # ── Squad info ──
    squad_size: Optional[int] = Field(None)
    avg_age: Optional[float] = Field(None)
    num_foreigners: Optional[int] = Field(None)
    total_market_value: Optional[str] = Field(None, description="VD: €1.23bn")

    # ── Formation / Tactics ──
    formation: Optional[str] = Field(None, description="VD: 4-3-3, 3-4-2-1")

    @field_validator("stadium_capacity", mode="before")
    @classmethod
    def coerce_capacity(cls, v: object) -> int | None:
        if v is None or str(v).strip() == "":
            return None
        # "60,704 Seats" → 60704
        cleaned = str(v).replace("Seats", "").replace("seats", "").strip()
        cleaned = cleaned.replace(",", "").replace(".", "")
        try:
            return int(cleaned)
        except (ValueError, TypeError):
            return None

    @field_validator("squad_size", "num_foreigners", mode="before")
    @classmethod
    def coerce_int(cls, v: object) -> int | None:
        if v is None or str(v).strip() == "":
            return None
        return _safe_int(v) or None

    @field_validator("avg_age", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        return _safe_float(v)


# ────────────────────────────────────────────────────────────
# 2. PLAYER MARKET VALUE
# ────────────────────────────────────────────────────────────
class PlayerMarketValue(BaseModel):
    """
    Giá trị chuyển nhượng cầu thủ từ Transfermarkt.

    Thu thập từ team kader page:
      /{team-slug}/kader/verein/{team_id}/saison_id/{year}/plus/1

    Bao gồm: tên, vị trí, tuổi, quốc tịch, giá trị, ngày gia nhập…
    """
    model_config = _BASE_CONFIG

    # ── Identifiers ──
    player_name: str = Field(...)
    player_id: Optional[str] = Field(None, description="Transfermarkt player ID")
    player_url: Optional[str] = Field(None)
    player_image_url: Optional[str] = Field(None, description="URL ảnh cầu thủ")

    # ── Team context ──
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    league_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)

    # ── Profile ──
    position: Optional[str] = Field(None, description="VD: Centre-Forward, Goalkeeper")
    shirt_number: Optional[int] = Field(None)
    date_of_birth: Optional[str] = Field(None, description="YYYY-MM-DD hoặc Mon DD, YYYY")
    age: Optional[int] = Field(None)
    nationality: Optional[str] = Field(None, description="Quốc tịch chính")
    second_nationality: Optional[str] = Field(None)
    height_cm: Optional[int] = Field(None, description="Chiều cao (cm)")
    foot: Optional[str] = Field(None, description="Chân thuận: left/right/both")

    # ── Contract ──
    joined: Optional[str] = Field(None, description="Ngày gia nhập CLB")
    contract_until: Optional[str] = Field(None)

    # ── Market Value ──
    market_value: Optional[str] = Field(None, description="VD: €65.00m, €800k")
    market_value_numeric: Optional[float] = Field(None, description="Giá trị quy ra EUR")

    @field_validator("shirt_number", "age", "height_cm", mode="before")
    @classmethod
    def coerce_int(cls, v: object) -> int | None:
        if v is None or str(v).strip() == "":
            return None
        return _safe_int(v) or None

    @field_validator("market_value_numeric", mode="before")
    @classmethod
    def coerce_market_value(cls, v: object) -> float | None:
        return _safe_float(v)


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
                "[%s] Bỏ qua item #%d – %s", context_label or model_class.__name__, idx, exc,
            )
    return validated
