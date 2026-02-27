# ============================================================
# schemas_fbref.py – Pydantic v2 Models cho FBref Data
# ============================================================
"""
Models cho dữ liệu thu thập từ FBref.

Covers Critical + Important items:
  1. StandingsRow          — Bảng xếp hạng giải đấu
  2. PlayerProfile         — Thông tin cầu thủ (tên, tuổi, quốc tịch, vị trí)
  3. PlayerSeasonStats     — Thống kê cầu thủ toàn mùa (goals, assists, mins…)
  4. SquadStats            — Thống kê tổng hợp đội bóng
  5. PlayerDefensiveStats  — Tackles, interceptions, blocks, pressures
  6. PlayerPossessionStats — Carries, take-ons, touches by zone
  7. PlayerGKStats         — PSxG, saves%, distribution
  8. FixtureRow            — Lịch thi đấu, kick-off, venue
  9. MatchPassingStats     — Pass data từ match reports

FBref data-stat attributes map trực tiếp vào field aliases.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

_BASE_CONFIG = {
    "populate_by_name": True,
    "str_strip_whitespace": True,
    "extra": "ignore",
}


def _safe_int(v: object) -> int:
    """Chuyển string thành int, loại bỏ dấu phẩy (1,234 → 1234)."""
    if v is None or v == "":
        return 0
    try:
        return int(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0


def _safe_float(v: object) -> float | None:
    """Chuyển string thành float, trả None nếu rỗng."""
    if v is None or v == "" or str(v).strip() == "":
        return None
    try:
        return float(str(v).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


# ────────────────────────────────────────────────────────────
# 1. STANDINGS ROW — Bảng xếp hạng
# ────────────────────────────────────────────────────────────
class StandingsRow(BaseModel):
    """
    Một dòng trong bảng xếp hạng EPL.

    FBref data-stat attributes:
        rank, team, games, wins, ties, losses,
        goals_for, goals_against, goal_diff, points,
        points_avg, last_5, attendance_per_g,
        top_team_scorers, top_keeper
    """
    model_config = _BASE_CONFIG

    position: int = Field(..., alias="rank")
    team_name: str = Field(..., alias="team")
    team_id: Optional[str] = Field(None, description="FBref team ID from link")
    team_url: Optional[str] = Field(None, description="FBref team URL")
    matches_played: int = Field(0, alias="games")
    wins: int = Field(0)
    draws: int = Field(0, alias="ties")
    losses: int = Field(0)
    goals_for: int = Field(0)
    goals_against: int = Field(0)
    goal_difference: str = Field("0", alias="goal_diff")
    points: int = Field(0)
    points_avg: Optional[float] = Field(None)
    form_last5: Optional[str] = Field(None, alias="last_5")
    attendance_per_g: Optional[str] = Field(None)
    top_scorer: Optional[str] = Field(None, alias="top_team_scorers")
    top_keeper: Optional[str] = Field(None)

    @field_validator(
        "position", "matches_played", "wins", "draws", "losses",
        "goals_for", "goals_against", "points",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator("points_avg", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        return _safe_float(v)


# ────────────────────────────────────────────────────────────
# 2. PLAYER PROFILE — Hồ sơ cầu thủ (từ squad page)
# ────────────────────────────────────────────────────────────
class PlayerProfile(BaseModel):
    """
    Thông tin cá nhân cầu thủ — extract từ squad page header row.

    Fields từ stats_standard_{comp_id} table:
        player, nationality, position, age
    + metadata từ link: player_id, player_url
    + team context: team_name, team_id
    """
    model_config = _BASE_CONFIG

    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None, description="FBref player ID")
    player_url: Optional[str] = Field(None)
    nationality: Optional[str] = Field(None)
    position: Optional[str] = Field(None)
    age: Optional[str] = Field(None, description="'YY-DDD' format from FBref")
    age_years: Optional[int] = Field(None, description="Tuổi tính bằng năm")

    # Team context (thêm từ scraper)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None, description="Mùa giải (VD: 2025-2026)")

    @field_validator("nationality", mode="before")
    @classmethod
    def clean_nationality(cls, v: object) -> str | None:
        """FBref format: 'es ESP' → 'ESP'."""
        if not v or str(v).strip() == "":
            return None
        parts = str(v).strip().split()
        return parts[-1] if parts else str(v).strip()

    @model_validator(mode="after")
    def extract_age_years(self) -> "PlayerProfile":
        """Parse '30-165' → 30 (years)."""
        if self.age and "-" in self.age:
            try:
                self.age_years = int(self.age.split("-")[0])
            except ValueError:
                pass
        return self


# ────────────────────────────────────────────────────────────
# 3. PLAYER SEASON STATS — Thống kê mùa giải
# ────────────────────────────────────────────────────────────
class PlayerSeasonStats(BaseModel):
    """
    Thống kê tổng hợp cầu thủ trong 1 mùa giải EPL.

    Gộp từ stats_standard + stats_shooting trên squad page.

    data-stat attrs (standard):
        player, nationality, position, age, games, games_starts,
        minutes, minutes_90s, goals, assists, goals_assists,
        goals_pens, pens_made, pens_att, cards_yellow, cards_red,
        goals_per90, assists_per90

    data-stat attrs (shooting):
        shots, shots_on_target, shots_on_target_pct,
        shots_per90, shots_on_target_per90
    """
    model_config = _BASE_CONFIG

    # Identifiers
    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None, description="Mùa giải (VD: 2025-2026)")
    nationality: Optional[str] = Field(None)
    position: Optional[str] = Field(None)
    age: Optional[str] = Field(None)

    # Playing time
    matches_played: int = Field(0, alias="games")
    starts: int = Field(0, alias="games_starts")
    minutes: int = Field(0)
    minutes_90s: Optional[float] = Field(None)

    # Attacking
    goals: int = Field(0)
    assists: int = Field(0)
    goals_assists: int = Field(0)
    goals_non_pen: int = Field(0, alias="goals_pens")
    pens_made: int = Field(0)
    pens_att: int = Field(0)

    # Shooting (merged from shooting table)
    shots: Optional[int] = Field(None)
    shots_on_target: Optional[int] = Field(None)
    shots_on_target_pct: Optional[float] = Field(None)

    # Per 90
    goals_per90: Optional[float] = Field(None)
    assists_per90: Optional[float] = Field(None)
    goals_assists_per90: Optional[float] = Field(None)

    # Discipline
    yellow_cards: int = Field(0, alias="cards_yellow")
    red_cards: int = Field(0, alias="cards_red")

    @field_validator(
        "matches_played", "starts", "minutes", "goals", "assists",
        "goals_assists", "goals_non_pen", "pens_made", "pens_att",
        "yellow_cards", "red_cards",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator("shots", "shots_on_target", mode="before")
    @classmethod
    def coerce_opt_int(cls, v: object) -> int | None:
        if v is None or str(v).strip() == "":
            return None
        return _safe_int(v)

    @field_validator(
        "minutes_90s", "shots_on_target_pct",
        "goals_per90", "assists_per90", "goals_assists_per90",
        mode="before",
    )
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        return _safe_float(v)

    @field_validator("nationality", mode="before")
    @classmethod
    def clean_nationality(cls, v: object) -> str | None:
        if not v or str(v).strip() == "":
            return None
        parts = str(v).strip().split()
        return parts[-1] if parts else str(v).strip()


# ────────────────────────────────────────────────────────────
# 4. SQUAD STATS — Thống kê đội bóng (từ league page)
# ────────────────────────────────────────────────────────────
class SquadStats(BaseModel):
    """
    Thống kê tổng hợp 1 đội bóng trong mùa giải.

    Từ league page table: stats_squads_standard_for
    data-stat: team, players_used, avg_age, possession, games,
               goals, assists, pens_made, pens_att,
               cards_yellow, cards_red, goals_per90, assists_per90
    """
    model_config = _BASE_CONFIG

    team_name: str = Field(..., alias="team")
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None, description="Mùa giải (VD: 2025-2026)")
    players_used: int = Field(0)
    avg_age: Optional[float] = Field(None)
    possession: Optional[float] = Field(None)
    matches_played: int = Field(0, alias="games")

    # Performance
    goals: int = Field(0)
    assists: int = Field(0)
    pens_made: int = Field(0)
    pens_att: int = Field(0)
    yellow_cards: int = Field(0, alias="cards_yellow")
    red_cards: int = Field(0, alias="cards_red")

    # Per 90
    goals_per90: Optional[float] = Field(None)
    assists_per90: Optional[float] = Field(None)

    @field_validator(
        "players_used", "matches_played", "goals", "assists",
        "pens_made", "pens_att", "yellow_cards", "red_cards",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator(
        "avg_age", "possession", "goals_per90", "assists_per90",
        mode="before",
    )
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        return _safe_float(v)


# ────────────────────────────────────────────────────────────
# HELPER: Safe parse list (tái sử dụng pattern từ schemas.py)
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
