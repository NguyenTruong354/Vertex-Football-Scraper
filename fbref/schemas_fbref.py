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
# 5. PLAYER DEFENSIVE STATS
# ────────────────────────────────────────────────────────────
class PlayerDefensiveStats(BaseModel):
    """
    Thống kê phòng ngự cầu thủ trong 1 mùa giải.

    Từ stats_defense_{comp_id} trên squad page.

    data-stat attrs:
        player, nationality, position, age, minutes_90s,
        tackles, tackles_won, tackles_def_3rd, tackles_mid_3rd, tackles_att_3rd,
        challenge_tackles, challenges, challenge_tackles_pct,
        blocks, blocked_shots, blocked_passes,
        interceptions, tackles_interceptions,
        clearances, errors
    """
    model_config = _BASE_CONFIG

    # Identifiers
    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)
    nationality: Optional[str] = Field(None)
    position: Optional[str] = Field(None)
    age: Optional[str] = Field(None)

    # Playing time
    minutes_90s: Optional[float] = Field(None)

    # Tackles
    tackles: int = Field(0)
    tackles_won: int = Field(0)
    tackles_def_3rd: int = Field(0)
    tackles_mid_3rd: int = Field(0)
    tackles_att_3rd: int = Field(0)

    # Challenges (dribblers tackled)
    challenge_tackles: int = Field(0)
    challenges: int = Field(0)
    challenge_tackles_pct: Optional[float] = Field(None)

    # Blocks
    blocks: int = Field(0)
    blocked_shots: int = Field(0, alias="blocked_shots")
    blocked_passes: int = Field(0, alias="blocked_passes")

    # Interceptions & Clearances
    interceptions: int = Field(0)
    tackles_interceptions: int = Field(0, description="Tkl+Int")
    clearances: int = Field(0)
    errors: int = Field(0)

    # Pressures
    pressures: int = Field(0, alias="pressures")
    pressure_regains: int = Field(0, alias="pressure_regains")
    pressure_regain_pct: Optional[float] = Field(None, alias="pressure_regain_pct")
    pressures_def_3rd: int = Field(0)
    pressures_mid_3rd: int = Field(0)
    pressures_att_3rd: int = Field(0)

    @field_validator(
        "tackles", "tackles_won", "tackles_def_3rd", "tackles_mid_3rd",
        "tackles_att_3rd", "challenge_tackles", "challenges",
        "blocks", "blocked_shots", "blocked_passes",
        "interceptions", "tackles_interceptions", "clearances", "errors",
        "pressures", "pressure_regains",
        "pressures_def_3rd", "pressures_mid_3rd", "pressures_att_3rd",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator(
        "minutes_90s", "challenge_tackles_pct", "pressure_regain_pct",
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
# 6. PLAYER POSSESSION & CARRY STATS
# ────────────────────────────────────────────────────────────
class PlayerPossessionStats(BaseModel):
    """
    Thống kê cầm bóng, mang bóng, rê bóng của cầu thủ.

    Từ stats_possession_{comp_id} trên squad page.

    data-stat attrs:
        player, nationality, position, age, minutes_90s,
        touches, touches_def_pen_area, touches_def_3rd,
        touches_mid_3rd, touches_att_3rd, touches_att_pen_area,
        touches_live_ball,
        take_ons, take_ons_won, take_ons_won_pct, take_ons_tackled,
        carries, carries_distance, carries_progressive_distance,
        progressive_carries, carries_into_final_third, carries_into_penalty_area,
        miscontrols, dispossessed,
        passes_received, progressive_passes_received
    """
    model_config = _BASE_CONFIG

    # Identifiers
    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)
    nationality: Optional[str] = Field(None)
    position: Optional[str] = Field(None)
    age: Optional[str] = Field(None)

    # Playing time
    minutes_90s: Optional[float] = Field(None)

    # Touches
    touches: int = Field(0)
    touches_def_pen_area: int = Field(0)
    touches_def_3rd: int = Field(0)
    touches_mid_3rd: int = Field(0)
    touches_att_3rd: int = Field(0)
    touches_att_pen_area: int = Field(0)
    touches_live_ball: int = Field(0)

    # Take-ons (Dribbles)
    take_ons: int = Field(0)
    take_ons_won: int = Field(0)
    take_ons_won_pct: Optional[float] = Field(None)
    take_ons_tackled: int = Field(0)
    take_ons_tackled_pct: Optional[float] = Field(None)

    # Carries
    carries: int = Field(0)
    carries_distance: Optional[float] = Field(None, description="Total carrying distance (yards)")
    carries_progressive_distance: Optional[float] = Field(None, description="Progressive carrying distance")
    progressive_carries: int = Field(0)
    carries_into_final_third: int = Field(0)
    carries_into_penalty_area: int = Field(0)
    miscontrols: int = Field(0)
    dispossessed: int = Field(0)

    # Receiving
    passes_received: int = Field(0)
    progressive_passes_received: int = Field(0)

    @field_validator(
        "touches", "touches_def_pen_area", "touches_def_3rd",
        "touches_mid_3rd", "touches_att_3rd", "touches_att_pen_area",
        "touches_live_ball",
        "take_ons", "take_ons_won", "take_ons_tackled",
        "carries", "progressive_carries",
        "carries_into_final_third", "carries_into_penalty_area",
        "miscontrols", "dispossessed",
        "passes_received", "progressive_passes_received",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator(
        "minutes_90s", "take_ons_won_pct", "take_ons_tackled_pct",
        "carries_distance", "carries_progressive_distance",
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
# 7. PLAYER GK STATS
# ────────────────────────────────────────────────────────────
class PlayerGKStats(BaseModel):
    """
    Thống kê thủ môn trong mùa giải.

    Từ stats_keeper_{comp_id} trên squad page.

    data-stat attrs:
        player, nationality, position, age,
        gk_games, gk_games_starts, minutes_gk,
        gk_goals_against, gk_goals_against_per90,
        gk_shots_on_target_against, gk_saves, gk_save_pct,
        gk_wins, gk_ties, gk_losses,
        gk_clean_sheets, gk_clean_sheets_pct,
        gk_pens_att, gk_pens_allowed, gk_pens_saved, gk_pens_missed,
        gk_psxg, gk_psxg_per_shot_on_target,
        gk_passes_completed_launched, gk_passes_launched,
        gk_passes_pct_launched,
        gk_passes, gk_passes_throws, gk_pct_passes_launched,
        gk_passes_length_avg,
        gk_goal_kicks, gk_pct_goal_kicks_launched, gk_goal_kick_length_avg,
        gk_crosses_faced, gk_crosses_stopped, gk_crosses_stopped_pct,
        gk_def_actions_outside_pen_area, gk_avg_distance_def_actions
    """
    model_config = _BASE_CONFIG

    # Identifiers
    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    season: Optional[str] = Field(None)
    nationality: Optional[str] = Field(None)
    position: Optional[str] = Field(None)
    age: Optional[str] = Field(None)

    # Playing time
    gk_games: int = Field(0)
    gk_games_starts: int = Field(0)
    minutes_gk: int = Field(0)

    # Goals against & saves
    gk_goals_against: int = Field(0)
    gk_goals_against_per90: Optional[float] = Field(None)
    gk_shots_on_target_against: int = Field(0)
    gk_saves: int = Field(0)
    gk_save_pct: Optional[float] = Field(None)

    # Results
    gk_wins: int = Field(0)
    gk_ties: int = Field(0)
    gk_losses: int = Field(0)

    # Clean sheets
    gk_clean_sheets: int = Field(0)
    gk_clean_sheets_pct: Optional[float] = Field(None)

    # Penalty kicks
    gk_pens_att: int = Field(0)
    gk_pens_allowed: int = Field(0)
    gk_pens_saved: int = Field(0)
    gk_pens_missed: int = Field(0)

    # PSxG (Post-Shot Expected Goals)
    gk_psxg: Optional[float] = Field(None, description="Post-shot xG")
    gk_psxg_per_shot_on_target: Optional[float] = Field(None)

    # Distribution — launches
    gk_passes_completed_launched: int = Field(0)
    gk_passes_launched: int = Field(0)
    gk_passes_pct_launched: Optional[float] = Field(None)

    # Distribution — all passes
    gk_passes: int = Field(0)
    gk_passes_throws: int = Field(0)
    gk_pct_passes_launched: Optional[float] = Field(None)
    gk_passes_length_avg: Optional[float] = Field(None)

    # Goal kicks
    gk_goal_kicks: int = Field(0)
    gk_pct_goal_kicks_launched: Optional[float] = Field(None)
    gk_goal_kick_length_avg: Optional[float] = Field(None)

    # Crosses & sweeper
    gk_crosses_faced: int = Field(0)
    gk_crosses_stopped: int = Field(0)
    gk_crosses_stopped_pct: Optional[float] = Field(None)
    gk_def_actions_outside_pen_area: int = Field(0)
    gk_avg_distance_def_actions: Optional[float] = Field(None)

    @field_validator(
        "gk_games", "gk_games_starts", "minutes_gk",
        "gk_goals_against", "gk_shots_on_target_against", "gk_saves",
        "gk_wins", "gk_ties", "gk_losses", "gk_clean_sheets",
        "gk_pens_att", "gk_pens_allowed", "gk_pens_saved", "gk_pens_missed",
        "gk_passes_completed_launched", "gk_passes_launched",
        "gk_passes", "gk_passes_throws",
        "gk_goal_kicks", "gk_crosses_faced", "gk_crosses_stopped",
        "gk_def_actions_outside_pen_area",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator(
        "gk_goals_against_per90", "gk_save_pct", "gk_clean_sheets_pct",
        "gk_psxg", "gk_psxg_per_shot_on_target",
        "gk_passes_pct_launched", "gk_pct_passes_launched",
        "gk_passes_length_avg",
        "gk_pct_goal_kicks_launched", "gk_goal_kick_length_avg",
        "gk_crosses_stopped_pct", "gk_avg_distance_def_actions",
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
# 8. FIXTURE ROW — Lịch thi đấu
# ────────────────────────────────────────────────────────────
class FixtureRow(BaseModel):
    """
    Một trận trong lịch thi đấu giải đấu.

    Từ trang Scores and Fixtures:
      /en/comps/{comp_id}/schedule/{slug}-Scores-and-Fixtures

    data-stat attrs:
        gameweek, date, time, home_team, home_xg, score,
        away_xg, away_team, attendance, venue, referee,
        match_report
    """
    model_config = _BASE_CONFIG

    gameweek: Optional[str] = Field(None)
    date: Optional[str] = Field(None)
    start_time: Optional[str] = Field(None, alias="time")
    dayofweek: Optional[str] = Field(None)
    home_team: Optional[str] = Field(None)
    home_xg: Optional[float] = Field(None)
    score: Optional[str] = Field(None)
    away_xg: Optional[float] = Field(None)
    away_team: Optional[str] = Field(None)
    attendance: Optional[str] = Field(None)
    venue: Optional[str] = Field(None)
    referee: Optional[str] = Field(None)
    match_report_url: Optional[str] = Field(None, description="URL match report")

    # Derived
    home_team_id: Optional[str] = Field(None)
    away_team_id: Optional[str] = Field(None)
    match_id: Optional[str] = Field(None, description="FBref match ID from URL")

    @field_validator("home_xg", "away_xg", mode="before")
    @classmethod
    def coerce_float(cls, v: object) -> float | None:
        return _safe_float(v)


# ────────────────────────────────────────────────────────────
# 9. MATCH PASSING STATS — Dữ liệu chuyền bóng từ match report
# ────────────────────────────────────────────────────────────
class MatchPassingStats(BaseModel):
    """
    Thống kê chuyền bóng cầu thủ trong 1 trận đấu.

    Từ match report page passing table.

    data-stat attrs:
        player, nationality, age, minutes,
        passes_completed, passes, passes_pct,
        passes_total_distance, passes_progressive_distance,
        passes_short_completed, passes_short, passes_pct_short,
        passes_medium_completed, passes_medium, passes_pct_medium,
        passes_long_completed, passes_long, passes_pct_long,
        assists, xa, key_passes,
        passes_into_final_third, passes_into_penalty_area,
        crosses_into_penalty_area, progressive_passes
    """
    model_config = _BASE_CONFIG

    # Match context
    match_id: Optional[str] = Field(None)
    match_date: Optional[str] = Field(None)
    home_team: Optional[str] = Field(None)
    away_team: Optional[str] = Field(None)

    # Player identifiers
    player_name: str = Field(..., alias="player")
    player_id: Optional[str] = Field(None)
    team_name: Optional[str] = Field(None)
    team_id: Optional[str] = Field(None)
    nationality: Optional[str] = Field(None)
    age: Optional[str] = Field(None)
    minutes: int = Field(0)

    # Total passes
    passes_completed: int = Field(0)
    passes: int = Field(0, description="Total passes attempted")
    passes_pct: Optional[float] = Field(None)
    passes_total_distance: Optional[float] = Field(None)
    passes_progressive_distance: Optional[float] = Field(None)

    # Short passes
    passes_short_completed: int = Field(0)
    passes_short: int = Field(0)
    passes_pct_short: Optional[float] = Field(None)

    # Medium passes
    passes_medium_completed: int = Field(0)
    passes_medium: int = Field(0)
    passes_pct_medium: Optional[float] = Field(None)

    # Long passes
    passes_long_completed: int = Field(0)
    passes_long: int = Field(0)
    passes_pct_long: Optional[float] = Field(None)

    # Creativity
    assists: int = Field(0)
    xa: Optional[float] = Field(None, alias="xa", description="Expected Assists")
    key_passes: int = Field(0)

    # Progressive / dangerous passes
    passes_into_final_third: int = Field(0)
    passes_into_penalty_area: int = Field(0)
    crosses_into_penalty_area: int = Field(0)
    progressive_passes: int = Field(0)

    @field_validator(
        "minutes",
        "passes_completed", "passes",
        "passes_short_completed", "passes_short",
        "passes_medium_completed", "passes_medium",
        "passes_long_completed", "passes_long",
        "assists", "key_passes",
        "passes_into_final_third", "passes_into_penalty_area",
        "crosses_into_penalty_area", "progressive_passes",
        mode="before",
    )
    @classmethod
    def coerce_int(cls, v: object) -> int:
        return _safe_int(v)

    @field_validator(
        "passes_pct", "passes_total_distance", "passes_progressive_distance",
        "passes_pct_short", "passes_pct_medium", "passes_pct_long",
        "xa",
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
