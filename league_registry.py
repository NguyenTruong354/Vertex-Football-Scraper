# ============================================================
# league_registry.py – Trung tâm quản lý giải đấu
# ============================================================
"""
Single source of truth cho tất cả giải đấu được hỗ trợ.

Khi thêm giải đấu mới, CHỈ CẦN thêm 1 entry vào ``LEAGUES`` dict.
Toàn bộ pipeline (Understat, FBref, Transfermarkt) sẽ tự động nhận diện.

Mỗi ``LeagueConfig`` chứa:
  • Thông tin chung: id, tên hiển thị, quốc gia
  • Understat mapping: understat_name (None nếu Understat không hỗ trợ)
  • FBref mapping:     fbref_comp_id + fbref_slug
  • Transfermarkt:     tm_comp_id + tm_slug
  • Season config:     default_season cho từng source

FBref comp_id reference:         https://fbref.com/en/comps/
Understat league names:          EPL, La_liga, Bundesliga, Serie_A, Ligue_1, RFPL
Transfermarkt comp_id reference: https://www.transfermarkt.com/wettbewerbe/europa

Ví dụ thêm giải mới:
──────────────────────────────────────────────────────────────
    "EREDIVISIE": LeagueConfig(
        league_id="EREDIVISIE",
        display_name="Eredivisie",
        country="Netherlands",
        understat_name=None,          # Understat không hỗ trợ
        fbref_comp_id=23,
        fbref_slug="Eredivisie",
        tm_comp_id="NL1",
        tm_slug="eredivisie",
    ),
──────────────────────────────────────────────────────────────
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class LeagueConfig:
    """Cấu hình cho một giải đấu."""

    # ── Identifiers ──
    league_id: str                        # Key duy nhất (VD: "EPL", "LALIGA")
    display_name: str                     # Tên hiển thị (VD: "Premier League")
    country: str                          # Quốc gia (VD: "England")

    # ── Understat ──
    understat_name: Optional[str] = None  # Tên dùng trong URL Understat
                                          # None = Understat không hỗ trợ giải này
    understat_season: str = "2025"        # Mùa giải mặc định (1 năm – e.g. "2025")

    # ── FBref ──
    fbref_comp_id: Optional[int] = None   # comp_id trong URL FBref
                                          # None = FBref không hỗ trợ giải này
    fbref_slug: Optional[str] = None      # URL slug (VD: "Premier-League")
    fbref_season: str = "2025-2026"       # Mùa giải mặc định (2 năm)
    fbref_season_short: str = "2025"      # Dạng ngắn

    # ── Transfermarkt ──
    tm_comp_id: Optional[str] = None      # Competition ID trên TM (VD: "GB1")
                                          # None = TM không hỗ trợ
    tm_slug: Optional[str] = None         # URL slug (VD: "premier-league")
    tm_season: str = "2025"               # Mùa giải (1 năm, dùng cho saison_id)

    # ── Meta ──
    priority: int = 1                     # Thứ tự ưu tiên (1 = cao nhất)
    active: bool = True                   # Có đang scrape không

    # ── Derived Properties ──

    @property
    def has_understat(self) -> bool:
        """Giải này có trên Understat không."""
        return self.understat_name is not None

    @property
    def has_fbref(self) -> bool:
        """Giải này có trên FBref không."""
        return self.fbref_comp_id is not None

    @property
    def has_transfermarkt(self) -> bool:
        """Giải này có trên Transfermarkt không."""
        return self.tm_comp_id is not None

    @property
    def tm_league_url(self) -> str:
        """URL trang tổng quan giải đấu trên Transfermarkt."""
        if not self.has_transfermarkt:
            raise ValueError(f"League {self.league_id} không có TM comp_id")
        return (
            f"https://www.transfermarkt.com/{self.tm_slug}"
            f"/startseite/wettbewerb/{self.tm_comp_id}"
            f"/saison_id/{self.tm_season}"
        )

    @property
    def fbref_league_url(self) -> str:
        """URL trang tổng quan giải đấu trên FBref."""
        if not self.has_fbref:
            raise ValueError(f"League {self.league_id} không có FBref comp_id")
        return f"https://fbref.com/en/comps/{self.fbref_comp_id}/{self.fbref_slug}-Stats"

    @property
    def fbref_table_suffix(self) -> str:
        """Suffix cho FBref table IDs (= comp_id)."""
        if not self.has_fbref:
            raise ValueError(f"League {self.league_id} không có FBref comp_id")
        return str(self.fbref_comp_id)

    def file_prefix(self) -> str:
        """Prefix cho tên file CSV (lowercase league_id)."""
        return self.league_id.lower()


# ════════════════════════════════════════════════════════════
# LEAGUE REGISTRY
# ════════════════════════════════════════════════════════════
# Thêm giải mới? → Thêm 1 entry ở đây, DONE.

LEAGUES: dict[str, LeagueConfig] = {

    # ── Top 5 Leagues (Understat + FBref) ──

    "EPL": LeagueConfig(
        league_id="EPL",
        display_name="Premier League",
        country="England",
        understat_name="EPL",
        fbref_comp_id=9,
        fbref_slug="Premier-League",
        tm_comp_id="GB1",
        tm_slug="premier-league",
        priority=1,
    ),

    "LALIGA": LeagueConfig(
        league_id="LALIGA",
        display_name="La Liga",
        country="Spain",
        understat_name="La_liga",
        fbref_comp_id=12,
        fbref_slug="La-Liga",
        tm_comp_id="ES1",
        tm_slug="laliga",
        priority=2,
    ),

    "BUNDESLIGA": LeagueConfig(
        league_id="BUNDESLIGA",
        display_name="Bundesliga",
        country="Germany",
        understat_name="Bundesliga",
        fbref_comp_id=20,
        fbref_slug="Bundesliga",
        tm_comp_id="L1",
        tm_slug="bundesliga",
        priority=3,
    ),

    "SERIEA": LeagueConfig(
        league_id="SERIEA",
        display_name="Serie A",
        country="Italy",
        understat_name="Serie_A",
        fbref_comp_id=11,
        fbref_slug="Serie-A",
        tm_comp_id="IT1",
        tm_slug="serie-a",
        priority=4,
    ),

    "LIGUE1": LeagueConfig(
        league_id="LIGUE1",
        display_name="Ligue 1",
        country="France",
        understat_name="Ligue_1",
        fbref_comp_id=13,
        fbref_slug="Ligue-1",
        tm_comp_id="FR1",
        tm_slug="ligue-1",
        priority=5,
    ),

    # ── Understat only ──

    "RFPL": LeagueConfig(
        league_id="RFPL",
        display_name="Russian Premier Liga",
        country="Russia",
        understat_name="RFPL",
        fbref_comp_id=None,
        fbref_slug=None,
        tm_comp_id="RU1",
        tm_slug="premier-liga",
        priority=10,
    ),

    # ── FBref only ──

    "UCL": LeagueConfig(
        league_id="UCL",
        display_name="Champions League",
        country="Europe",
        understat_name=None,
        fbref_comp_id=8,
        fbref_slug="Champions-League",
        tm_comp_id="CL",
        tm_slug="uefa-champions-league",
        priority=6,
    ),

    "EREDIVISIE": LeagueConfig(
        league_id="EREDIVISIE",
        display_name="Eredivisie",
        country="Netherlands",
        understat_name=None,
        fbref_comp_id=23,
        fbref_slug="Eredivisie",
        tm_comp_id="NL1",
        tm_slug="eredivisie",
        priority=8,
    ),

    "LIGA_PORTUGAL": LeagueConfig(
        league_id="LIGA_PORTUGAL",
        display_name="Primeira Liga",
        country="Portugal",
        understat_name=None,
        fbref_comp_id=32,
        fbref_slug="Primeira-Liga",
        tm_comp_id="PO1",
        tm_slug="liga-portugal",
        priority=9,
    ),
}


# ════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════

def get_league(league_id: str) -> LeagueConfig:
    """
    Lấy cấu hình giải đấu theo league_id.

    Raises:
        KeyError nếu league_id không tồn tại.
    """
    key = league_id.upper()
    if key not in LEAGUES:
        available = ", ".join(sorted(LEAGUES.keys()))
        raise KeyError(
            f"League '{league_id}' không tồn tại. "
            f"Các giải hỗ trợ: {available}"
        )
    return LEAGUES[key]


def get_understat_leagues() -> list[LeagueConfig]:
    """Trả về danh sách giải đấu có trên Understat (đã sort theo priority)."""
    return sorted(
        [lg for lg in LEAGUES.values() if lg.has_understat and lg.active],
        key=lambda x: x.priority,
    )


def get_fbref_leagues() -> list[LeagueConfig]:
    """Trả về danh sách giải đấu có trên FBref (đã sort theo priority)."""
    return sorted(
        [lg for lg in LEAGUES.values() if lg.has_fbref and lg.active],
        key=lambda x: x.priority,
    )


def get_transfermarkt_leagues() -> list[LeagueConfig]:
    """Trả về danh sách giải đấu có trên Transfermarkt (đã sort theo priority)."""
    return sorted(
        [lg for lg in LEAGUES.values() if lg.has_transfermarkt and lg.active],
        key=lambda x: x.priority,
    )


def get_all_active_leagues() -> list[LeagueConfig]:
    """Trả về tất cả giải đấu đang active (đã sort theo priority)."""
    return sorted(
        [lg for lg in LEAGUES.values() if lg.active],
        key=lambda x: x.priority,
    )


def list_leagues() -> str:
    """Trả về bảng tóm tắt tất cả giải đấu (text-based)."""
    lines = [
        f"{'ID':<16} {'Name':<25} {'Country':<15} {'Understat':<12} {'FBref':<8} {'TM':<6} {'Active'}",
        "─" * 90,
    ]
    for lg in sorted(LEAGUES.values(), key=lambda x: x.priority):
        lines.append(
            f"{lg.league_id:<16} {lg.display_name:<25} {lg.country:<15} "
            f"{'✓' if lg.has_understat else '✗':<12} "
            f"{'✓' if lg.has_fbref else '✗':<8} "
            f"{'✓' if lg.has_transfermarkt else '✗':<6} "
            f"{'✓' if lg.active else '✗'}"
        )
    return "\n".join(lines)


def resolve_understat_name(league_id: str) -> str:
    """
    Chuyển league_id thành tên Understat URL-safe.

    VD: "LALIGA" → "La_liga", "EPL" → "EPL"

    Raises:
        ValueError nếu giải không có trên Understat.
    """
    lg = get_league(league_id)
    if not lg.has_understat:
        raise ValueError(f"League '{league_id}' không có trên Understat.")
    return lg.understat_name  # type: ignore[return-value]
