# ============================================================
# queries.py — Sample Analytical Queries
# ============================================================
"""
Tập hợp các truy vấn phân tích mẫu.

Cách dùng:
    python db/queries.py                      # Chạy tất cả demo queries
    python db/queries.py --query top_scorers  # Chỉ chạy 1 query

Hoặc import trực tiếp:
    from db.queries import run_query, top_xg_players
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_engine  # noqa: E402  # used in run_query()
from sqlalchemy import text  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


# ────────────────────────────────────────────────────────────
# Generic query runner
# ────────────────────────────────────────────────────────────

def run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """
    Chạy SQL query → trả về DataFrame.

    Args:
        sql:    SQL string (có thể dùng %s placeholder).
        params: Tuple parameters.

    Returns:
        pd.DataFrame với kết quả.
    """
    engine = get_engine()
    # SQLAlchemy dùng :name params, nhưng %s cũng hoạt động qua text()
    # Chuyển %s → :p1, :p2,... để tránh warning
    named_sql = sql
    named_params: dict = {}
    for i, val in enumerate(params, 1):
        named_sql = named_sql.replace("%s", f":p{i}", 1)
        named_params[f"p{i}"] = val
    with engine.connect() as conn:
        df = pd.read_sql_query(text(named_sql), conn, params=named_params)
    return df


# ────────────────────────────────────────────────────────────
# QUERY 1: Top 10 cầu thủ tổng xG mùa này
# ────────────────────────────────────────────────────────────

TOP_XG_SQL = """
SELECT
    player,
    COUNT(*)                    AS shots,
    ROUND(SUM(xg)::numeric, 3)  AS total_xg,
    ROUND(AVG(xg)::numeric, 4)  AS avg_xg_per_shot,
    SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END) AS goals,
    ROUND((SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END)::numeric
           / NULLIF(SUM(xg)::numeric, 0)), 3)         AS xg_efficiency
FROM shots
WHERE league_id = %s
GROUP BY player
ORDER BY total_xg DESC
LIMIT 10
"""

def top_xg_players(league_id: str = "EPL") -> pd.DataFrame:
    """Top 10 cầu thủ có tổng xG cao nhất."""
    return run_query(TOP_XG_SQL, (league_id,))


# ────────────────────────────────────────────────────────────
# QUERY 2: Bảng xếp hạng hiện tại
# ────────────────────────────────────────────────────────────

STANDINGS_SQL = """
SELECT
    position,
    team_name,
    matches_played  AS mp,
    wins     AS w,
    draws    AS d,
    losses   AS l,
    goals_for    AS gf,
    goals_against AS ga,
    goal_difference AS gd,
    points,
    form_last5
FROM standings
WHERE league_id = %s
ORDER BY position
"""

def get_standings(league_id: str = "EPL") -> pd.DataFrame:
    """Bảng xếp hạng giải đấu."""
    return run_query(STANDINGS_SQL, (league_id,))


# ────────────────────────────────────────────────────────────
# QUERY 3: xG per match của từng đội (home + away tổng hợp)
# ────────────────────────────────────────────────────────────

TEAM_XG_SQL = """
SELECT
    team,
    COUNT(*)                         AS matches,
    ROUND(SUM(xg_for)::numeric, 2)   AS total_xg_for,
    ROUND(SUM(xg_against)::numeric, 2) AS total_xg_against,
    ROUND(AVG(xg_for)::numeric, 3)   AS avg_xg_for,
    ROUND(AVG(xg_against)::numeric, 3) AS avg_xg_against,
    ROUND((SUM(xg_for) - SUM(xg_against))::numeric, 2) AS xg_diff
FROM (
    -- Home games
    SELECT h_team AS team, h_xg AS xg_for, a_xg AS xg_against
    FROM match_stats WHERE league_id = %s
    UNION ALL
    -- Away games
    SELECT a_team AS team, a_xg AS xg_for, h_xg AS xg_against
    FROM match_stats WHERE league_id = %s
) sub
GROUP BY team
ORDER BY avg_xg_for DESC
"""

def team_xg_stats(league_id: str = "EPL") -> pd.DataFrame:
    """xG trung bình tạo ra và nhận vào cho mỗi đội."""
    return run_query(TEAM_XG_SQL, (league_id, league_id))


# ────────────────────────────────────────────────────────────
# QUERY 4: Top cầu thủ goals + assists mùa này (FBref)
# ────────────────────────────────────────────────────────────

TOP_CONTRIB_SQL = """
SELECT
    player_name,
    team_name,
    position,
    matches_played  AS mp,
    minutes,
    goals,
    assists,
    goals_assists   AS g_plus_a,
    goals_per90,
    assists_per90,
    shots_on_target_pct AS sot_pct
FROM player_season_stats
WHERE league_id = %s
  AND minutes > 100
ORDER BY goals_assists DESC, goals DESC
LIMIT 15
"""

def top_contributors(league_id: str = "EPL") -> pd.DataFrame:
    """Top 15 cầu thủ theo goals + assists (FBref season stats)."""
    return run_query(TOP_CONTRIB_SQL, (league_id,))


# ────────────────────────────────────────────────────────────
# QUERY 5: Shot locations – breakdown by situation
# ────────────────────────────────────────────────────────────

SHOT_BREAKDOWN_SQL = """
SELECT
    situation,
    COUNT(*)                     AS shots,
    SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END) AS goals,
    ROUND(SUM(xg)::numeric, 2)   AS total_xg,
    ROUND(AVG(xg)::numeric, 4)   AS avg_xg,
    ROUND(
        100.0 * SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 1
    ) AS conversion_pct
FROM shots
WHERE league_id = %s
GROUP BY situation
ORDER BY shots DESC
"""

def shot_situation_breakdown(league_id: str = "EPL") -> pd.DataFrame:
    """Phân loại cú sút theo tình huống: OpenPlay, SetPiece, Penalty..."""
    return run_query(SHOT_BREAKDOWN_SQL, (league_id,))


# ────────────────────────────────────────────────────────────
# QUERY 6: Đội có possession cao nhất
# ────────────────────────────────────────────────────────────

POSSESSION_SQL = """
SELECT
    s.team_name,
    s.possession,
    s.goals,
    s.assists,
    s.yellow_cards,
    s.red_cards,
    st.points,
    st.wins
FROM squad_stats s
LEFT JOIN standings st
    ON s.team_id = st.team_id AND s.league_id = st.league_id
WHERE s.league_id = %s
ORDER BY s.possession DESC
"""

def possession_vs_results(league_id: str = "EPL") -> pd.DataFrame:
    """Possession trung bình của từng đội + điểm trên bảng."""
    return run_query(POSSESSION_SQL, (league_id,))


# ────────────────────────────────────────────────────────────
# QUERY 7: xG overperformers / underperformers
# ────────────────────────────────────────────────────────────

OVERPERFORM_SQL = """
SELECT
    p.player,
    SUM(CASE WHEN s.result = 'Goal' THEN 1 ELSE 0 END)   AS actual_goals,
    ROUND(SUM(s.xg)::numeric, 2)                          AS total_xg,
    ROUND(
        SUM(CASE WHEN s.result = 'Goal' THEN 1 ELSE 0 END)
        - SUM(s.xg)::numeric,
        2
    )                                                     AS xg_diff,
    COUNT(*)                                              AS shots
FROM shots s
JOIN (
    SELECT DISTINCT player_id, player
    FROM player_match_stats
    WHERE league_id = %s
) p ON s.player_id = p.player_id
WHERE s.league_id = %s
  AND s.result NOT IN ('OwnGoal')
GROUP BY p.player
HAVING COUNT(*) >= 5   -- ít nhất 5 cú sút
ORDER BY xg_diff DESC
LIMIT 10
"""

def xg_overperformers(league_id: str = "EPL") -> pd.DataFrame:
    """Top 10 cầu thủ overperform xG (ghi nhiều hơn kỳ vọng)."""
    return run_query(OVERPERFORM_SQL, (league_id, league_id))


# ────────────────────────────────────────────────────────────
# REGISTRY: tên → hàm
# ────────────────────────────────────────────────────────────

QUERIES: dict[str, callable] = {
    "top_xg": top_xg_players,
    "standings": get_standings,
    "team_xg": team_xg_stats,
    "top_contrib": top_contributors,
    "shot_breakdown": shot_situation_breakdown,
    "possession": possession_vs_results,
    "overperformers": xg_overperformers,
}


# ────────────────────────────────────────────────────────────
# Demo runner
# ────────────────────────────────────────────────────────────

def _print_result(title: str, df: pd.DataFrame) -> None:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    if df.empty:
        print("  (không có dữ liệu)")
    else:
        print(df.to_string(index=False))


def run_all_demos(league_id: str = "EPL") -> None:
    """Chạy tất cả queries demo và in kết quả ra console."""
    demos = [
        ("📊 Bảng xếp hạng",      get_standings),
        ("⚽ Top 10 xG players",   top_xg_players),
        ("🔄 xG per team",         team_xg_stats),
        ("🎯 Top G+A season stats", top_contributors),
        ("📍 Shot breakdown",      shot_situation_breakdown),
        ("🏟 Possession vs Points", possession_vs_results),
        ("📈 xG Overperformers",   xg_overperformers),
    ]
    for title, fn in demos:
        try:
            df = fn(league_id)
            _print_result(f"{title} | {league_id}", df)
        except Exception as exc:
            print(f"\n[SKIP] {title}: {exc}")


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Run sample analytics queries")
    parser.add_argument("--league", default="EPL")
    parser.add_argument("--query", choices=list(QUERIES.keys()),
                        help="Chạy 1 query cụ thể (mặc định: tất cả)")
    args = parser.parse_args()

    league_id = args.league.upper()
    if args.query:
        fn = QUERIES[args.query]
        df = fn(league_id)
        _print_result(args.query, df)
    else:
        run_all_demos(league_id)


if __name__ == "__main__":
    cli()
