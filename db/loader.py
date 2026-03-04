# ============================================================
# loader.py — Load CSV files vào PostgreSQL
# ============================================================
"""
Đọc CSV files từ output/ → insert vào PostgreSQL.

Chiến lược upsert:
  Dùng ON CONFLICT DO UPDATE để an toàn khi load lại.

Usage:
    python db/loader.py               # Load tất cả CSVs (EPL mặc định)
    python db/loader.py --league EPL  # Chỉ định league
    python db/loader.py --table shots # Chỉ load 1 bảng
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras

# Project root
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_connection  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

# ────────────────────────────────────────────────────────────
# CSV → Table mapping
# ────────────────────────────────────────────────────────────

def _csv_path(league_id: str, suffix: str) -> Path:
    prefix = league_id.lower()
    # Try source-specific subdirs first, then flat output/
    candidates = [
        ROOT / "output" / "understat" / prefix / f"dataset_{prefix}_{suffix}.csv",
        ROOT / "output" / "fbref" / prefix / f"dataset_{prefix}_{suffix}.csv",
        ROOT / "output" / "sofascore" / prefix / f"dataset_{prefix}_{suffix}.csv",
        ROOT / "output" / "transfermarkt" / prefix / f"dataset_{prefix}_{suffix}.csv",
        ROOT / "output" / f"dataset_{prefix}_{suffix}.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    return ROOT / "output" / f"dataset_{prefix}_{suffix}.csv"


# ────────────────────────────────────────────────────────────
# Generic bulk upsert helper
# ────────────────────────────────────────────────────────────

def _upsert(
    conn,
    table: str,
    df: pd.DataFrame,
    conflict_cols: list[str],
    *,
    league_id: str = "EPL",
) -> int:
    """
    Bulk upsert DataFrame rows into a PostgreSQL table.

    Returns number of rows upserted.
    """
    if df.empty:
        return 0

    df = df.copy()
    df["league_id"] = league_id

    import numpy as np
    # Replace NaN với None cho psycopg2 (df.where không hoạt động trên float dtype)
    df = df.replace({np.nan: None})

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join(f'"{c}"' for c in cols)

    # ON CONFLICT DO UPDATE
    update_cols = [c for c in cols if c not in conflict_cols and c != "loaded_at"]
    update_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)

    sql = (
        f'INSERT INTO {table} ({col_names}) VALUES ({placeholders})\n'
        f'ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}'
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)

    conn.commit()
    return len(rows)


# ────────────────────────────────────────────────────────────
# Individual table loaders
# LOAD ORDER: phải load parent tables TRƯỚC child tables
#   match_stats  → parent cho shots, player_match_stats
#   standings    → parent cho squad_rosters, squad_stats, player_season_stats
# ────────────────────────────────────────────────────────────

def load_shots(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "xg")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "shots", df, ["id", "league_id"], league_id=league_id)
    logger.info("  shots: %d rows upserted ← %s", count, path.name)
    return count


def load_player_match_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "player_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "player_match_stats", df, ["id", "league_id"], league_id=league_id)
    logger.info("  player_match_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_match_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "match_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "match_stats", df, ["match_id", "league_id"], league_id=league_id)
    logger.info("  match_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_standings(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "standings")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    df["season"] = "2025-2026"
    count = _upsert(conn, "standings", df, ["team_id", "league_id", "season"], league_id=league_id)
    logger.info("  standings: %d rows upserted ← %s", count, path.name)
    return count


def load_squad_rosters(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "squad_rosters")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "squad_rosters", df, ["player_id", "team_id", "league_id"], league_id=league_id)
    logger.info("  squad_rosters: %d rows upserted ← %s", count, path.name)
    return count


def load_squad_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "squad_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "squad_stats", df, ["team_id", "season", "league_id"], league_id=league_id)
    logger.info("  squad_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_player_season_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "player_season_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "player_season_stats", df, ["player_id", "team_id", "season", "league_id"], league_id=league_id)
    logger.info("  player_season_stats: %d rows upserted ← %s", count, path.name)
    return count


# ────────────────────────────────────────────────────────────
# FBref — fixtures, gk_stats
# ────────────────────────────────────────────────────────────

def load_fixtures(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "fixtures")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    # Bỏ fixtures chưa có match_id (trận chưa diễn ra)
    df = df.dropna(subset=["match_id"])
    count = _upsert(conn, "fixtures", df, ["match_id", "league_id"], league_id=league_id)
    logger.info("  fixtures: %d rows upserted ← %s", count, path.name)
    return count


def load_gk_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "gk_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "gk_stats", df, ["player_id", "team_id", "league_id"], league_id=league_id)
    logger.info("  gk_stats: %d rows upserted ← %s", count, path.name)
    return count


# ────────────────────────────────────────────────────────────
# SofaScore — ss_events, player_avg_positions, heatmaps
# ────────────────────────────────────────────────────────────

def load_ss_events(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "ss_events")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "ss_events", df, ["event_id", "league_id"], league_id=league_id)
    logger.info("  ss_events: %d rows upserted ← %s", count, path.name)
    return count


def load_player_avg_positions(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "player_avg_positions")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "player_avg_positions", df, ["event_id", "player_id", "league_id"], league_id=league_id)
    logger.info("  player_avg_positions: %d rows upserted ← %s", count, path.name)
    return count


def load_heatmaps(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "heatmaps")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    df.rename(columns={"heatmap_points_json": "heatmap_points"}, inplace=True)
    count = _upsert(conn, "heatmaps", df, ["event_id", "player_id", "league_id"], league_id=league_id)
    logger.info("  heatmaps: %d rows upserted ← %s", count, path.name)
    return count


# ────────────────────────────────────────────────────────────
# Transfermarkt — team_metadata, market_values
# ────────────────────────────────────────────────────────────

def load_team_metadata(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "team_metadata")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "team_metadata", df, ["team_id", "league_id"], league_id=league_id)
    logger.info("  team_metadata: %d rows upserted ← %s", count, path.name)
    return count


def load_market_values(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "market_values")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path, on_bad_lines="skip")
    count = _upsert(conn, "market_values", df, ["player_id", "team_id", "league_id"], league_id=league_id)
    logger.info("  market_values: %d rows upserted ← %s", count, path.name)
    return count


# ────────────────────────────────────────────────────────────
# Cross-reference builder: Understat player_id ↔ FBref player_id
# ────────────────────────────────────────────────────────────

def rebuild_crossref(conn, league_id: str = "EPL") -> int:
    """
    Build bảng player_crossref bằng cách match tên cầu thủ giữa
    Understat (shots.player) và FBref (player_season_stats.player_name).

    Thuật toán:
      1. Lấy distinct (player_id, player) từ shots  → normalize tên
      2. Lấy (player_id, player_name) từ player_season_stats → normalize tên
      3. INNER JOIN trên tên đã normalize (lower + strip)
      4. Upsert vào player_crossref với matched_by = 'name_exact'

    Trả về số rows đã upsert.
    """
    # Bước 1: Distinct Understat players (BIGINT id, text name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT player_id, lower(trim(player)) AS norm_name
            FROM shots
            WHERE league_id = %s AND player_id IS NOT NULL AND player IS NOT NULL
            """,
            (league_id,),
        )
        understat_rows = cur.fetchall()  # [(player_id, norm_name), ...]

    if not understat_rows:
        logger.warning("  crossref: shots bảng rỗng hoặc không có data cho league %s", league_id)
        return 0

    # Bước 2: FBref players (TEXT id, text name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT player_id, lower(trim(player_name)) AS norm_name
            FROM player_season_stats
            WHERE league_id = %s AND player_id IS NOT NULL AND player_name IS NOT NULL
            """,
            (league_id,),
        )
        fbref_rows = cur.fetchall()  # [(player_id, norm_name), ...]

    if not fbref_rows:
        logger.warning("  crossref: player_season_stats bảng rỗng hoặc không có data cho league %s", league_id)
        return 0

    # Bước 3: In-memory name JOIN
    fbref_by_name: dict[str, str] = {norm: pid for pid, norm in fbref_rows}
    matched: list[tuple] = []
    for us_id, norm_name in understat_rows:
        fb_id = fbref_by_name.get(norm_name)
        if fb_id:
            matched.append((us_id, fb_id, norm_name, league_id, "name_exact"))

    if not matched:
        logger.warning("  crossref: không match được cầu thủ nào (tên không khớp)")
        return 0

    # Bước 4: Upsert vào player_crossref
    sql = """
        INSERT INTO player_crossref
            (understat_player_id, fbref_player_id, canonical_name, league_id, matched_by)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (understat_player_id, fbref_player_id, league_id)
        DO UPDATE SET
            canonical_name = EXCLUDED.canonical_name,
            matched_by     = EXCLUDED.matched_by,
            loaded_at      = NOW()
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, matched, page_size=500)
    conn.commit()

    logger.info("  crossref: %d players matched (Understat→FBref) ← name_exact", len(matched))
    return len(matched)


# ────────────────────────────────────────────────────────────
# Load all tables
# ────────────────────────────────────────────────────────────

# QUAN TRỌNG: thứ tự dict này = thứ tự load.
# Parent tables PHẢI đứng trước child tables để FK không bị lỗi.
LOADERS: dict[str, callable] = {
    # ── Nhóm A: Parent tables ─────────────────────────────
    "match_stats":           load_match_stats,           # parent: shots, player_match_stats
    "standings":             load_standings,              # parent: squad_*, player_season_stats
    # ── Nhóm B: Child tables (Understat) ─────────────────
    "shots":                 load_shots,
    "player_match_stats":    load_player_match_stats,
    # ── Nhóm C: Child tables (FBref) ─────────────────────
    "squad_rosters":         load_squad_rosters,
    "squad_stats":           load_squad_stats,
    "player_season_stats":   load_player_season_stats,
    "fixtures":              load_fixtures,
    "gk_stats":              load_gk_stats,
    # ── Nhóm D: SofaScore ────────────────────────────────
    "ss_events":             load_ss_events,
    "player_avg_positions":  load_player_avg_positions,
    "heatmaps":              load_heatmaps,
    # ── Nhóm E: Transfermarkt ────────────────────────────
    "team_metadata":         load_team_metadata,
    "market_values":         load_market_values,
    # ── Nhóm F: Cross-source mapping (build sau cùng) ────
    "crossref":              rebuild_crossref,
}


def load_all(league_id: str = "EPL", tables: list[str] | None = None) -> dict[str, int]:
    """
    Load tất cả (hoặc một số) tables vào PostgreSQL.

    Args:
        league_id: League ID (VD: "EPL").
        tables:    Danh sách tên bảng muốn load. None = tất cả.

    Returns:
        dict tên_bảng → số rows đã upsert.
    """
    selected = tables or list(LOADERS.keys())
    results: dict[str, int] = {}

    t_start = time.perf_counter()
    logger.info("=" * 50)
    logger.info("LOAD CSV → PostgreSQL | league=%s", league_id)
    logger.info("=" * 50)

    conn = get_connection()
    try:
        for name in selected:
            loader_fn = LOADERS.get(name)
            if not loader_fn:
                logger.warning("Unknown table: %s", name)
                continue
            results[name] = loader_fn(conn, league_id)
    finally:
        conn.close()

    elapsed = time.perf_counter() - t_start
    total_rows = sum(results.values())
    logger.info("─" * 50)
    logger.info("Hoàn thành: %d rows tổng | %.2fs", total_rows, elapsed)
    return results


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Load CSVs into PostgreSQL")
    parser.add_argument("--league", default="EPL")
    parser.add_argument("--table", nargs="+", choices=list(LOADERS.keys()),
                        help="Chỉ load bảng cụ thể (mặc định: tất cả). Thứ tự: match_stats → standings → shots → ...")
    args = parser.parse_args()

    load_all(league_id=args.league.upper(), tables=args.table)


if __name__ == "__main__":
    cli()
