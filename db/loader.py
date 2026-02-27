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
    # Try new-style output subdir first, then flat output/
    candidates = [
        ROOT / "output" / "understat" / prefix / f"dataset_{prefix}_{suffix}.csv",
        ROOT / "output" / "fbref" / prefix / f"dataset_{prefix}_{suffix}.csv",
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

    # Replace NaN với None cho psycopg2
    df = df.where(df.notna(), other=None)

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
# ────────────────────────────────────────────────────────────

def load_shots(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "xg")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "shots", df, ["id", "league_id"], league_id=league_id)
    logger.info("  shots: %d rows upserted ← %s", count, path.name)
    return count


def load_player_match_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "player_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "player_match_stats", df, ["id", "league_id"], league_id=league_id)
    logger.info("  player_match_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_match_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "match_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "match_stats", df, ["match_id", "league_id"], league_id=league_id)
    logger.info("  match_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_standings(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "standings")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "standings", df, ["team_id", "league_id"], league_id=league_id)
    logger.info("  standings: %d rows upserted ← %s", count, path.name)
    return count


def load_squad_rosters(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "squad_rosters")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "squad_rosters", df, ["player_id", "team_id", "league_id"], league_id=league_id)
    logger.info("  squad_rosters: %d rows upserted ← %s", count, path.name)
    return count


def load_squad_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "squad_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "squad_stats", df, ["team_id", "season", "league_id"], league_id=league_id)
    logger.info("  squad_stats: %d rows upserted ← %s", count, path.name)
    return count


def load_player_season_stats(conn, league_id: str = "EPL") -> int:
    path = _csv_path(league_id, "player_season_stats")
    if not path.exists():
        logger.warning("  File không tồn tại: %s", path)
        return 0
    df = pd.read_csv(path)
    count = _upsert(conn, "player_season_stats", df, ["player_id", "team_id", "season", "league_id"], league_id=league_id)
    logger.info("  player_season_stats: %d rows upserted ← %s", count, path.name)
    return count


# ────────────────────────────────────────────────────────────
# Load all tables
# ────────────────────────────────────────────────────────────

LOADERS: dict[str, callable] = {
    "shots": load_shots,
    "player_match_stats": load_player_match_stats,
    "match_stats": load_match_stats,
    "standings": load_standings,
    "squad_rosters": load_squad_rosters,
    "squad_stats": load_squad_stats,
    "player_season_stats": load_player_season_stats,
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
                        help="Chỉ load bảng cụ thể (mặc định: tất cả)")
    args = parser.parse_args()

    load_all(league_id=args.league.upper(), tables=args.table)


if __name__ == "__main__":
    cli()
