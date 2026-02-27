#!/usr/bin/env python3
# ============================================================
# setup_db.py — One-shot PostgreSQL Setup + Load + Query
# ============================================================
"""
Khởi tạo toàn bộ PostgreSQL database từ đầu:
  1. Tạo database (nếu chưa có)
  2. Chạy schema.sql → tạo tất cả tables
  3. Load CSVs → upsert data
  4. Chạy sample queries → in kết quả

Usage:
    python db/setup_db.py                    # Setup + load EPL + demo queries
    python db/setup_db.py --league EPL       # Chỉ định league
    python db/setup_db.py --no-queries       # Bỏ qua demo queries
    python db/setup_db.py --schema-only      # Chỉ tạo tables (không load data)

Yêu cầu:
    PostgreSQL server đang chạy ở localhost.
    File .env đã điền đúng thông tin (copy từ .env.example).
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extensions

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import db.config_db as cfg  # noqa: E402
from db.loader import load_all, LOADERS  # noqa: E402
from db.queries import run_all_demos  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

SCHEMA_FILE = Path(__file__).parent / "schema.sql"


# ────────────────────────────────────────────────────────────
# 1. Tạo database nếu chưa có
# ────────────────────────────────────────────────────────────

def ensure_database() -> bool:
    """
    Kết nối tới postgres (maintenance DB) và tạo database nếu chưa có.
    Returns True nếu OK.
    """
    # Kết nối maintenance database (postgres)
    try:
        conn = psycopg2.connect(
            host=cfg.DB_HOST,
            port=cfg.DB_PORT,
            dbname="postgres",
            user=cfg.DB_USER,
            password=cfg.DB_PASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (cfg.DB_NAME,),
            )
            exists = cur.fetchone() is not None

        if not exists:
            with conn.cursor() as cur:
                cur.execute(
                    f'CREATE DATABASE "{cfg.DB_NAME}" '
                    f"ENCODING 'UTF8' LC_COLLATE 'en_US.UTF-8' LC_CTYPE 'en_US.UTF-8' "
                    f"TEMPLATE template0"
                )
            logger.info("✓ Đã tạo database: %s", cfg.DB_NAME)
        else:
            logger.info("✓ Database đã tồn tại: %s", cfg.DB_NAME)

        conn.close()
        return True

    except psycopg2.OperationalError as exc:
        logger.error("✗ Không kết nối được PostgreSQL: %s", exc)
        logger.error(
            "  Kiểm tra: server đang chạy? .env đúng?\n"
            "  host=%s port=%s user=%s",
            cfg.DB_HOST, cfg.DB_PORT, cfg.DB_USER,
        )
        return False


# ────────────────────────────────────────────────────────────
# 2. Chạy schema.sql
# ────────────────────────────────────────────────────────────

def run_schema() -> bool:
    """Đọc schema.sql và chạy trên DB target."""
    sql = SCHEMA_FILE.read_text(encoding="utf-8")
    try:
        conn = cfg.get_connection()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        conn.close()
        logger.info("✓ Schema applied: %s", SCHEMA_FILE.name)
        return True
    except Exception as exc:
        logger.error("✗ Schema error: %s", exc)
        return False


# ────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────

def main(
    league_id: str = "EPL",
    schema_only: bool = False,
    no_queries: bool = False,
    tables: list[str] | None = None,
) -> None:
    t_start = time.perf_counter()

    print("=" * 60)
    print("  VERTEX FOOTBALL — PostgreSQL Setup")
    print(f"  DB:     {cfg.DB_HOST}:{cfg.DB_PORT}/{cfg.DB_NAME}")
    print(f"  League: {league_id}")
    print("=" * 60)

    # ── Step 1: Create database ──
    print("\n[1/4] Kiểm tra database...")
    if not ensure_database():
        sys.exit(1)

    # ── Step 2: Apply schema ──
    print("\n[2/4] Apply schema.sql...")
    if not run_schema():
        sys.exit(1)

    if schema_only:
        print("\n✓ Schema only mode — done.")
        return

    # ── Step 3: Load data ──
    print(f"\n[3/4] Load CSVs → PostgreSQL (league={league_id})...")
    results = load_all(league_id=league_id, tables=tables)

    total = sum(results.values())
    print(f"\n  ✓ Đã load: {total} rows tổng")
    for name, count in results.items():
        status = "✓" if count > 0 else "─"
        print(f"    {status} {name:<28} {count:>6} rows")

    # ── Step 4: Demo queries ──
    if not no_queries and total > 0:
        print(f"\n[4/4] Demo queries (league={league_id})...")
        run_all_demos(league_id)

    elapsed = time.perf_counter() - t_start
    print(f"\n{'='*60}")
    print(f"  ✅ Hoàn thành trong {elapsed:.1f}s")
    print(f"{'='*60}")


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def cli() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Vertex Football — PostgreSQL Setup & Loader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python db/setup_db.py                         # Full setup EPL
  python db/setup_db.py --league EPL            # Chỉ định league
  python db/setup_db.py --schema-only           # Chỉ tạo tables
  python db/setup_db.py --no-queries            # Setup + load, không demo
  python db/setup_db.py --table shots standings # Chỉ load 2 bảng

Sau khi setup, có thể dùng:
  python db/queries.py                          # Demo analytics
  python db/queries.py --query standings        # 1 query
  python db/loader.py                           # Reload CSVs
        """,
    )
    parser.add_argument("--league", default="EPL", help="League ID (default: EPL)")
    parser.add_argument("--schema-only", action="store_true",
                        help="Chỉ tạo tables, không load data")
    parser.add_argument("--no-queries", action="store_true",
                        help="Bỏ qua demo queries sau khi load")
    parser.add_argument("--table", nargs="+", choices=list(LOADERS.keys()),
                        help="Chỉ load các bảng được chỉ định")
    args = parser.parse_args()

    main(
        league_id=args.league.upper(),
        schema_only=args.schema_only,
        no_queries=args.no_queries,
        tables=args.table,
    )


if __name__ == "__main__":
    cli()
