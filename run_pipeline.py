# ============================================================
# run_pipeline.py — Orchestrator: Scrape + Load tự động
# ============================================================
"""
Chạy toàn bộ pipeline: Scrape 4 nguồn → CSV → PostgreSQL.

Usage:
    python run_pipeline.py                         # EPL, full
    python run_pipeline.py --league EPL LALIGA     # Nhiều league
    python run_pipeline.py --quick-test            # Test nhanh
    python run_pipeline.py --load-only             # Chỉ load CSV có sẵn
    python run_pipeline.py --scrape-only           # Chỉ scrape, không load DB
    python run_pipeline.py --skip-sofascore        # Bỏ qua SofaScore
    python run_pipeline.py --ss-match-limit 10     # Giới hạn SS 10 trận
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable

logger = logging.getLogger("pipeline")
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


# ────────────────────────────────────────────────────────────
# Helper: chạy subprocess và log output
# ────────────────────────────────────────────────────────────

def _run(cmd: list[str], cwd: Path, label: str) -> bool:
    """Chạy command, stream output, trả về True nếu thành công."""
    logger.info("▶ %s — %s", label, " ".join(cmd))
    t0 = time.perf_counter()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=False,
            text=True,
            timeout=4 * 3600,  # 4h max
        )
        elapsed = time.perf_counter() - t0
        if proc.returncode == 0:
            logger.info("✓ %s — %.1fs", label, elapsed)
            return True
        else:
            logger.error("✗ %s — exit code %d (%.1fs)", label, proc.returncode, elapsed)
            return False
    except subprocess.TimeoutExpired:
        logger.error("✗ %s — TIMEOUT sau 4h", label)
        return False
    except Exception as e:
        logger.error("✗ %s — %s", label, e)
        return False


# ────────────────────────────────────────────────────────────
# Scraper runners
# ────────────────────────────────────────────────────────────

def scrape_understat(league: str, *, limit: int = 0) -> bool:
    cmd = [PYTHON, "async_scraper.py", "--league", league]
    if limit > 0:
        cmd += ["--limit", str(limit)]
    return _run(cmd, ROOT / "understat", f"Understat [{league}]")


def scrape_fbref(league: str, *, limit: int = 0, match_limit: int = 0) -> bool:
    cmd = [PYTHON, "fbref_scraper.py", "--league", league]
    if limit > 0:
        cmd += ["--limit", str(limit)]
    if match_limit > 0:
        cmd += ["--match-limit", str(match_limit)]
    return _run(cmd, ROOT / "fbref", f"FBref [{league}]")


def scrape_sofascore(league: str, *, match_limit: int = 0) -> bool:
    cmd = [PYTHON, "sofascore_client.py", "--league", league]
    if match_limit > 0:
        cmd += ["--match-limit", str(match_limit)]
    else:
        cmd += ["--match-limit", "0"]
    return _run(cmd, ROOT / "sofascore", f"SofaScore [{league}]")


def scrape_transfermarkt(league: str, *, limit: int = 0) -> bool:
    cmd = [PYTHON, "tm_scraper.py", "--league", league]
    if limit > 0:
        cmd += ["--limit", str(limit)]
    return _run(cmd, ROOT / "transfermarkt", f"Transfermarkt [{league}]")


def load_db(league: str) -> bool:
    cmd = [PYTHON, "-m", "db.loader", "--league", league]
    return _run(cmd, ROOT, f"DB Load [{league}]")


def build_team_canonical(league: str) -> bool:
    cmd = [PYTHON, "tools/maintenance/build_team_canonical.py", "--league", league]
    return _run(cmd, ROOT, f"Team Canonical [{league}]")


def build_match_crossref(league: str) -> bool:
    cmd = [PYTHON, "tools/maintenance/build_match_crossref.py", "--league", league]
    return _run(cmd, ROOT, f"Match Crossref [{league}]")


def populate_tm_crossref(league: str) -> bool:
    cmd = [PYTHON, "tools/fill_tm_crossref.py"]
    # Currently fill_tm_crossref runs for all rows, but we pass league for logging context if needed later
    return _run(cmd, ROOT, f"TM Crossref [{league}]")


def refresh_materialized_views(league: str) -> bool:
    # Strict refresh order: helper → profiles → teams → downstream
    cmd = [
        PYTHON, "-c",
        "from db.config_db import get_connection; conn = get_connection(); "
        "cur = conn.cursor(); "
        "cur.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_tm_player_candidates;'); "
        "cur.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_profiles;'); "
        "cur.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_team_profiles;'); "
        "cur.execute('REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_complete_stats;'); "
        "conn.commit(); cur.close(); conn.close();"
    ]
    return _run(cmd, ROOT, f"Refresh MVs [{league}]")


# ────────────────────────────────────────────────────────────
# League support matrix
# ────────────────────────────────────────────────────────────

LEAGUE_SOURCES = {
    "EPL":           {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LALIGA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "BUNDESLIGA":    {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "SERIEA":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGUE1":        {"understat": True,  "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "RFPL":          {"understat": True,  "fbref": False, "sofascore": True,  "transfermarkt": True},
    "UCL":           {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "EREDIVISIE":    {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
    "LIGA_PORTUGAL": {"understat": False, "fbref": True,  "sofascore": True,  "transfermarkt": True},
}


# ────────────────────────────────────────────────────────────
# Main pipeline
# ────────────────────────────────────────────────────────────

def run_pipeline(args: argparse.Namespace) -> None:
    leagues = [l.upper() for l in args.league]
    results: dict[str, dict[str, str]] = {}

    t_global = time.perf_counter()

    for league in leagues:
        logger.info("=" * 60)
        logger.info("PIPELINE START — %s", league)
        logger.info("=" * 60)

        sources = LEAGUE_SOURCES.get(league, {})
        results[league] = {}

        # ── Quick-test overrides ─────────────────────────────
        us_limit = 2 if args.quick_test else 0
        fb_limit = 2 if args.quick_test else 0
        fb_match = 1 if args.quick_test else 0
        ss_match = 1 if args.quick_test else args.ss_match_limit
        tm_limit = 1 if args.quick_test else 0

        # ── Scrape phase ─────────────────────────────────────
        if not args.load_only:
            # Understat
            if not args.skip_understat and sources.get("understat"):
                ok = scrape_understat(league, limit=us_limit)
                results[league]["understat"] = "OK" if ok else "FAIL"
            elif not sources.get("understat"):
                logger.info("⏭ Understat — không hỗ trợ %s", league)
                results[league]["understat"] = "SKIP"

            # FBref
            if not args.skip_fbref and sources.get("fbref"):
                ok = scrape_fbref(league, limit=fb_limit, match_limit=fb_match)
                results[league]["fbref"] = "OK" if ok else "FAIL"
            elif not sources.get("fbref"):
                logger.info("⏭ FBref — không hỗ trợ %s", league)
                results[league]["fbref"] = "SKIP"

            # SofaScore
            if not args.skip_sofascore and sources.get("sofascore"):
                ok = scrape_sofascore(league, match_limit=ss_match)
                results[league]["sofascore"] = "OK" if ok else "FAIL"
            elif not sources.get("sofascore"):
                logger.info("⏭ SofaScore — không hỗ trợ %s", league)
                results[league]["sofascore"] = "SKIP"

            # Transfermarkt
            if not args.skip_transfermarkt and sources.get("transfermarkt"):
                ok = scrape_transfermarkt(league, limit=tm_limit)
                results[league]["transfermarkt"] = "OK" if ok else "FAIL"
            elif not sources.get("transfermarkt"):
                logger.info("⏭ Transfermarkt — không hỗ trợ %s", league)
                results[league]["transfermarkt"] = "SKIP"

        # ── DB load phase ────────────────────────────────────
        if not args.scrape_only:
            ok = load_db(league)
            results[league]["db_load"] = "OK" if ok else "FAIL"
            
            if ok:
                if not args.skip_crossref_build:
                    ok_tc = build_team_canonical(league)
                    results[league]["team_canonical"] = "OK" if ok_tc else "FAIL"

                    ok_mcr = build_match_crossref(league)
                    results[league]["match_crossref"] = "OK" if ok_mcr else "FAIL"

                # ── Post-load data processing ─────────────────────
                populate_tm_crossref(league)
                refresh_materialized_views(league)

    # ── Summary ──────────────────────────────────────────────
    elapsed = time.perf_counter() - t_global
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY — %.1fs total", elapsed)
    logger.info("=" * 60)

    any_fail = False
    for league, steps in results.items():
        parts = [f"{k}={v}" for k, v in steps.items()]
        status = "FAIL" if any(v == "FAIL" for v in steps.values()) else "OK"
        if status == "FAIL":
            any_fail = True
        logger.info("  %s: %s  [%s]", league, status, ", ".join(parts))

    logger.info("=" * 60)
    if any_fail:
        logger.warning("Có lỗi! Kiểm tra log phía trên.")
        sys.exit(1)
    else:
        logger.info("Tất cả thành công!")


# ────────────────────────────────────────────────────────────
# CLI
# ────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Vertex Football Scraper — Full Pipeline Orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ví dụ:
  python run_pipeline.py                              # EPL full
  python run_pipeline.py --league EPL LALIGA          # 2 league
  python run_pipeline.py --quick-test                 # Test nhanh
  python run_pipeline.py --load-only                  # Chỉ load DB
  python run_pipeline.py --skip-sofascore             # Bỏ SS
  python run_pipeline.py --ss-match-limit 10          # SS giới hạn 10 trận
        """,
    )
    parser.add_argument(
        "--league", nargs="+", default=["EPL"],
        help="League IDs (default: EPL). VD: EPL LALIGA BUNDESLIGA",
    )
    parser.add_argument(
        "--scrape-only", action="store_true",
        help="Chỉ scrape CSV, không load vào DB",
    )
    parser.add_argument(
        "--load-only", action="store_true",
        help="Chỉ load CSV có sẵn vào DB (không scrape)",
    )
    parser.add_argument(
        "--skip-understat", action="store_true",
        help="Bỏ qua Understat scraper",
    )
    parser.add_argument(
        "--skip-fbref", action="store_true",
        help="Bỏ qua FBref scraper",
    )
    parser.add_argument(
        "--skip-sofascore", action="store_true",
        help="Bỏ qua SofaScore scraper",
    )
    parser.add_argument(
        "--skip-transfermarkt", action="store_true",
        help="Bỏ qua Transfermarkt scraper",
    )
    parser.add_argument(
        "--ss-match-limit", type=int, default=0,
        help="Giới hạn số trận SofaScore (0 = tất cả, default: 0)",
    )
    parser.add_argument(
        "--quick-test", action="store_true",
        help="Test nhanh: limit 2 teams, 1 match mỗi scraper",
    )
    parser.add_argument(
        "--skip-crossref-build", action="store_true",
        help="Bỏ qua build team_canonical + match_crossref sau DB load",
    )

    args = parser.parse_args()

    if args.scrape_only and args.load_only:
        parser.error("Không thể dùng cả --scrape-only và --load-only")

    run_pipeline(args)


if __name__ == "__main__":
    main()
