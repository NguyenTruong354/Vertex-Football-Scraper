"""
build_match_crossref.py — Build match_crossref from source tables + team_canonical.

Phase B of Tầng 2 foundation.
Reads from: fixtures, match_stats, ss_events, team_canonical.
Writes to: match_crossref.

IMPORTANT — Idempotency & Conflict Target:
  Upsert MUST target the correct source-specific unique partial index, NOT the
  surrogate PK (crossref_id). Each source row is inserted/updated via its own
  partial unique index:
    - ON CONFLICT (league_id, fbref_match_id) WHERE fbref_match_id IS NOT NULL
    - ON CONFLICT (league_id, understat_match_id) WHERE understat_match_id IS NOT NULL
    - ON CONFLICT (league_id, sofascore_event_id) WHERE sofascore_event_id IS NOT NULL
  This prevents duplicate rows when a second pass enriches a row that was
  created with NULL source IDs in a previous pass.

IMPORTANT — Timestamps treated as UTC:
  All datetime_str values from Understat match_stats are treated as UTC.
  When date() is extracted, the UTC date is used. Matches played late evening
  in non-UTC timezones (e.g., 23:00 GMT+7 = 16:00 UTC previous day) will be
  handled by Rule B (+/- 1 day window) with confidence 0.9.

Usage:
    python tools/maintenance/build_match_crossref.py              # EPL only
    python tools/maintenance/build_match_crossref.py --league EPL
    python tools/maintenance/build_match_crossref.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_connection  # noqa: E402
from tools.maintenance.build_team_canonical import normalize_team_name  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)


# ── Resolve team name to fbref_team_id ───────────────────────

def _build_team_lookup(cur, league_id: str) -> dict[str, str]:
    """Build normalized_name → fbref_team_id lookup from team_canonical.

    Returns dict mapping multiple normalized names (fbref, understat,
    sofascore, tm) to the same fbref_team_id.
    """
    cur.execute("""
        SELECT fbref_team_id, canonical_name, fbref_name,
               understat_name, sofascore_name, tm_name
        FROM team_canonical
        WHERE league_id = %s AND is_active = TRUE
    """, (league_id,))

    lookup: dict[str, str] = {}
    for row in cur.fetchall():
        fid = row[0]
        for name in row[1:]:
            if name:
                norm = normalize_team_name(name)
                if norm:
                    lookup[norm] = fid
    return lookup


def _resolve_team(name: str, lookup: dict[str, str]) -> str | None:
    """Resolve a team name to fbref_team_id via normalized lookup."""
    if not name:
        return None
    norm = normalize_team_name(name)
    return lookup.get(norm)


# ── Fetch match data from each source ────────────────────────

def _fetch_fbref_matches(cur, league_id: str) -> list[dict]:
    """Fetch fixtures with match_id and team IDs (already fbref_team_id)."""
    cur.execute("""
        SELECT match_id, date, home_team_id, away_team_id
        FROM fixtures
        WHERE league_id = %s AND match_id IS NOT NULL
    """, (league_id,))
    results = []
    for r in cur.fetchall():
        d = r[1]
        if d:
            try:
                d = date.fromisoformat(str(d)[:10])
            except ValueError:
                continue
        else:
            continue
        results.append({
            "fbref_match_id": r[0],
            "match_date": d,
            "home_fbref_team_id": r[2],
            "away_fbref_team_id": r[3],
        })
    return results


def _fetch_understat_matches(cur, league_id: str, team_lookup: dict[str, str]) -> list[dict]:
    """Fetch match_stats with resolved team IDs.

    All timestamps treated as UTC — date() extracts UTC date.
    """
    cur.execute("""
        SELECT match_id, datetime_str, h_team, a_team, season
        FROM match_stats
        WHERE league_id = %s
    """, (league_id,))
    results = []
    for r in cur.fetchall():
        dt_str = r[1]
        if dt_str:
            try:
                d = date.fromisoformat(str(dt_str)[:10])
            except ValueError:
                continue
        else:
            continue
        home_id = _resolve_team(r[2], team_lookup)
        away_id = _resolve_team(r[3], team_lookup)
        if not home_id or not away_id:
            continue
        results.append({
            "understat_match_id": r[0],
            "match_date": d,
            "home_fbref_team_id": home_id,
            "away_fbref_team_id": away_id,
            "season": str(r[4]) if r[4] else None,
        })
    return results


def _fetch_sofascore_matches(cur, league_id: str, team_lookup: dict[str, str]) -> list[dict]:
    """Fetch ss_events with resolved team IDs."""
    cur.execute("""
        SELECT event_id, match_date, home_team, away_team
        FROM ss_events
        WHERE league_id = %s AND event_id IS NOT NULL
    """, (league_id,))
    results = []
    for r in cur.fetchall():
        md = r[1]
        if md:
            try:
                d = date.fromisoformat(str(md)[:10])
            except ValueError:
                continue
        else:
            continue
        home_id = _resolve_team(r[2], team_lookup)
        away_id = _resolve_team(r[3], team_lookup)
        if not home_id or not away_id:
            continue
        results.append({
            "sofascore_event_id": r[0],
            "match_date": d,
            "home_fbref_team_id": home_id,
            "away_fbref_team_id": away_id,
        })
    return results


# ── Matching logic ───────────────────────────────────────────

def _match_key(home_id: str, away_id: str, d: date) -> str:
    return f"{home_id}|{away_id}|{d.isoformat()}"


def build_match_crossref(cur, league_id: str, dry_run: bool = False) -> dict:
    """Build match_crossref by merging fbref, understat, sofascore matches.

    Matching rules:
      - Rule A (conf=1.0): exact date + home + away
      - Rule B (conf=0.9): ±1 day + home + away (timezone edge case)
    """
    stats = {
        "fbref_total": 0, "us_total": 0, "ss_total": 0,
        "merged": 0, "inserted": 0,
        "rule_a": 0, "rule_b": 0,
        "unresolved_us": 0, "unresolved_ss": 0,
    }

    team_lookup = _build_team_lookup(cur, league_id)
    if not team_lookup:
        logger.warning("No team_canonical entries for %s — run build_team_canonical first", league_id)
        return stats

    # Fetch all source matches
    fbref = _fetch_fbref_matches(cur, league_id)
    understat = _fetch_understat_matches(cur, league_id, team_lookup)
    sofascore = _fetch_sofascore_matches(cur, league_id, team_lookup)

    stats["fbref_total"] = len(fbref)
    stats["us_total"] = len(understat)
    stats["ss_total"] = len(sofascore)

    logger.info("Source matches: FBref=%d Understat=%d SofaScore=%d",
                len(fbref), len(understat), len(sofascore))

    # Build crossref rows — anchor on FBref (most reliable source)
    crossref_rows: dict[str, dict] = {}  # keyed by fbref_match_id

    for m in fbref:
        key = m["fbref_match_id"]
        crossref_rows[key] = {
            "fbref_match_id": m["fbref_match_id"],
            "understat_match_id": None,
            "sofascore_event_id": None,
            "home_fbref_team_id": m["home_fbref_team_id"],
            "away_fbref_team_id": m["away_fbref_team_id"],
            "match_date": m["match_date"],
            "original_date": None,
            "is_rescheduled": False,
            "league_id": league_id,
            "season": None,
            "matched_by": "auto_exact",
            "confidence": 1.0,
            "notes": None,
        }

    # Index fbref by (home, away, date) for fast matching
    fbref_by_exact: dict[str, str] = {}  # match_key → fbref_match_id
    fbref_by_teams: dict[str, list[tuple[date, str]]] = defaultdict(list)  # "home|away" → [(date, fbref_match_id)]
    for m in fbref:
        k = _match_key(m["home_fbref_team_id"], m["away_fbref_team_id"], m["match_date"])
        fbref_by_exact[k] = m["fbref_match_id"]
        team_pair = f"{m['home_fbref_team_id']}|{m['away_fbref_team_id']}"
        fbref_by_teams[team_pair].append((m["match_date"], m["fbref_match_id"]))

    # Match Understat → FBref
    for us in understat:
        k = _match_key(us["home_fbref_team_id"], us["away_fbref_team_id"], us["match_date"])
        if k in fbref_by_exact:
            # Rule A: exact match
            fid = fbref_by_exact[k]
            crossref_rows[fid]["understat_match_id"] = us["understat_match_id"]
            if us["season"]:
                crossref_rows[fid]["season"] = us["season"]
            stats["rule_a"] += 1
        else:
            # Rule B: ±1 day window
            team_pair = f"{us['home_fbref_team_id']}|{us['away_fbref_team_id']}"
            matched = False
            for fb_date, fid in fbref_by_teams.get(team_pair, []):
                if abs((fb_date - us["match_date"]).days) <= 1:
                    crossref_rows[fid]["understat_match_id"] = us["understat_match_id"]
                    crossref_rows[fid]["confidence"] = min(crossref_rows[fid]["confidence"], 0.9)
                    crossref_rows[fid]["matched_by"] = "auto_date_window"
                    crossref_rows[fid]["notes"] = f"understat date {us['match_date']} vs fbref {fb_date}"
                    if us["season"]:
                        crossref_rows[fid]["season"] = us["season"]
                    stats["rule_b"] += 1
                    matched = True
                    break
            if not matched:
                stats["unresolved_us"] += 1
                logger.debug("Unresolved understat: id=%s %s vs %s on %s",
                             us["understat_match_id"], us["home_fbref_team_id"],
                             us["away_fbref_team_id"], us["match_date"])

    # Match SofaScore → FBref
    for ss in sofascore:
        k = _match_key(ss["home_fbref_team_id"], ss["away_fbref_team_id"], ss["match_date"])
        if k in fbref_by_exact:
            fid = fbref_by_exact[k]
            crossref_rows[fid]["sofascore_event_id"] = ss["sofascore_event_id"]
            stats["rule_a"] += 1
        else:
            team_pair = f"{ss['home_fbref_team_id']}|{ss['away_fbref_team_id']}"
            matched = False
            for fb_date, fid in fbref_by_teams.get(team_pair, []):
                if abs((fb_date - ss["match_date"]).days) <= 1:
                    crossref_rows[fid]["sofascore_event_id"] = ss["sofascore_event_id"]
                    crossref_rows[fid]["confidence"] = min(crossref_rows[fid]["confidence"], 0.9)
                    if crossref_rows[fid]["matched_by"] == "auto_exact":
                        crossref_rows[fid]["matched_by"] = "auto_date_window"
                    old_notes = crossref_rows[fid]["notes"] or ""
                    crossref_rows[fid]["notes"] = (
                        f"{old_notes}; sofascore date {ss['match_date']} vs fbref {fb_date}".strip("; ")
                    )
                    stats["rule_b"] += 1
                    matched = True
                    break
            if not matched:
                stats["unresolved_ss"] += 1
                logger.debug("Unresolved sofascore: id=%s %s vs %s on %s",
                             ss["sofascore_event_id"], ss["home_fbref_team_id"],
                             ss["away_fbref_team_id"], ss["match_date"])

    stats["merged"] = sum(
        1 for r in crossref_rows.values()
        if r["understat_match_id"] or r["sofascore_event_id"]
    )

    logger.info("Matching done: merged=%d rule_a=%d rule_b=%d unresolved_us=%d unresolved_ss=%d",
                stats["merged"], stats["rule_a"], stats["rule_b"],
                stats["unresolved_us"], stats["unresolved_ss"])

    if dry_run:
        triple = sum(
            1 for r in crossref_rows.values()
            if r["understat_match_id"] and r["fbref_match_id"] and r["sofascore_event_id"]
        )
        logger.info("[DRY] Would upsert %d crossref rows (triple-link: %d / %.1f%%)",
                    len(crossref_rows), triple,
                    100.0 * triple / len(crossref_rows) if crossref_rows else 0)
        return stats

    # Upsert — use source-specific conflict key, NOT surrogate PK.
    # Anchor on fbref_match_id since all rows have one (FBref is our anchor source).
    sql = """
        INSERT INTO match_crossref
            (fbref_match_id, understat_match_id, sofascore_event_id,
             home_fbref_team_id, away_fbref_team_id, match_date,
             original_date, is_rescheduled,
             league_id, season, matched_by, confidence, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (league_id, fbref_match_id) WHERE fbref_match_id IS NOT NULL
        DO UPDATE SET
            understat_match_id = COALESCE(EXCLUDED.understat_match_id, match_crossref.understat_match_id),
            sofascore_event_id = COALESCE(EXCLUDED.sofascore_event_id, match_crossref.sofascore_event_id),
            match_date         = EXCLUDED.match_date,
            original_date      = CASE
                WHEN match_crossref.match_date != EXCLUDED.match_date
                     AND match_crossref.original_date IS NULL
                THEN match_crossref.match_date
                ELSE match_crossref.original_date
            END,
            is_rescheduled     = CASE
                WHEN match_crossref.match_date != EXCLUDED.match_date THEN TRUE
                ELSE match_crossref.is_rescheduled
            END,
            season             = COALESCE(EXCLUDED.season, match_crossref.season),
            matched_by         = EXCLUDED.matched_by,
            confidence         = EXCLUDED.confidence,
            notes              = EXCLUDED.notes,
            updated_at         = NOW()
    """
    rows = [
        (
            r["fbref_match_id"], r["understat_match_id"], r["sofascore_event_id"],
            r["home_fbref_team_id"], r["away_fbref_team_id"], r["match_date"],
            r["original_date"], r["is_rescheduled"],
            r["league_id"], r["season"], r["matched_by"], r["confidence"], r["notes"],
        )
        for r in crossref_rows.values()
    ]
    cur.executemany(sql, rows)
    stats["inserted"] = len(rows)
    logger.info("match_crossref: %d rows upserted for %s", len(rows), league_id)
    return stats


# ── Validation ───────────────────────────────────────────────

def validate_crossref(cur, league_id: str) -> bool:
    """Run sanity checks on match_crossref. Returns True if all pass."""
    ok = True

    # Check 1: no duplicate source IDs
    for col in ("understat_match_id", "fbref_match_id", "sofascore_event_id"):
        cur.execute(f"""
            SELECT {col}, COUNT(*) FROM match_crossref
            WHERE league_id = %s AND {col} IS NOT NULL
            GROUP BY {col} HAVING COUNT(*) > 1
        """, (league_id,))
        dupes = cur.fetchall()
        if dupes:
            logger.error("FAIL: duplicate %s found: %s", col, dupes)
            ok = False
        else:
            logger.info("OK: no duplicate %s", col)

    # Check 2: coverage stats
    cur.execute("""
        SELECT
            COUNT(*) AS total,
            COUNT(understat_match_id) AS has_us,
            COUNT(fbref_match_id) AS has_fb,
            COUNT(sofascore_event_id) AS has_ss,
            COUNT(CASE WHEN understat_match_id IS NOT NULL
                        AND fbref_match_id IS NOT NULL
                        AND sofascore_event_id IS NOT NULL THEN 1 END) AS triple
        FROM match_crossref WHERE league_id = %s
    """, (league_id,))
    r = cur.fetchone()
    if r:
        total, has_us, has_fb, has_ss, triple = r
        logger.info("Coverage: total=%d US=%d(%.0f%%) FB=%d(%.0f%%) SS=%d(%.0f%%) triple=%d(%.0f%%)",
                    total,
                    has_us, 100 * has_us / max(total, 1),
                    has_fb, 100 * has_fb / max(total, 1),
                    has_ss, 100 * has_ss / max(total, 1),
                    triple, 100 * triple / max(total, 1))

    # Check 3: is_rescheduled consistency
    cur.execute("""
        SELECT COUNT(*) FROM match_crossref
        WHERE league_id = %s AND is_rescheduled = TRUE AND original_date IS NULL
    """, (league_id,))
    bad1 = cur.fetchone()[0]
    if bad1 > 0:
        logger.error("FAIL: %d rows with is_rescheduled=TRUE but original_date IS NULL", bad1)
        ok = False
    else:
        logger.info("OK: is_rescheduled=TRUE always has original_date")

    cur.execute("""
        SELECT COUNT(*) FROM match_crossref
        WHERE league_id = %s AND is_rescheduled = FALSE AND original_date IS NOT NULL
    """, (league_id,))
    bad2 = cur.fetchone()[0]
    if bad2 > 0:
        logger.warning("WARN: %d rows with is_rescheduled=FALSE but original_date IS NOT NULL", bad2)
    else:
        logger.info("OK: is_rescheduled=FALSE has no original_date")

    return ok


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build match_crossref")
    parser.add_argument("--league", default="EPL", help="League ID")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    league_id = args.league.upper()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info("=" * 50)
            logger.info("BUILD MATCH CROSSREF | league=%s dry=%s", league_id, args.dry_run)
            logger.info("=" * 50)

            stats = build_match_crossref(cur, league_id, dry_run=args.dry_run)
            if not args.dry_run:
                conn.commit()
                logger.info("─" * 50)
                logger.info("Running validation checks...")
                validate_crossref(cur, league_id)

            logger.info("─" * 50)
            logger.info("Done. Stats: %s", stats)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
