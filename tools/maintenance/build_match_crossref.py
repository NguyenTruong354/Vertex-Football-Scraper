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


def _new_crossref_row(league_id: str, **kwargs) -> dict:
    """Create a blank crossref row with defaults, overridden by kwargs."""
    row = {
        "fbref_match_id": None,
        "understat_match_id": None,
        "sofascore_event_id": None,
        "home_fbref_team_id": None,
        "away_fbref_team_id": None,
        "match_date": None,
        "original_date": None,
        "is_rescheduled": False,
        "league_id": league_id,
        "season": None,
        "matched_by": "auto_exact",
        "confidence": 1.0,
        "notes": None,
    }
    row.update(kwargs)
    return row


def _match_secondary_to_index(
    secondary: list[dict],
    id_field: str,
    anchor_by_exact: dict[str, str],
    anchor_by_teams: dict[str, list[tuple[date, str]]],
    crossref_rows: dict[str, dict],
    stats: dict,
    source_label: str,
) -> None:
    """Match secondary source matches to anchor index using Rule A/B.

    Mutates crossref_rows and stats in place.
    """
    unresolved_key = f"unresolved_{source_label}"
    for m in secondary:
        k = _match_key(m["home_fbref_team_id"], m["away_fbref_team_id"], m["match_date"])
        if k in anchor_by_exact:
            aid = anchor_by_exact[k]
            crossref_rows[aid][id_field] = m[id_field]
            if m.get("season") and not crossref_rows[aid].get("season"):
                crossref_rows[aid]["season"] = m["season"]
            stats["rule_a"] += 1
        else:
            team_pair = f"{m['home_fbref_team_id']}|{m['away_fbref_team_id']}"
            matched = False
            for anch_date, aid in anchor_by_teams.get(team_pair, []):
                if abs((anch_date - m["match_date"]).days) <= 1:
                    crossref_rows[aid][id_field] = m[id_field]
                    crossref_rows[aid]["confidence"] = min(
                        crossref_rows[aid]["confidence"], 0.9)
                    crossref_rows[aid]["matched_by"] = "auto_date_window"
                    old_notes = crossref_rows[aid]["notes"] or ""
                    crossref_rows[aid]["notes"] = (
                        f"{old_notes}; {source_label} date {m['match_date']} "
                        f"vs anchor {anch_date}".strip("; ")
                    )
                    if m.get("season") and not crossref_rows[aid].get("season"):
                        crossref_rows[aid]["season"] = m["season"]
                    stats["rule_b"] += 1
                    matched = True
                    break
            if not matched:
                stats[unresolved_key] += 1
                logger.debug("Unresolved %s: id=%s %s vs %s on %s",
                             source_label, m[id_field],
                             m["home_fbref_team_id"],
                             m["away_fbref_team_id"], m["match_date"])


def _build_anchor_index(
    matches: list[dict],
    id_field: str,
) -> tuple[dict[str, str], dict[str, list[tuple[date, str]]]]:
    """Build exact-key and team-pair indexes for anchor matches."""
    by_exact: dict[str, str] = {}
    by_teams: dict[str, list[tuple[date, str]]] = defaultdict(list)
    for m in matches:
        mid = str(m[id_field]) if m[id_field] is not None else m[id_field]
        k = _match_key(m["home_fbref_team_id"], m["away_fbref_team_id"], m["match_date"])
        by_exact[k] = mid
        team_pair = f"{m['home_fbref_team_id']}|{m['away_fbref_team_id']}"
        by_teams[team_pair].append((m["match_date"], mid))
    return by_exact, by_teams


def _build_fbref_anchor(
    fbref: list[dict],
    understat: list[dict],
    sofascore: list[dict],
    league_id: str,
    stats: dict,
) -> tuple[dict[str, dict], dict]:
    """Build crossref rows anchored on FBref fixtures."""
    crossref_rows: dict[str, dict] = {}

    for m in fbref:
        key = m["fbref_match_id"]
        crossref_rows[key] = _new_crossref_row(
            league_id,
            fbref_match_id=m["fbref_match_id"],
            home_fbref_team_id=m["home_fbref_team_id"],
            away_fbref_team_id=m["away_fbref_team_id"],
            match_date=m["match_date"],
        )

    by_exact, by_teams = _build_anchor_index(fbref, "fbref_match_id")

    _match_secondary_to_index(
        understat, "understat_match_id",
        by_exact, by_teams, crossref_rows, stats, "us")
    _match_secondary_to_index(
        sofascore, "sofascore_event_id",
        by_exact, by_teams, crossref_rows, stats, "ss")

    return crossref_rows, stats


def _build_understat_anchor(
    understat: list[dict],
    sofascore: list[dict],
    league_id: str,
    stats: dict,
) -> tuple[dict[str, dict], dict]:
    """Build crossref rows anchored on Understat matches (no FBref data)."""
    logger.info("No FBref fixtures — anchoring on Understat (%d matches)", len(understat))
    crossref_rows: dict[str, dict] = {}

    for m in understat:
        key = str(m["understat_match_id"])
        crossref_rows[key] = _new_crossref_row(
            league_id,
            understat_match_id=m["understat_match_id"],
            home_fbref_team_id=m["home_fbref_team_id"],
            away_fbref_team_id=m["away_fbref_team_id"],
            match_date=m["match_date"],
            season=m.get("season"),
        )

    by_exact, by_teams = _build_anchor_index(understat, "understat_match_id")

    _match_secondary_to_index(
        sofascore, "sofascore_event_id",
        by_exact, by_teams, crossref_rows, stats, "ss")

    return crossref_rows, stats


def build_match_crossref(cur, league_id: str, dry_run: bool = False) -> dict:
    """Build match_crossref by merging fbref, understat, sofascore matches.

    Anchor strategy:
      - If FBref fixtures exist: anchor on FBref, attach US/SS via matching
      - If no FBref fixtures: anchor on Understat, attach SS via matching

    Matching rules:
      - Rule A (conf=1.0): exact date + home + away
      - Rule B (conf=0.9): ±1 day + home + away (timezone edge case)
    """
    stats = {
        "fbref_total": 0, "us_total": 0, "ss_total": 0,
        "merged": 0, "inserted": 0,
        "rule_a": 0, "rule_b": 0,
        "unresolved_us": 0, "unresolved_ss": 0,
        "anchor": "fbref",
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

    if fbref:
        crossref_rows, stats = _build_fbref_anchor(
            fbref, understat, sofascore, league_id, stats)
    elif understat:
        stats["anchor"] = "understat"
        crossref_rows, stats = _build_understat_anchor(
            understat, sofascore, league_id, stats)
    else:
        logger.warning("No FBref or Understat data for %s — nothing to build", league_id)
        return stats

    stats["merged"] = sum(
        1 for r in crossref_rows.values()
        if (r["understat_match_id"] and r["fbref_match_id"])
        or (r["sofascore_event_id"] and (r["fbref_match_id"] or r["understat_match_id"]))
    )

    logger.info("Matching done: anchor=%s merged=%d rule_a=%d rule_b=%d "
                "unresolved_us=%d unresolved_ss=%d",
                stats["anchor"], stats["merged"], stats["rule_a"], stats["rule_b"],
                stats["unresolved_us"], stats["unresolved_ss"])

    if dry_run:
        linked = sum(
            1 for r in crossref_rows.values()
            if sum(1 for k in ("fbref_match_id", "understat_match_id", "sofascore_event_id")
                   if r.get(k)) >= 2
        )
        logger.info("[DRY] Would upsert %d crossref rows (multi-link: %d / %.1f%%)",
                    len(crossref_rows), linked,
                    100.0 * linked / len(crossref_rows) if crossref_rows else 0)
        return stats

    # Clear secondary IDs from old crossref records to prevent UniqueViolation during reassignment
    if stats["anchor"] == "fbref":
        for r in crossref_rows.values():
            if r.get("understat_match_id"):
                cur.execute(
                    "UPDATE match_crossref SET understat_match_id = NULL WHERE league_id = %s AND understat_match_id = %s",
                    (league_id, r["understat_match_id"])
                )
            if r.get("sofascore_event_id"):
                cur.execute(
                    "UPDATE match_crossref SET sofascore_event_id = NULL WHERE league_id = %s AND sofascore_event_id = %s",
                    (league_id, r["sofascore_event_id"])
                )
    else:
        for r in crossref_rows.values():
            if r.get("sofascore_event_id"):
                cur.execute(
                    "UPDATE match_crossref SET sofascore_event_id = NULL WHERE league_id = %s AND sofascore_event_id = %s",
                    (league_id, r["sofascore_event_id"])
                )

    # Upsert — use source-specific conflict key based on anchor.
    if stats["anchor"] == "fbref":
        conflict_clause = (
            "ON CONFLICT (league_id, fbref_match_id) "
            "WHERE fbref_match_id IS NOT NULL"
        )
    else:
        conflict_clause = (
            "ON CONFLICT (league_id, understat_match_id) "
            "WHERE understat_match_id IS NOT NULL"
        )

    sql = f"""
        INSERT INTO match_crossref
            (fbref_match_id, understat_match_id, sofascore_event_id,
             home_fbref_team_id, away_fbref_team_id, match_date,
             original_date, is_rescheduled,
             league_id, season, matched_by, confidence, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        {conflict_clause}
        DO UPDATE SET
            understat_match_id = COALESCE(EXCLUDED.understat_match_id, match_crossref.understat_match_id),
            sofascore_event_id = COALESCE(EXCLUDED.sofascore_event_id, match_crossref.sofascore_event_id),
            fbref_match_id     = COALESCE(EXCLUDED.fbref_match_id, match_crossref.fbref_match_id),
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
    logger.info("match_crossref: %d rows upserted for %s (anchor=%s)",
                len(rows), league_id, stats["anchor"])
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
