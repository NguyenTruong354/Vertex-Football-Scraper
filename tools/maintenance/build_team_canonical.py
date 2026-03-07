"""
build_team_canonical.py — Seed team_registry + team_canonical from source tables.

Phase A of Tầng 2 foundation.
Reads from: standings, fixtures, ss_events, match_stats, team_metadata.
Writes to: team_registry, team_canonical.

Usage:
    python tools/maintenance/build_team_canonical.py              # EPL only
    python tools/maintenance/build_team_canonical.py --league EPL
    python tools/maintenance/build_team_canonical.py --dry-run    # preview only
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_connection  # noqa: E402

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)

# ── Normalize helpers ────────────────────────────────────────

ABBREVIATION_MAP = {
    "utd": "united",
    "fc": "",
    "afc": "",
    "cf": "",
    "sc": "",
}

# Known alias clusters: all forms → single canonical normalized form.
# Key = normalized variant, Value = canonical normalized form.
# Built from common EPL/European name differences across sources.
ALIAS_MAP = {
    "wolves": "wolverhampton wanderers",
    "wolverhampton": "wolverhampton wanderers",
    "tottenham": "tottenham hotspur",
    "spurs": "tottenham hotspur",
    "west ham": "west ham united",
    "leeds": "leeds united",
    "brighton": "brighton hove albion",
    "brighton and hove albion": "brighton hove albion",
    "man city": "manchester city",
    "man united": "manchester united",
    "man utd": "manchester united",
    "newcastle": "newcastle united",
    "nott'm forest": "nottingham forest",
    "nottm forest": "nottingham forest",
    "leicester": "leicester city",
    "palace": "crystal palace",
}


def normalize_team_name(name: str) -> str:
    """Deterministic team name normalization.

    Steps:
      1) Unicode NFD + strip diacritics (Atlético → Atletico)
      2) Lowercase + trim + collapse whitespace
      3) Unify apostrophe variants (U+2019 → U+0027)
      4) Remove punctuation noise (., -, &)
      5) Apply abbreviation map (utd → united, fc → '')
      6) Apply alias map (wolves → wolverhampton wanderers)
    """
    if not name:
        return ""
    # 1) Unicode NFD + strip combining marks
    s = unicodedata.normalize("NFD", name)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # 2) Lower, trim, collapse spaces
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    # 3) Unify apostrophes
    s = s.replace("\u2019", "'").replace("\u2018", "'")
    # 4) Remove punctuation noise
    s = s.replace(".", "").replace("-", " ").replace("&", " ")
    s = s.replace("'", "")
    s = re.sub(r"\s+", " ", s).strip()
    # 5) Abbreviation map — word-level
    tokens = s.split()
    tokens = [ABBREVIATION_MAP.get(t, t) for t in tokens]
    tokens = [t for t in tokens if t]  # remove empty from fc→''
    s = " ".join(tokens).strip()
    # 6) Alias map — full string lookup
    s = ALIAS_MAP.get(s, s)
    return s


# ── DB helpers ───────────────────────────────────────────────

def _fetch_standings_teams(cur, league_id: str) -> list[dict]:
    """Get distinct (team_id, team_name) from standings.

    Filters to only FBref team_ids (8-char hex strings) to exclude
    SofaScore numeric IDs that may have leaked into standings.
    """
    cur.execute(
        "SELECT DISTINCT team_id, team_name FROM standings "
        "WHERE league_id = %s AND team_id ~ '^[0-9a-f]{8}$'",
        (league_id,),
    )
    return [{"fbref_team_id": r[0], "fbref_team_name": r[1]} for r in cur.fetchall()]


def _fetch_fixture_teams(cur, league_id: str) -> list[dict]:
    """Get distinct teams from fixtures (home + away)."""
    cur.execute("""
        SELECT DISTINCT home_team_id, home_team FROM fixtures WHERE league_id = %s AND home_team_id IS NOT NULL
        UNION
        SELECT DISTINCT away_team_id, away_team FROM fixtures WHERE league_id = %s AND away_team_id IS NOT NULL
    """, (league_id, league_id))
    return [{"fbref_team_id": r[0], "fbref_name": r[1]} for r in cur.fetchall()]


def _fetch_sofascore_teams(cur, league_id: str) -> list[dict]:
    """Get distinct (team_name, team_id) from ss_events."""
    cur.execute("""
        SELECT DISTINCT home_team, home_team_id FROM ss_events WHERE league_id = %s AND home_team_id IS NOT NULL
        UNION
        SELECT DISTINCT away_team, away_team_id FROM ss_events WHERE league_id = %s AND away_team_id IS NOT NULL
    """, (league_id, league_id))
    return [{"sofascore_name": r[0], "sofascore_team_id": r[1]} for r in cur.fetchall()]


def _fetch_understat_teams(cur, league_id: str) -> list[str]:
    """Get distinct team names from match_stats (understat has no team_id)."""
    cur.execute("""
        SELECT DISTINCT h_team FROM match_stats WHERE league_id = %s
        UNION
        SELECT DISTINCT a_team FROM match_stats WHERE league_id = %s
    """, (league_id, league_id))
    return [r[0] for r in cur.fetchall() if r[0]]


def _fetch_tm_teams(cur, league_id: str) -> list[dict]:
    """Get distinct (team_name, team_id) from team_metadata."""
    cur.execute(
        "SELECT DISTINCT team_name, team_id FROM team_metadata WHERE league_id = %s",
        (league_id,),
    )
    return [{"tm_name": r[0], "tm_team_id": r[1]} for r in cur.fetchall()]


# ── Build functions ──────────────────────────────────────────

def build_team_registry(cur, league_id: str, dry_run: bool = False) -> int:
    """Seed team_registry from standings (most reliable source)."""
    teams = _fetch_standings_teams(cur, league_id)
    if not teams:
        logger.warning("No standings teams found for %s", league_id)
        return 0

    if dry_run:
        logger.info("[DRY] Would upsert %d teams into team_registry", len(teams))
        for t in teams:
            logger.info("  %s | %s", t["fbref_team_id"], t["fbref_team_name"])
        return len(teams)

    sql = """
        INSERT INTO team_registry (league_id, fbref_team_id, fbref_team_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (league_id, fbref_team_id) DO UPDATE SET
            fbref_team_name = EXCLUDED.fbref_team_name,
            updated_at = NOW()
    """
    rows = [(league_id, t["fbref_team_id"], t["fbref_team_name"]) for t in teams]
    cur.executemany(sql, rows)
    logger.info("team_registry: %d rows upserted for %s", len(rows), league_id)
    return len(rows)


def build_team_canonical(cur, league_id: str, dry_run: bool = False) -> dict:
    """Build team_canonical by matching source aliases to fbref anchor.

    Strategy:
      1) Seed from standings (fbref_team_id → canonical_name)
      2) Enrich with fixture names
      3) Match SofaScore teams by normalized name
      4) Match Understat teams by normalized name
      5) Match Transfermarkt teams by normalized name
      6) Report unresolved
    """
    stats = {"seeded": 0, "ss_matched": 0, "us_matched": 0, "tm_matched": 0, "unresolved": []}

    # Step 1: Seed from standings
    standings_teams = _fetch_standings_teams(cur, league_id)
    if not standings_teams:
        logger.warning("No standings data for %s — cannot build canonical", league_id)
        return stats

    # Build canonical map: normalized_name → {fbref_team_id, canonical_name, fbref_name}
    canonical_map: dict[str, dict] = {}
    for t in standings_teams:
        norm = normalize_team_name(t["fbref_team_name"])
        canonical_map[norm] = {
            "league_id": league_id,
            "fbref_team_id": t["fbref_team_id"],
            "canonical_name": t["fbref_team_name"],
            "fbref_name": t["fbref_team_name"],
            "understat_name": None,
            "sofascore_name": None,
            "tm_name": None,
            "sofascore_team_id": None,
            "tm_team_id": None,
            "matched_by": "manual_seed",
        }
    stats["seeded"] = len(canonical_map)

    # Step 2: Enrich fixture names (same fbref_team_id, may have slightly different display name)
    fixture_teams = _fetch_fixture_teams(cur, league_id)
    for ft in fixture_teams:
        for entry in canonical_map.values():
            if entry["fbref_team_id"] == ft["fbref_team_id"]:
                if not entry["fbref_name"]:
                    entry["fbref_name"] = ft["fbref_name"]
                break

    # Step 3: Match SofaScore by normalized name
    ss_teams = _fetch_sofascore_teams(cur, league_id)
    for ss in ss_teams:
        norm = normalize_team_name(ss["sofascore_name"])
        if norm in canonical_map:
            canonical_map[norm]["sofascore_name"] = ss["sofascore_name"]
            canonical_map[norm]["sofascore_team_id"] = ss["sofascore_team_id"]
            canonical_map[norm]["matched_by"] = "auto_exact"
            stats["ss_matched"] += 1
        else:
            stats["unresolved"].append(("sofascore", ss["sofascore_name"], norm))

    # Step 4: Match Understat by normalized name
    us_teams = _fetch_understat_teams(cur, league_id)
    for us_name in us_teams:
        norm = normalize_team_name(us_name)
        if norm in canonical_map:
            canonical_map[norm]["understat_name"] = us_name
            stats["us_matched"] += 1
        else:
            stats["unresolved"].append(("understat", us_name, norm))

    # Step 5: Match Transfermarkt by normalized name
    tm_teams = _fetch_tm_teams(cur, league_id)
    for tm in tm_teams:
        norm = normalize_team_name(tm["tm_name"])
        if norm in canonical_map:
            canonical_map[norm]["tm_name"] = tm["tm_name"]
            canonical_map[norm]["tm_team_id"] = tm["tm_team_id"]
            stats["tm_matched"] += 1
        else:
            stats["unresolved"].append(("transfermarkt", tm["tm_name"], norm))

    # Summary log
    logger.info("Canonical map built: %d teams | SS=%d US=%d TM=%d | unresolved=%d",
                len(canonical_map), stats["ss_matched"], stats["us_matched"],
                stats["tm_matched"], len(stats["unresolved"]))

    if stats["unresolved"]:
        logger.warning("Unresolved teams:")
        for source, name, norm in stats["unresolved"]:
            logger.warning("  [%s] '%s' (normalized: '%s')", source, name, norm)

    if dry_run:
        logger.info("[DRY] Would upsert %d rows into team_canonical", len(canonical_map))
        return stats

    # Step 6: Upsert into team_canonical
    sql = """
        INSERT INTO team_canonical
            (league_id, fbref_team_id, canonical_name,
             fbref_name, understat_name, sofascore_name, tm_name,
             sofascore_team_id, tm_team_id, matched_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (league_id, fbref_team_id) DO UPDATE SET
            canonical_name    = EXCLUDED.canonical_name,
            fbref_name        = EXCLUDED.fbref_name,
            understat_name    = COALESCE(EXCLUDED.understat_name, team_canonical.understat_name),
            sofascore_name    = COALESCE(EXCLUDED.sofascore_name, team_canonical.sofascore_name),
            tm_name           = COALESCE(EXCLUDED.tm_name, team_canonical.tm_name),
            sofascore_team_id = COALESCE(EXCLUDED.sofascore_team_id, team_canonical.sofascore_team_id),
            tm_team_id        = COALESCE(EXCLUDED.tm_team_id, team_canonical.tm_team_id),
            matched_by        = EXCLUDED.matched_by,
            updated_at        = NOW()
    """
    rows = [
        (
            e["league_id"], e["fbref_team_id"], e["canonical_name"],
            e["fbref_name"], e["understat_name"], e["sofascore_name"], e["tm_name"],
            e["sofascore_team_id"], e["tm_team_id"], e["matched_by"],
        )
        for e in canonical_map.values()
    ]
    cur.executemany(sql, rows)
    logger.info("team_canonical: %d rows upserted for %s", len(rows), league_id)
    return stats


# ── Main ─────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Build team_registry + team_canonical")
    parser.add_argument("--league", default="EPL", help="League ID")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    args = parser.parse_args()

    league_id = args.league.upper()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            logger.info("=" * 50)
            logger.info("BUILD TEAM CANONICAL | league=%s dry=%s", league_id, args.dry_run)
            logger.info("=" * 50)

            reg_count = build_team_registry(cur, league_id, dry_run=args.dry_run)
            if not args.dry_run:
                conn.commit()

            stats = build_team_canonical(cur, league_id, dry_run=args.dry_run)
            if not args.dry_run:
                conn.commit()

            # Summary
            logger.info("─" * 50)
            logger.info("Registry: %d teams", reg_count)
            logger.info("Canonical: seeded=%d SS=%d US=%d TM=%d unresolved=%d",
                        stats["seeded"], stats["ss_matched"], stats["us_matched"],
                        stats["tm_matched"], len(stats["unresolved"]))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
