"""
AI Insight Pipeline — Producer Layer

Applies pre-LLM gates (cooldown, delta significance) before enqueueing
jobs into ai_insight_jobs. Uses ON CONFLICT DO NOTHING for dedupe safety.

Phase B (shadow): cooldown/delta gates query is_published=TRUE only,
so in shadow mode they effectively pass — queue is bounded by dedupe_key
partial unique constraint (1 active job per event).
"""

import hashlib
import json
import logging
from typing import Optional, Any

log = logging.getLogger(__name__)

# ── Cooldown defaults (seconds) ──
COOLDOWN_LIVE_BADGE = 600   # 10 minutes


def _safe_int(val: Any) -> int:
    try:
        if isinstance(val, str):
            val = val.replace('%', '').strip()
        return int(val)
    except (ValueError, TypeError):
        return 0


def _compute_fingerprint(payload: dict) -> str:
    """SHA256 of canonical JSON (sorted keys, exclude volatiles, 4-decimal floats)."""
    EXCLUDE_KEYS = {"timestamp", "worker_id", "lease_until"}

    def _normalize(obj):
        if isinstance(obj, dict):
            return {k: _normalize(v) for k, v in sorted(obj.items())
                    if k not in EXCLUDE_KEYS}
        elif isinstance(obj, float):
            return round(obj, 4)
        elif isinstance(obj, list):
            return [_normalize(item) for item in obj]
        return obj

    canonical = json.dumps(_normalize(payload), sort_keys=True,
                           separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _check_cooldown(cur, event_id: int, job_type: str,
                    cooldown_seconds: int) -> bool:
    """Return True if cooldown has passed (OK to enqueue). False = blocked."""
    cur.execute("""
        SELECT finished_at
        FROM ai_insight_jobs
        WHERE event_id = %s
          AND job_type = %s
          AND status = 'succeeded'
          AND is_published = TRUE
        ORDER BY finished_at DESC
        LIMIT 1
    """, (event_id, job_type))
    row = cur.fetchone()
    if row is None:
        return True
    cur.execute(
        "SELECT NOW() - %s > make_interval(secs => %s)",
        (row[0], cooldown_seconds),
    )
    return cur.fetchone()[0]


def _check_delta_significance(payload: dict, cur, event_id: int,
                              job_type: str) -> bool:
    """Return True if payload differs significantly from last published insight."""
    cur.execute("""
        SELECT payload_json
        FROM ai_insight_jobs
        WHERE event_id = %s
          AND job_type = %s
          AND status = 'succeeded'
          AND is_published = TRUE
        ORDER BY finished_at DESC
        LIMIT 1
    """, (event_id, job_type))
    row = cur.fetchone()
    if row is None:
        return True  # no prior published → significant by default

    last = row[0] if isinstance(row[0], dict) else json.loads(row[0])

    # Score change is always significant
    if (payload.get("home_score") != last.get("home_score")
            or payload.get("away_score") != last.get("away_score")):
        return True

    # Possession delta >= 5
    if abs(payload.get("possession_home", 0)
           - last.get("possession_home", 0)) >= 5:
        return True

    # xG gap delta >= 0.25
    old_gap = abs(last.get("xg_home", 0) - last.get("xg_away", 0))
    new_gap = abs(payload.get("xg_home", 0) - payload.get("xg_away", 0))
    if abs(new_gap - old_gap) >= 0.25:
        return True

    return False


def _has_priority_bypass(payload: dict) -> bool:
    """Check if a priority bypass condition exists (goal, red card, late goal)."""
    incidents = payload.get("incidents", [])
    for inc in incidents:
        inc_type = inc.get("incidentType", "")
        inc_class = inc.get("incidentClass", "")
        if inc_type == "goal":
            return True
        if inc_type == "card" and inc_class in ("red", "yellowRed"):
            return True
    return False


# ════════════════════════════════════════════════════════════
# PUBLIC API
# ════════════════════════════════════════════════════════════

def enqueue_live_badge(
    event_id: int,
    league_id: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    minute: int,
    statistics: dict,
    incidents: list,
    momentum_score: int,
    prompt_version: str = "v1",
) -> Optional[int]:
    """
    Pre-gate + enqueue a live_badge job.
    Returns job ID if enqueued, None if skipped (gate-blocked or dedupe).
    """
    from db.config_db import get_connection

    payload = {
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "minute": minute,
        "momentum_score": momentum_score,
        "possession_home": _safe_int(
            statistics.get("Ball possession", {}).get("home", 50)),
        "possession_away": _safe_int(
            statistics.get("Ball possession", {}).get("away", 50)),
        "shots_on_home": _safe_int(
            statistics.get("Shots on target", {}).get("home", 0)),
        "shots_on_away": _safe_int(
            statistics.get("Shots on target", {}).get("away", 0)),
        "xg_home": float(
            statistics.get("Expected goals", {}).get("home", 0.0)),
        "xg_away": float(
            statistics.get("Expected goals", {}).get("away", 0.0)),
        "incidents": incidents,
    }

    has_bypass = _has_priority_bypass(payload)
    dedupe_key = f"live_badge:{league_id}:{event_id}"
    fingerprint = _compute_fingerprint(payload)
    priority = 50 if has_bypass else 100

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Pre-gate 1: Cooldown (skip if bypass)
        if not has_bypass:
            if not _check_cooldown(cur, event_id, "live_badge",
                                   COOLDOWN_LIVE_BADGE):
                log.debug("PRE-GATE cooldown blocked event=%d", event_id)
                return None

        # Pre-gate 2: Delta significance (skip if bypass)
        if not has_bypass:
            if not _check_delta_significance(payload, cur, event_id,
                                             "live_badge"):
                log.debug("PRE-GATE delta blocked event=%d", event_id)
                return None

        # Enqueue with ON CONFLICT DO NOTHING (dedupe contract)
        cur.execute("""
            INSERT INTO ai_insight_jobs
                (job_type, event_id, league_id, team_focus, status, priority,
                 dedupe_key, fingerprint, payload_json, prompt_version)
            VALUES
                ('live_badge', %s, %s, %s, 'queued', %s,
                 %s, %s, %s, %s)
            ON CONFLICT (dedupe_key) WHERE status IN ('queued', 'running')
            DO NOTHING
            RETURNING id
        """, (
            event_id, league_id, home_team,
            priority, dedupe_key, fingerprint,
            json.dumps(payload, ensure_ascii=False),
            prompt_version,
        ))

        row = cur.fetchone()
        conn.commit()

        if row:
            log.info("Enqueued live_badge job=%d event=%d league=%s bypass=%s",
                     row[0], event_id, league_id, has_bypass)
            return row[0]
        else:
            log.debug("Dedupe blocked live_badge event=%d (active job exists)",
                      event_id)
            return None

    except Exception as exc:
        conn.rollback()
        log.error("Failed to enqueue live_badge event=%d: %s", event_id, exc)
        return None
    finally:
        conn.close()


def enqueue_match_story(
    event_id: int,
    league_id: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    statistics: dict,
    incidents: list,
    prompt_version: str = "v1",
) -> Optional[int]:
    """
    Enqueue a match_story job (post-match).
    No cooldown/delta pre-gates — match_story is one-shot per event.
    Dedupe via ON CONFLICT protects against double-enqueue.
    Returns job ID if enqueued, None if skipped.
    """
    from db.config_db import get_connection

    payload = {
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "statistics": statistics,
        "incidents": incidents,
    }

    dedupe_key = f"match_story:{league_id}:{event_id}"
    fingerprint = _compute_fingerprint(payload)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_insight_jobs
                (job_type, event_id, league_id, team_focus, status, priority,
                 dedupe_key, fingerprint, payload_json, prompt_version)
            VALUES
                ('match_story', %s, %s, %s, 'queued', 100,
                 %s, %s, %s, %s)
            ON CONFLICT (dedupe_key) WHERE status IN ('queued', 'running')
            DO NOTHING
            RETURNING id
        """, (
            event_id, league_id, home_team,
            dedupe_key, fingerprint,
            json.dumps(payload, ensure_ascii=False),
            prompt_version,
        ))
        row = cur.fetchone()
        conn.commit()

        if row:
            log.info("Enqueued match_story job=%d event=%d league=%s",
                     row[0], event_id, league_id)
            return row[0]
        else:
            log.debug("Dedupe blocked match_story event=%d", event_id)
            return None
    except Exception as exc:
        conn.rollback()
        log.error("Failed to enqueue match_story event=%d: %s", event_id, exc)
        return None
    finally:
        conn.close()


def enqueue_player_trend(
    league_id: str,
    player_id: str,
    player_name: str,
    trend: str,
    trend_score: int,
    goals: list,
    assists: list,
    xg_arr: list,
    xa_arr: list,
    match_count: int,
    prompt_version: str = "v1",
) -> Optional[int]:
    """
    Enqueue a player_trend job (nightly).
    Dedupe key is per player+league (one active job per player at a time).
    Returns job ID if enqueued, None if skipped.
    """
    from db.config_db import get_connection

    payload = {
        "player_id": player_id,
        "player_name": player_name,
        "trend": trend,
        "trend_score": trend_score,
        "goals": goals,
        "assists": assists,
        "xg_arr": xg_arr,
        "xa_arr": xa_arr,
        "match_count": match_count,
    }

    dedupe_key = f"player_trend:{league_id}:{player_id}"
    fingerprint = _compute_fingerprint(payload)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ai_insight_jobs
                (job_type, event_id, league_id, team_focus, status, priority,
                 dedupe_key, fingerprint, payload_json, prompt_version)
            VALUES
                ('player_trend', NULL, %s, %s, 'queued', 100,
                 %s, %s, %s, %s)
            ON CONFLICT (dedupe_key) WHERE status IN ('queued', 'running')
            DO NOTHING
            RETURNING id
        """, (
            league_id, player_name,
            dedupe_key, fingerprint,
            json.dumps(payload, ensure_ascii=False),
            prompt_version,
        ))
        row = cur.fetchone()
        conn.commit()

        if row:
            log.info("Enqueued player_trend job=%d player=%s league=%s",
                     row[0], player_name, league_id)
            return row[0]
        else:
            log.debug("Dedupe blocked player_trend player=%s", player_name)
            return None
    except Exception as exc:
        conn.rollback()
        log.error("Failed to enqueue player_trend player=%s: %s",
                  player_name, exc)
        return None
    finally:
        conn.close()
