"""
AI Insight Pipeline — Worker Layer

Picks queued jobs from ai_insight_jobs using FOR UPDATE SKIP LOCKED,
executes LLM calls, applies post-LLM quality gates (novelty + template diversity),
and updates job status with retry/backoff.
"""

import json
import logging
import os
import pathlib
import random
import re
import time
import asyncio
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Optional

from services.llm_client import LLMClient
from services.agent_orchestrator import AgentOrchestrator

log = logging.getLogger(__name__)

# --- Historical Gate Definitions ---
SKIP_HISTORICAL_LLM = os.environ.get("SKIP_HISTORICAL_LLM", "true").lower() == "true"
HISTORICAL_THRESHOLD_DAYS = int(os.environ.get("HISTORICAL_THRESHOLD_DAYS", "14"))

def _is_historical_match(payload: dict) -> bool:
    if not SKIP_HISTORICAL_LLM:
        return False
    
    ref_time_raw = payload.get("finished_at") or payload.get("kickoff") or payload.get("kickoff_utc")
    if not ref_time_raw:
        return False
    
    # Handle ISO strings vs Datetime objects
    if isinstance(ref_time_raw, str):
        try:
            from dateutil.parser import parse
            ref_time = parse(ref_time_raw)
        except Exception:
            return False
    else:
        ref_time = ref_time_raw

    if ref_time.tzinfo is None:
        ref_time = ref_time.replace(tzinfo=timezone.utc)

    cutoff = datetime.now(timezone.utc) - timedelta(days=HISTORICAL_THRESHOLD_DAYS)
    
    # Season check: Current year or year-1 based on month
    CURRENT_SEASON = datetime.now(timezone.utc).year if datetime.now(timezone.utc).month >= 7 else datetime.now(timezone.utc).year - 1
    season_val = payload.get("season")
    is_old_season = False
    
    if season_val:
        try:
            is_old_season = int(season_val) < CURRENT_SEASON
        except ValueError:
            # handle formats like '2023/24'
            if str(season_val)[:4].isdigit():
                is_old_season = int(str(season_val)[:4]) < CURRENT_SEASON

    return is_old_season or (ref_time < cutoff)
# -----------------------------------

# ── Lease durations (seconds) by job_type ──
LEASE_DURATION = {
    "live_badge": 120,      # 2 minutes
    "match_story": 300,     # 5 minutes
    "player_trend": 300,    # 5 minutes
}

# ── Novelty gate threshold ──
NOVELTY_THRESHOLD = 0.85

# ── Template diversity: phrase bank ──
_PHRASE_BANK_PATH = (
    pathlib.Path(__file__).resolve().parent / "config" / "insight_phrase_bank.json"
)
_phrase_bank: Optional[dict] = None


def _load_phrase_bank() -> dict:
    """Load and cache phrase bank from JSON config."""
    global _phrase_bank
    if _phrase_bank is None:
        with open(_PHRASE_BANK_PATH, encoding="utf-8") as f:
            _phrase_bank = json.load(f)
    return _phrase_bank


# ── Template diversity: constants ──
DIVERSITY_CONSECUTIVE_LIMIT = 2   # cannot repeat in last N consecutive
DIVERSITY_ROLLING_CAP = 2         # max same opening in 30-min window
DIVERSITY_WINDOW_MINUTES = 30

# ── Singleton Orchestrator ──
_orchestrator: Optional[AgentOrchestrator] = None

def init_orchestrator():
    """Explicitly initialize the orchestrator singleton (call on boot)."""
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = AgentOrchestrator(LLMClient())

async def shutdown_orchestrator():
    """Explicitly shutdown the orchestrator and close HTTPX connection pools."""
    global _orchestrator
    if _orchestrator is not None:
        if _orchestrator.llm.async_groq_client_1:
            await _orchestrator.llm.async_groq_client_1.close()
        if _orchestrator.llm.async_groq_client_2:
            await _orchestrator.llm.async_groq_client_2.close()
        _orchestrator = None

def _get_orchestrator() -> AgentOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        init_orchestrator()
    return _orchestrator

def _build_pipeline_context(job_type: str, payload: dict) -> dict:
    stats = {}
    if "xg_home" in payload:
        stats["expected_goals_home"] = payload.get("xg_home", 0)
        stats["expected_goals_away"] = payload.get("xg_away", 0)
        stats["possession_home"] = payload.get("possession_home", 50)
        stats["possession_away"] = payload.get("possession_away", 50)
        stats["goals_home"] = payload.get("home_score", 0)
        stats["goals_away"] = payload.get("away_score", 0)
    elif "statistics" in payload:
        s = payload["statistics"]
        stats["expected_goals_home"] = s.get("Expected goals", {}).get("home", 0)
        stats["expected_goals_away"] = s.get("Expected goals", {}).get("away", 0)
        poss_h = s.get("Ball possession", {}).get("home", "50")
        if isinstance(poss_h, str): poss_h = poss_h.replace('%', '')
        stats["possession_home"] = int(poss_h) if str(poss_h).isdigit() else 50
        stats["goals_home"] = payload.get("home_score", 0)
        stats["goals_away"] = payload.get("away_score", 0)

    momentum = payload.get("momentum_score", 0.5)
    return {
        "stats": stats,
        "incidents": payload.get("incidents", []),
        "standings": {"context": "Ongoing Season"},
        "momentum_score": float(momentum) if str(momentum).replace('.','',1).isdigit() else 0.5
    }


# ════════════════════════════════════════════════════════════
# JOB PICK
# ════════════════════════════════════════════════════════════

def pick_job(league_id: str = None) -> Optional[dict]:
    """Pick one queued job using FOR UPDATE SKIP LOCKED.
    Returns dict with job data or None."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()

        if league_id:
            cur.execute("""
                SELECT id, job_type, event_id, league_id, team_focus,
                       payload_json, prompt_version, attempt_count, max_attempts, priority
                FROM ai_insight_jobs
                WHERE league_id = %s
                  AND status = 'queued'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, (league_id,))
        else:
            cur.execute("""
                SELECT id, job_type, event_id, league_id, team_focus,
                       payload_json, prompt_version, attempt_count, max_attempts, priority
                FROM ai_insight_jobs
                WHERE status = 'queued'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY priority ASC, created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)

        row = cur.fetchone()
        if not row:
            conn.commit()
            return None

        job_id = row[0]
        job_type = row[1]
        lease_sec = LEASE_DURATION.get(job_type, 120)
        worker_id = f"w-{os.getpid()}"

        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'running',
                worker_id = %s,
                lease_until = NOW() + make_interval(secs => %s),
                started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE id = %s
        """, (worker_id, lease_sec, job_id))

        conn.commit()

        payload = row[5] if isinstance(row[5], dict) else json.loads(row[5])

        return {
            "id": job_id,
            "job_type": job_type,
            "event_id": row[2],
            "league_id": row[3],
            "team_focus": row[4],
            "payload": payload,
            "prompt_version": row[6],
            "attempt_count": row[7] + 1,
            "max_attempts": row[8],
            "priority": row[9],
        }
    except Exception as exc:
        conn.rollback()
        log.error("Failed to pick job: %s", exc)
        return None
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# PROMPT BUILDING
# ════════════════════════════════════════════════════════════

def _derive_context_trigger(p: dict) -> str:
    """Derive a context trigger description from payload stats."""
    home = p.get("home_team", "Home")
    away = p.get("away_team", "Away")
    poss_h = p.get("possession_home", 50)
    poss_a = p.get("possession_away", 50)
    xg_h = p.get("xg_home", 0.0)
    xg_a = p.get("xg_away", 0.0)
    sot_h = p.get("shots_on_home", 0)
    sot_a = p.get("shots_on_away", 0)

    # Priority: red cards in incidents > possession > xG > shots
    incidents = p.get("incidents", [])
    for inc in incidents:
        if (inc.get("incidentType") == "card"
                and inc.get("incidentClass") in ("red", "yellowRed")):
            team_down = home if inc.get("isHome") else away
            return f"{team_down} is down to fewer players after a red card."

    if poss_h >= 65:
        return (f"{home} dominating possession with {poss_h}% "
                f"of the ball.")
    if poss_a >= 65:
        return (f"{away} dominating possession with {poss_a}% "
                f"of the ball.")

    if (xg_h - xg_a) >= 1.0:
        return (f"{home}'s chance quality (xG {xg_h:.2f}) far exceeds "
                f"{away} (xG {xg_a:.2f}).")
    if (xg_a - xg_h) >= 1.0:
        return (f"{away}'s chance quality (xG {xg_a:.2f}) far exceeds "
                f"{home} (xG {xg_h:.2f}).")

    if sot_h >= 6 and sot_a <= 2:
        return (f"{home} peppering the goal with {sot_h} shots on target.")
    if sot_a >= 6 and sot_h <= 2:
        return (f"{away} peppering the goal with {sot_a} shots on target.")

    return f"Momentum shift detected in {home} vs {away}."


def _build_live_badge_prompt(payload: dict, version: str = None) -> tuple:
    """Build (system_prompt, user_prompt) for live_badge job."""
    from services.prompt_registry import get_prompt
    system_prompt = get_prompt("live_badge", version)

    ctx = _derive_context_trigger(payload)

    user_prompt = (
        f"Match minute {payload['minute']}. "
        f"Score: {payload['home_team']} {payload['home_score']}-"
        f"{payload['away_score']} {payload['away_team']}.\n"
        f"Stats:\n"
        f"- Possession: {payload['home_team']} "
        f"{payload.get('possession_home', 50)}% - "
        f"{payload.get('possession_away', 50)}% {payload['away_team']}\n"
        f"- Shots on target: {payload['home_team']} "
        f"{payload.get('shots_on_home', 0)} - "
        f"{payload.get('shots_on_away', 0)} {payload['away_team']}\n"
        f"- xG: {payload['home_team']} "
        f"{payload.get('xg_home', 0):.2f} - "
        f"{payload.get('xg_away', 0):.2f} {payload['away_team']}\n"
        f"- Key context: {ctx}\n\n"
        f"Write one ultra-concise insight (under 25 words)."
    )
    return system_prompt, user_prompt


def _format_stats_block(stats: dict) -> str:
    """Format a statistics dict into readable lines for match_story prompt."""
    if not stats:
        return "No detailed statistics available."
    lines = []
    for key in ["Ball possession", "Total shots", "Shots on target",
                "Expected goals", "Corner kicks", "Fouls",
                "Big chances", "Goalkeeper saves"]:
        if key in stats:
            h = stats[key].get("home", "?")
            a = stats[key].get("away", "?")
            lines.append(f"- {key}: {h} vs {a}")
    return "\n".join(lines) if lines else "No detailed statistics available."


def _format_incidents_block(incidents: list, home_team: str) -> str:
    """Format incidents list into readable lines for match_story prompt."""
    if not incidents:
        return "No key events recorded."
    lines = []
    for inc in incidents:
        inc_type = inc.get("incidentType", "")
        minute = inc.get("time", "?")
        player = inc.get("player", {}).get("name", "")
        team = home_team if inc.get("isHome") else "Away"
        if inc_type == "goal":
            lines.append(f"- Goal min {minute}: {player} ({team})")
        elif inc_type == "card":
            card = "Red" if inc.get("incidentClass") == "red" else "Yellow"
            lines.append(f"- {card} card min {minute}: {player} ({team})")
    return "\n".join(lines[:15]) if lines else "No key events recorded."


def _build_match_story_prompt(payload: dict, version: str = None) -> tuple:
    """Build (system_prompt, user_prompt) for match_story job."""
    from services.prompt_registry import get_prompt
    system_prompt = get_prompt("match_story", version)

    home = payload["home_team"]
    away = payload["away_team"]
    hs = payload["home_score"]
    as_ = payload["away_score"]

    if hs > as_:
        result = f"{home} wins against {away}"
    elif as_ > hs:
        result = f"{away} wins against {home}"
    else:
        result = f"{home} draws with {away}"

    stats_block = _format_stats_block(payload.get("statistics", {}))
    incidents_block = _format_incidents_block(
        payload.get("incidents", []), home
    )

    user_prompt = (
        f"Match: {home} {hs}-{as_} {away}\n"
        f"Result: {result}\n\n"
        f"Match statistics:\n{stats_block}\n\n"
        f"Key incidents:\n{incidents_block}\n\n"
        f"Write a brief summary (3-4 sentences, max 80 words)."
    )
    return system_prompt, user_prompt


def _build_player_trend_prompt(payload: dict, version: str = None) -> tuple:
    """Build (system_prompt, user_prompt) for player_trend job."""
    from services.prompt_registry import get_prompt
    system_prompt = get_prompt("player_trend", version)

    name = payload["player_name"]
    trend = payload["trend"]
    n = payload["match_count"]
    goals = payload.get("goals", [])
    assists = payload.get("assists", [])
    xg = payload.get("xg_arr", [])

    total_g = sum(goals)
    total_a = sum(assists)
    total_xg = sum(xg)
    recent_g = sum(goals[-2:]) if len(goals) >= 2 else sum(goals)
    recent_a = sum(assists[-2:]) if len(assists) >= 2 else sum(assists)
    recent_xg = sum(xg[-2:]) if len(xg) >= 2 else sum(xg)

    trend_label = {
        "GREEN": "rising form",
        "RED": "falling form",
        "NEUTRAL": "stable form",
    }.get(trend, "stable form")

    user_prompt = (
        f"Player: {name}\n"
        f"Form: {trend_label}\n"
        f"Stats last {n} matches: {total_g} goals, {total_a} assists, "
        f"total xG {total_xg:.2f}\n"
        f"Last 2 matches: {recent_g} goals, {recent_a} assists, "
        f"xG {recent_xg:.2f}\n\n"
        f"Write ONE concise comment (under 20 words)."
    )
    return system_prompt, user_prompt


# ════════════════════════════════════════════════════════════
# POST-LLM GATES
# ════════════════════════════════════════════════════════════

def _novelty_gate(event_id: int, job_type: str,
                  candidate_text: str) -> bool:
    """Return True if candidate is novel enough to publish.
    False = too similar to recent published insights."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT COALESCE(result_text_en, result_text)
            FROM ai_insight_jobs
            WHERE event_id = %s
              AND job_type = %s
              AND status = 'succeeded'
              AND is_published = TRUE
              AND finished_at IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 5
        """, (event_id, job_type))

        for (prev_text,) in cur.fetchall():
            if not prev_text:
                continue
            sim = SequenceMatcher(
                None, candidate_text.lower(), prev_text.lower()
            ).ratio()
            if sim >= NOVELTY_THRESHOLD:
                log.info("Novelty gate BLOCKED (sim=%.2f >= %.2f)",
                         sim, NOVELTY_THRESHOLD)
                return False
        return True
    except Exception as exc:
        log.warning("Novelty gate error: %s", exc)
        return True  # fail-open
    finally:
        conn.close()


def _extract_opening_phrase(text: str, n_tokens: int = 5) -> str:
    """Extract first N tokens from text after normalization (lowercase, strip punctuation)."""
    normalized = re.sub(r'[^\w\s]', '', text.lower()).strip()
    tokens = normalized.split()[:n_tokens]
    return " ".join(tokens)


def _template_diversity_gate(event_id: int, job_type: str,
                             candidate_text: str) -> bool:
    """Return True if candidate passes template diversity rules.
    False = repeated opening phrase blocked.

    Rules per plan §7.4:
    - Opening phrase (first 5 tokens) cannot repeat in 2 consecutive
      published insights for the same event.
    - Same opening phrase max 2 times per 30-minute event window.
    """
    from db.config_db import get_connection

    candidate_opening = _extract_opening_phrase(candidate_text)
    if not candidate_opening:
        return True  # no opening phrase → pass

    conn = get_connection()
    try:
        cur = conn.cursor()

        # 1. Consecutive repeat check: last N published insights
        cur.execute("""
            SELECT COALESCE(result_text_en, result_text)
            FROM ai_insight_jobs
            WHERE event_id = %s
              AND job_type = %s
              AND status = 'succeeded'
              AND is_published = TRUE
              AND finished_at IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT %s
        """, (event_id, job_type, DIVERSITY_CONSECUTIVE_LIMIT))

        recent_openings = []
        for (prev_text,) in cur.fetchall():
            if prev_text:
                recent_openings.append(_extract_opening_phrase(prev_text))

        # Block if ALL recent openings match candidate (consecutive repeat)
        if (len(recent_openings) >= DIVERSITY_CONSECUTIVE_LIMIT
                and all(op == candidate_opening for op in recent_openings)):
            log.info("Template diversity BLOCKED: consecutive repeat '%s'",
                     candidate_opening)
            return False

        # 2. Rolling 30-minute cap check
        cur.execute("""
            SELECT result_text
            FROM ai_insight_jobs
            WHERE event_id = %s
              AND job_type = %s
              AND status = 'succeeded'
              AND is_published = TRUE
              AND finished_at > NOW() - make_interval(mins => %s)
            ORDER BY finished_at DESC
        """, (event_id, job_type, DIVERSITY_WINDOW_MINUTES))

        window_count = 0
        for (prev_text,) in cur.fetchall():
            if prev_text and _extract_opening_phrase(prev_text) == candidate_opening:
                window_count += 1

        if window_count >= DIVERSITY_ROLLING_CAP:
            log.info("Template diversity BLOCKED: rolling cap %d/%d "
                     "for '%s' in %d-min window",
                     window_count, DIVERSITY_ROLLING_CAP,
                     candidate_opening, DIVERSITY_WINDOW_MINUTES)
            return False

        return True
    except Exception as exc:
        log.warning("Template diversity gate error: %s", exc)
        return True  # fail-open
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# JOB EXECUTION
# ════════════════════════════════════════════════════════════

async def execute_job(job: dict, *, shadow_mode: bool = True) -> bool:
    """Execute a picked job: Agent calls + post-gates + status update.
    Returns True when job reaches terminal state."""
    job_id = job["id"]
    job_type = job["job_type"]
    event_id = job["event_id"]
    payload = job["payload"]

    t0 = time.monotonic()

    try:
        # 1. Historical Gate
        if job_type in ["live_badge", "match_story"] and _is_historical_match(payload):
            log.info("Skipping historical match %s", event_id)
            await asyncio.to_thread(_drop_job, job_id, "historical_match", "Skipped by historical gate", "", "", 0)
            return True

        # 2. Build Pipeline Context
        ctx = _build_pipeline_context(job_type, payload)
        
        # 3. Call Multi-Agent Orchestrator
        orchestrator = _get_orchestrator()
        priority = job.get("priority", 100) # Use priority from job
        result_dict = await orchestrator.run_pipeline(job_type, ctx, priority=priority)
        latency_ms = int((time.monotonic() - t0) * 1000)

        if not result_dict:
            await asyncio.to_thread(_retry_or_fail, job, "llm_empty", "All LLM agents failed or rejected verification")
            return True

        text_en = result_dict.get("final_insight_en") or result_dict.get("dominant_narrative", "")
        text_vi = result_dict.get("final_insight_vi", "")
        result_text = json.dumps(result_dict)

        # 4. Post-LLM gate: Novelty
        if job_type == "live_badge" and event_id is not None:
            novelty_pass = await asyncio.to_thread(_novelty_gate, event_id, job_type, text_en)
            if not novelty_pass:
                await asyncio.to_thread(_drop_job, job_id, "near_duplicate", result_text, text_en, text_vi, latency_ms)
                return True

        # 5. Post-LLM gate: Template diversity
        if job_type == "live_badge" and event_id is not None:
            diversity_pass = await asyncio.to_thread(_template_diversity_gate, event_id, job_type, text_en)
            if not diversity_pass:
                await asyncio.to_thread(_drop_job, job_id, "template_repeat", result_text, text_en, text_vi, latency_ms)
                return True

        # 6. Success — update job row
        is_published = not shadow_mode
        
        def db_success_update():
            from db.config_db import get_connection
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute("""
                    UPDATE ai_insight_jobs
                    SET status = 'succeeded',
                        result_text = %s,
                        result_text_en = %s,
                        result_text_vi = %s,
                        is_published = %s,
                        published_at = CASE WHEN %s THEN NOW() ELSE NULL END,
                        latency_ms = %s,
                        finished_at = NOW(),
                        lease_until = NULL,
                        worker_id = NULL
                    WHERE id = %s
                """, (result_text, text_en, text_vi, is_published, is_published, latency_ms, job_id))

                if is_published:
                    _write_destination(cur, job_type, job, result_text, text_en, text_vi)

                conn.commit()
            finally:
                conn.close()

            if is_published and text_en:
                try:
                    _send_insight_embed(job_type, job, text_en, text_vi, latency_ms)
                except Exception as exc:
                    log.debug("Discord embed send error: %s", exc)

        await asyncio.to_thread(db_success_update)

        log.info("Job %d succeeded: %s latency=%dms published=%s", job_id, job_type, latency_ms, is_published)
        return True

    except Exception as exc:
        log.error("Job %d execution error: %s", job_id, exc)
        await asyncio.to_thread(_retry_or_fail, job, "execution_error", str(exc))
        return True

# ════════════════════════════════════════════════════════════
# DISCORD EMBED FOR INSIGHTS
# ════════════════════════════════════════════════════════════

def _send_insight_embed(job_type: str, job: dict,
                        text_en: str, text_vi: str,
                        latency_ms: int) -> None:
    """Send a Rich Embed to Discord after insight is saved to DB."""
    import os, json, urllib.request, urllib.error

    webhook_url = (
        os.environ.get("DISCORD_WEBHOOK_LIVE")
        or os.environ.get("DISCORD_WEBHOOK")
        or ""
    )
    if not webhook_url:
        return

    payload_data = job.get("payload", {})
    league_id = job.get("league_id", "?")
    event_id = job.get("event_id", "")
    pv = job.get("prompt_version", "v1")

    # Build title based on job type
    TYPE_LABELS = {
        "live_badge": "⚡ Live Insight",
        "match_story": "📖 Match Story",
        "player_trend": "📈 Player Trend",
    }
    title = TYPE_LABELS.get(job_type, job_type)

    # Sub-title context
    if job_type == "live_badge":
        home = payload_data.get("home_team", "?")
        away = payload_data.get("away_team", "?")
        title += f" | {home} vs {away}"
    elif job_type == "match_story":
        home = payload_data.get("home_team", "?")
        away = payload_data.get("away_team", "?")
        hs = payload_data.get("home_score", "?")
        as_ = payload_data.get("away_score", "?")
        title += f" | {home} {hs}-{as_} {away}"
    elif job_type == "player_trend":
        name = payload_data.get("player_name", "?")
        trend = payload_data.get("trend", "?")
        title += f" | {name} ({trend})"

    embed = {
        "title": f"🤖 {title}",
        "color": 0xFFA500,
        "fields": [
            {"name": "🇺🇸 English", "value": text_en or "(empty)", "inline": False},
            {"name": "🇻🇳 Tiếng Việt", "value": text_vi or "(empty)", "inline": False},
        ],
        "footer": {
            "text": f"League: {league_id} | Prompt: {pv} | Latency: {latency_ms}ms"
        },
    }

    data = json.dumps({"embeds": [embed]}).encode()
    try:
        req = urllib.request.Request(
            webhook_url, data=data,
            headers={"Content-Type": "application/json", "User-Agent": "VertexWorker/2.0"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10)
    except urllib.error.HTTPError as e:
        if e.code == 429:
            try:
                body = json.loads(e.read().decode())
                retry_after = body.get("retry_after", 5)
            except Exception:
                retry_after = 5
            import time
            time.sleep(retry_after)
            urllib.request.urlopen(
                urllib.request.Request(
                    webhook_url, data=data,
                    headers={"Content-Type": "application/json", "User-Agent": "VertexWorker/2.0"},
                    method="POST",
                ),
                timeout=10,
            )
    except Exception:
        pass  # fail-silent


# ════════════════════════════════════════════════════════════
# DESTINATION TABLE WRITES
# ════════════════════════════════════════════════════════════

def _write_destination(cur, job_type: str, job: dict,
                       result_text: str, text_en: str, text_vi: str) -> None:
    """Write LLM result to the appropriate destination table.
    Called within the same transaction as the job status update."""
    payload = job["payload"]

    if job_type == "match_story":
        cur.execute("""
            INSERT INTO match_summaries
                (event_id, league_id, home_team, away_team,
                 home_score, away_score, summary_text,
                 summary_text_en, summary_text_vi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                summary_text = EXCLUDED.summary_text,
                summary_text_en = EXCLUDED.summary_text_en,
                summary_text_vi = EXCLUDED.summary_text_vi,
                loaded_at = NOW()
        """, (
            job["event_id"], job["league_id"],
            payload["home_team"], payload["away_team"],
            payload["home_score"], payload["away_score"],
            result_text, text_en, text_vi
        ))
        log.info("Job %d: wrote match_summaries event=%d",
                 job["id"], job["event_id"])

    elif job_type == "player_trend":
        cur.execute("""
            INSERT INTO player_insights
                (player_id, player_name, league_id, trend,
                 trend_score, insight_text, insight_text_en, insight_text_vi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (player_id, league_id) DO UPDATE SET
                player_name = EXCLUDED.player_name,
                trend = EXCLUDED.trend,
                trend_score = EXCLUDED.trend_score,
                insight_text = EXCLUDED.insight_text,
                insight_text_en = EXCLUDED.insight_text_en,
                insight_text_vi = EXCLUDED.insight_text_vi,
                loaded_at = NOW()
        """, (
            payload["player_id"], payload["player_name"],
            job["league_id"], payload["trend"],
            payload["trend_score"], result_text, text_en, text_vi
        ))
        log.info("Job %d: wrote player_insights player=%s",
                 job["id"], payload["player_name"])

    # live_badge: no separate destination table (result stays in ai_insight_jobs)


# ════════════════════════════════════════════════════════════
# STATUS HELPERS
# ════════════════════════════════════════════════════════════

def _drop_job(job_id: int, reason: str,
              result_text: str, text_en: str, text_vi: str, latency_ms: int) -> None:
    """Mark job as dropped with reason."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'dropped',
                reason_code = %s,
                result_text = %s,
                result_text_en = %s,
                result_text_vi = %s,
                latency_ms = %s,
                finished_at = NOW(),
                lease_until = NULL,
                worker_id = NULL
            WHERE id = %s
        """, (reason, result_text, text_en, text_vi, latency_ms, job_id))
        conn.commit()
        log.info("Job %d dropped: reason=%s", job_id, reason)
    except Exception as exc:
        conn.rollback()
        log.error("Failed to drop job %d: %s", job_id, exc)
    finally:
        conn.close()


def _fail_job(job_id: int, error_code: str, error_message: str) -> None:
    """Mark job as failed."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'failed',
                error_code = %s,
                error_message = %s,
                finished_at = NOW(),
                lease_until = NULL,
                worker_id = NULL
            WHERE id = %s
        """, (error_code, error_message[:500], job_id))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.error("Failed to mark job %d as failed: %s", job_id, exc)
    finally:
        conn.close()


def _retry_or_fail(job: dict, error_code: str, error_message: str) -> None:
    """Retry with exponential backoff if attempts remain, else fail."""
    from db.config_db import get_connection
    job_id = job["id"]
    attempt = job["attempt_count"]
    max_attempts = job["max_attempts"]

    if attempt >= max_attempts:
        _fail_job(job_id, error_code, error_message)
        return

    # Exponential backoff: 30s, 60s, 120s... + jitter
    base_delay = 30 * (2 ** (attempt - 1))
    jitter = random.uniform(0, base_delay * 0.3)
    delay = base_delay + jitter

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'queued',
                error_code = %s,
                error_message = %s,
                next_retry_at = NOW() + make_interval(secs => %s),
                lease_until = NULL,
                worker_id = NULL
            WHERE id = %s
        """, (error_code, error_message[:500], delay, job_id))
        conn.commit()
        log.info("Job %d retry %d/%d in %.0fs",
                 job_id, attempt, max_attempts, delay)
    except Exception as exc:
        conn.rollback()
        log.error("Failed to retry job %d: %s", job_id, exc)
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# LEASE RECOVERY
# ════════════════════════════════════════════════════════════

def recover_expired_leases() -> int:
    """Recover expired running jobs. Returns count of recovered jobs."""
    from db.config_db import get_connection
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Requeue expired with attempts left
        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'queued',
                worker_id = NULL,
                lease_until = NULL,
                next_retry_at = NOW() + INTERVAL '30 seconds'
            WHERE status = 'running'
              AND lease_until < NOW()
              AND attempt_count < max_attempts
        """)
        requeued = cur.rowcount

        # Fail expired with no attempts left
        cur.execute("""
            UPDATE ai_insight_jobs
            SET status = 'failed',
                reason_code = 'lease_expired',
                finished_at = NOW(),
                lease_until = NULL,
                worker_id = NULL
            WHERE status = 'running'
              AND lease_until < NOW()
              AND attempt_count >= max_attempts
        """)
        failed = cur.rowcount

        conn.commit()

        if requeued or failed:
            log.info("Lease recovery: requeued=%d failed=%d",
                     requeued, failed)
        return requeued + failed
    except Exception as exc:
        conn.rollback()
        log.error("Lease recovery failed: %s", exc)
        return 0
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════
# PUBLIC: WORKER CYCLE
# ════════════════════════════════════════════════════════════

async def run_worker_cycle(league_id: str = None, *,
                     shadow_mode: bool = True,
                     max_jobs: int = 20,
                     shutdown_event: asyncio.Event = None) -> int:
    """Run one async worker cycle: recover leases + process available jobs.
    Returns number of jobs processed."""
    orchestrator = _get_orchestrator()
    can_work = False
    
    if orchestrator.llm.async_groq_client_1 and orchestrator.llm.cb_groq_1.can_execute():
        can_work = True
    elif orchestrator.llm.async_groq_client_2 and orchestrator.llm.cb_groq_2.can_execute():
        can_work = True

    if not can_work:
        await asyncio.to_thread(recover_expired_leases)
        return 0

    await asyncio.to_thread(recover_expired_leases)

    processed = 0
    while processed < max_jobs:
        if shutdown_event and shutdown_event.is_set():
            log.info("⏹ AI Worker loop interrupted by shutdown event")
            break
            
        job = await asyncio.to_thread(pick_job, league_id)
        if not job:
            break
        await execute_job(job, shadow_mode=shadow_mode)
        processed += 1

    return processed

