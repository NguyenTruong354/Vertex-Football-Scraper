"""
Vertex Football - Live Match Insight Engine

Analyzes raw SofaScore statistics and context to determine if a match has a notable
"Momentum" swing (e.g. high possession, red cards, shot storms).
If a threshold is met, it uses llm_client to generate a punchy, Vietnamese broadcast-style insight.
"""

import logging
from typing import Tuple, Dict, Any, Optional
from services.llm_client import LLMClient

log = logging.getLogger(__name__)

# Initialize LLM Client once (re-used across analyses)
llm = LLMClient()

def _safe_int(val: Any) -> int:
    try:
        if isinstance(val, str):
            val = val.replace('%', '').strip()
        return int(val)
    except (ValueError, TypeError):
        return 0

def _get_last_published_insight(event_id: int) -> str | None:
    """
    Lấy insight text của live_badge job được publish gần nhất cho event này.
    Dùng để inject vào prompt tránh lặp nội dung.
    Returns insight_text string hoặc None nếu không có.
    """
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT result_text
            FROM ai_insight_jobs
            WHERE event_id = %s
              AND job_type = 'live_badge'
              AND status = 'succeeded'
              AND is_published = TRUE
            ORDER BY published_at DESC
            LIMIT 1
        """, (event_id,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        return None

def analyze(
    home_team: str,
    away_team: str,
    minute: int,
    home_score: int,
    away_score: int,
    statistics: Dict[str, Dict[str, Any]],
    incidents: list[dict],
    event_id: int | None = None,
) -> Tuple[int, str]:
    """
    Analyzes match state and returns a (momentum_score, insight_text) tuple.
    Returns (0, "") if the match is too early or boring to warrant an insight.
    """
    
    # Don't generate too early
    if minute < 15:
        return 0, ""

    # Parse key stats from SofaScore format
    possession_home = _safe_int(statistics.get("Ball possession", {}).get("home", 50))
    possession_away = _safe_int(statistics.get("Ball possession", {}).get("away", 50))
    
    shots_home = _safe_int(statistics.get("Total shots", {}).get("home", 0))
    shots_away = _safe_int(statistics.get("Total shots", {}).get("away", 0))
    
    shots_on_home = _safe_int(statistics.get("Shots on target", {}).get("home", 0))
    shots_on_away = _safe_int(statistics.get("Shots on target", {}).get("away", 0))
    
    xg_home = float(statistics.get("Expected goals", {}).get("home", 0.0))
    xg_away = float(statistics.get("Expected goals", {}).get("away", 0.0))
    
    corners_home = _safe_int(statistics.get("Corner kicks", {}).get("home", 0))
    corners_away = _safe_int(statistics.get("Corner kicks", {}).get("away", 0))

    # Check for red cards
    home_reds = sum(1 for i in incidents if i.get("incidentType") == "card" and i.get("incidentClass") == "red" and i.get("isHome") is True)
    away_reds = sum(1 for i in incidents if i.get("incidentType") == "card" and i.get("incidentClass") == "red" and i.get("isHome") is False)

    # 1. Determine Momentum Triggers
    context_trigger = ""
    momentum_score = 0
    
    if home_reds > 0 or away_reds > 0:
        team_down = home_team if home_reds > 0 else away_team
        context_trigger = f"{team_down} is playing with fewer players due to a red card."
        momentum_score = 80
        
    elif possession_home >= 65:
        context_trigger = f"{home_team} is completely dominating with {possession_home}% ball possession."
        momentum_score = 60 + (possession_home - 65)
    elif possession_away >= 65:
        context_trigger = f"{away_team} is completely dominating with {possession_away}% ball possession."
        momentum_score = 60 + (possession_away - 65)
        
    elif (xg_home - xg_away) >= 1.0:
        context_trigger = f"Chance quality of {home_team} (xG {xg_home:.2f}) is vastly superior to {away_team} (xG {xg_away:.2f})."
        momentum_score = 75
    elif (xg_away - xg_home) >= 1.0:
        context_trigger = f"Chance quality of {away_team} (xG {xg_away:.2f}) is vastly superior to {home_team} (xG {xg_home:.2f})."
        momentum_score = 75
        
    elif shots_on_home >= 6 and shots_on_away <= 2:
        context_trigger = f"{home_team} is continuously threatening the goal with rapid shots on target ({shots_on_home} shots)."
        momentum_score = 70
    elif shots_on_away >= 6 and shots_on_home <= 2:
        context_trigger = f"{away_team} is continuously threatening the goal with rapid shots on target ({shots_on_away} shots)."
        momentum_score = 70
        
    # If no major momentum swing, return empty
    if momentum_score < 60:
        return 0, ""

    from services.prompt_registry import get_prompt
    system_prompt = get_prompt("live_badge")
    
    # Lấy insight trước đó để tránh lặp
    last_insight = _get_last_published_insight(event_id)
    avoid_repeat_block = (
        f"\nPREVIOUSLY PUBLISHED insight: \"{last_insight}\"\n"
        f"DO NOT repeat this idea. Emphasize a DIFFERENT aspect."
        if last_insight else ""
    )
    
    user_prompt = f"""
Match minute {minute}. Current score: {home_team} {home_score}-{away_score} {away_team}.
Key statistics:
- Ball possession: {home_team} {possession_home}% - {possession_away}% {away_team}
- Shots on target: {home_team} {shots_on_home} - {shots_on_away} {away_team}
- xG (Expected Goals): {home_team} {xg_home:.2f} - {xg_away:.2f} {away_team}
- Match highlight: {context_trigger}
{avoid_repeat_block}
Write a super concise insight (Under 25 words).
"""
    
    # 3. Call LLM Service Router
    from services.text_utils import clean_insight
    insight_text = llm.generate_insight(user_prompt, system_instruction=system_prompt)
    if not insight_text:
        insight_text = context_trigger
    else:
        insight_text = clean_insight(insight_text, max_sentences=1)

    return momentum_score, insight_text
