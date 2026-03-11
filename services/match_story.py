"""
Vertex Football - 30-Second Match Story Generator

After a match ends, this module queries the database for match stats
(Understat xG, SofaScore events, live_snapshots) and sends a structured
prompt to the LLM to generate a 3-4 sentence narrative summary in Vietnamese.

Usage:
    from match_story import generate_story
    story = generate_story(event_id=14023980, league="EPL")
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Thêm project root vào sys.path để nhận diện 'services' package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.llm_client import LLMClient

log = logging.getLogger(__name__)
llm = LLMClient()

from services.prompt_registry import get_prompt
SYSTEM_PROMPT = get_prompt("match_story")


def generate_story(
    event_id: int,
    league: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    statistics: Optional[dict] = None,
    incidents: Optional[list] = None,
) -> str:
    """
    Generate a 30-second match story using LLM.
    
    Args:
        event_id: SofaScore event ID
        league: League code (EPL, LALIGA, etc.)
        home_team / away_team: Team names
        home_score / away_score: Final scores
        statistics: Dict from live_snapshots.statistics_json (if available)
        incidents: List from live_snapshots.incidents_json (if available)
    
    Returns:
        A narrative summary string, or "" if generation fails.
    """
    # Build context from available data
    stats_block = _format_statistics(statistics) if statistics else "Không có dữ liệu thống kê chi tiết."
    incidents_block = _format_incidents(incidents, home_team) if incidents else "Không có dữ liệu sự kiện."

    # Determine match result
    if home_score > away_score:
        result = f"{home_team} thắng {away_team}"
    elif away_score > home_score:
        result = f"{away_team} thắng {home_team}"
    else:
        result = f"{home_team} hòa {away_team}"

    user_prompt = f"""Trận đấu {league}: {home_team} {home_score}-{away_score} {away_team}
Kết quả: {result}

Thống kê trận đấu:
{stats_block}

Sự kiện chính:
{incidents_block}

Hãy viết tóm tắt ngắn gọn (3-4 câu, tối đa 80 từ)."""

    log.info("📝 Generating match story for %s vs %s...", home_team, away_team)
    story = llm.generate_insight(user_prompt, system_instruction=SYSTEM_PROMPT)

    if story:
        log.info("✓ Match story generated (%d chars)", len(story))
    else:
        log.warning("✗ Failed to generate match story for event %d", event_id)
    
    return story


def generate_and_save(
    event_id: int,
    league: str,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    statistics: Optional[dict] = None,
    incidents: Optional[list] = None,
) -> bool:
    """Enqueue a match story job to AI pipeline."""
    from services.insight_producer import enqueue_match_story
    job_id = enqueue_match_story(
        event_id=event_id,
        league_id=league,
        home_team=home_team,
        away_team=away_team,
        home_score=home_score,
        away_score=away_score,
        statistics=statistics or {},
        incidents=incidents or [],
    )
    if job_id:
        log.info("✓ Match story generation enqueued for event %d (job=%d)", event_id, job_id)
        return True
    
    log.warning("✗ Failed or skipped enqueueing match story for event %d", event_id)
    return False


def _format_statistics(stats: dict) -> str:
    """Format statistics dict into human-readable text for the LLM prompt."""
    lines = []
    key_stats = [
        "Ball possession", "Total shots", "Shots on target",
        "Expected goals", "Corner kicks", "Fouls", "Offsides",
        "Big chances", "Big chances missed",
        "Goalkeeper saves", "Passes", "Accurate passes"
    ]
    for key in key_stats:
        if key in stats:
            home_val = stats[key].get("home", "?")
            away_val = stats[key].get("away", "?")
            lines.append(f"- {key}: {home_val} vs {away_val}")
    return "\n".join(lines) if lines else "Không có dữ liệu."


def _format_incidents(incidents: list, home_team: str) -> str:
    """Format incidents list into human-readable text for the LLM prompt."""
    lines = []
    for inc in incidents:
        inc_type = inc.get("incidentType", "")
        minute = inc.get("time", "?")
        player = inc.get("player", {}).get("name", "")
        is_home = inc.get("isHome", False)
        team = home_team if is_home else "Đội khách"
        
        if inc_type == "goal":
            lines.append(f"- Bàn thắng phút {minute}: {player} ({team})")
        elif inc_type == "card":
            card_class = inc.get("incidentClass", "")
            card_label = "Thẻ đỏ" if card_class == "red" else "Thẻ vàng"
            lines.append(f"- {card_label} phút {minute}: {player} ({team})")
        elif inc_type == "varDecision":
            lines.append(f"- VAR phút {minute} ({team})")
    return "\n".join(lines) if lines else "Không có sự kiện đáng chú ý."


if __name__ == "__main__":
    # Quick Test Block
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    print("\n" + "="*60)
    print("      VERTEX FOOTBALL - MATCH STORY QUICK TEST")
    print("="*60)
    
    # Mock data: Liverpool vs Man City
    mock_stats = {
        "Ball possession": {"home": 45, "away": 55},
        "Total shots": {"home": 12, "away": 18},
        "Shots on target": {"home": 5, "away": 8},
        "Expected goals": {"home": 1.45, "away": 2.15},
        "Big chances": {"home": 2, "away": 4},
    }
    mock_incidents = [
        {"time": 23, "incidentType": "goal", "player": {"name": "Haaland"}, "isHome": False},
        {"time": 45, "incidentType": "goal", "player": {"name": "Salah"}, "isHome": True},
        {"time": 68, "incidentType": "card", "incidentClass": "red", "player": {"name": "Rodri"}, "isHome": False},
        {"time": 89, "incidentType": "goal", "player": {"name": "Luis Diaz"}, "isHome": True},
    ]
    
    story = generate_story(
        event_id=999,
        league="EPL",
        home_team="Liverpool",
        away_team="Man City",
        home_score=2,
        away_score=1,
        statistics=mock_stats,
        incidents=mock_incidents
    )
    
    print("-" * 60)
    if story:
        print(f"✨ AI MATCH STORY:\n{story}")
    else:
        print("❌ Test failed.")
    print("="*60 + "\n")
