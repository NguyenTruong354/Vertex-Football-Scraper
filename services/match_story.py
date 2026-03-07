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
from typing import Optional
from services.llm_client import LLMClient

log = logging.getLogger(__name__)
llm = LLMClient()

SYSTEM_PROMPT = (
    "Bạn là chuyên gia bình luận bóng đá hàng đầu Việt Nam, "
    "nổi tiếng với lối viết sắc bén, giàu cảm xúc và có chiều sâu chiến thuật.\n"
    "Nhiệm vụ: Viết một đoạn tóm tắt trận đấu ngắn gọn (3-4 câu, tối đa 80 từ) bằng tiếng Việt.\n"
    "Yêu cầu:\n"
    "- Câu mở đầu nêu tỷ số và bối cảnh (ai thắng/thua/hòa)\n"
    "- Câu giữa phân tích lý do (xG, kiểm soát bóng, thẻ đỏ, cầu thủ nổi bật)\n"
    "- Câu cuối đánh giá tổng thể hoặc bất ngờ nếu có\n"
    "- KHÔNG dùng emoji, gạch đầu dòng, hay markdown\n"
    "- Viết liền mạch như một bình luận viên đang kể chuyện"
)


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
    """Generate a story and save it to the match_summaries table."""
    story = generate_story(
        event_id=event_id,
        league=league,
        home_team=home_team,
        away_team=away_team,
        home_score=home_score,
        away_score=away_score,
        statistics=statistics,
        incidents=incidents,
    )
    if not story:
        return False
    
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO match_summaries
                (event_id, league_id, home_team, away_team, home_score, away_score, summary_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                summary_text = EXCLUDED.summary_text,
                loaded_at = NOW()
        """, (event_id, league, home_team, away_team, home_score, away_score, story))
        conn.commit()
        cur.close()
        conn.close()
        log.info("✓ Match story saved to DB for event %d", event_id)
        return True
    except Exception as exc:
        log.error("✗ Failed to save match story: %s", exc)
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
