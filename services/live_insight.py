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

def analyze(
    home_team: str,
    away_team: str,
    minute: int,
    home_score: int,
    away_score: int,
    statistics: Dict[str, Dict[str, Any]],
    incidents: list[dict]
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
        context_trigger = f"{team_down} đang phải chơi thiếu người do thẻ đỏ."
        momentum_score = 80
        
    elif possession_home >= 65:
        context_trigger = f"{home_team} đang áp đảo hoàn toàn với {possession_home}% thời lượng kiểm soát bóng."
        momentum_score = 60 + (possession_home - 65)
    elif possession_away >= 65:
        context_trigger = f"{away_team} đang áp đảo hoàn toàn với {possession_away}% thời lượng kiểm soát bóng."
        momentum_score = 60 + (possession_away - 65)
        
    elif (xg_home - xg_away) >= 1.0:
        context_trigger = f"Chất lượng cơ hội của {home_team} (xG {xg_home:.2f}) hoàn toàn vượt trội so với {away_team} (xG {xg_away:.2f})."
        momentum_score = 75
    elif (xg_away - xg_home) >= 1.0:
        context_trigger = f"Chất lượng cơ hội của {away_team} (xG {xg_away:.2f}) hoàn toàn vượt trội so với {home_team} (xG {xg_home:.2f})."
        momentum_score = 75
        
    elif shots_on_home >= 6 and shots_on_away <= 2:
        context_trigger = f"{home_team} đang sút trúng đích dồn dập ({shots_on_home} lần) uy hiếp khung thành liên tục."
        momentum_score = 70
    elif shots_on_away >= 6 and shots_on_home <= 2:
        context_trigger = f"{away_team} đang sút trúng đích dồn dập ({shots_on_away} lần) uy hiếp khung thành liên tục."
        momentum_score = 70
        
    # If no major momentum swing, return empty
    if momentum_score < 60:
        return 0, ""

    # 2. Build the LLM Prompt
    system_prompt = (
        "Bạn là một chuyên gia phân tích dữ liệu bóng đá trực tiếp (Live Data Analyst). "
        "Dựa vào thống kê trận đấu truyền vào, hãy viết MỘT câu nhận định (tối đa 25 từ) bằng tiếng Anh."
        "Câu này sẽ được hiển thị dạng thẻ Badge 'Live Insight' trên App. "
        "Hãy viết thật thu hút, mạch lạc, vừa có chuyên môn vừa có cảm xúc (không dùng emoji, không gạch đầu dòng)."
    )
    
    user_prompt = f"""
Trận đấu phút {minute}. Tỷ số hiện tại: {home_team} {home_score}-{away_score} {away_team}.
Thống kê chính:
- Tỷ lệ cầm bóng: {home_team} {possession_home}% - {possession_away}% {away_team}
- Sút trúng đích: {home_team} {shots_on_home} - {shots_on_away} {away_team}
- xG (Bàn thắng kỳ vọng): {home_team} {xg_home:.2f} - {xg_away:.2f} {away_team}
- Điểm nhấn trận đấu: {context_trigger}

Viết một câu nhận định siêu ngắn gọn (Dưới 25 chữ).
"""
    
    # 3. Call LLM Service Router
    insight_text = llm.generate_insight(user_prompt, system_instruction=system_prompt)
    if not insight_text:
        # Fallback static text if all LLMs fail
        insight_text = context_trigger

    return momentum_score, insight_text
