"""
Vertex Football - Player Performance Trend Analyzer

Runs as a nightly job (e.g. 4 AM) on the e2-micro VM.
Queries the last 5 matches of each player from `player_match_stats`,
calculates a trend direction (GREEN = rising, RED = falling, NEUTRAL = stable),
and generates a short AI-written explanation saved to `player_insights`.
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
SYSTEM_PROMPT = get_prompt("player_trend")


def analyze_all_players(league: str, force: bool = False) -> list[dict]:
    """
    Analyze the trend of all players in a league and return a list of insights.
    
    Returns:
        List of dicts: [{player_id, player_name, league_id, trend, score, insight_text}, ...]
    """
    try:
        from db.config_db import get_connection
        conn = get_connection()
        cur = conn.cursor()

        # Step 1: Early Exit Gate (No active matches)
        if not force:
            cur.execute("""
                SELECT COUNT(*) FROM match_stats 
                WHERE TO_TIMESTAMP(datetime_str, 'YYYY-MM-DD HH24:MI:SS') >= NOW() - INTERVAL '5 days'
                  AND league_id = %s
            """, (league,))
            if cur.fetchone()[0] == 0:
                log.info("🚫 No active matches in the last 5 days for %s. Skipping player trend analysis.", league)
                cur.close()
                conn.close()
                return []

        # Step 2: Fetch Existing Insights for Dirty Check
        cur.execute("SELECT player_id, trend, trend_score FROM player_insights WHERE league_id = %s", (league,))
        existing_insights = {row[0]: {"trend": row[1], "score": float(row[2])} for row in cur.fetchall()}

        # Step 3: Fetch Player Stats with Participation Filter
        active_cte = ""
        if not force:
            active_cte = """
            active_matches AS (
                SELECT match_id FROM match_stats 
                WHERE TO_TIMESTAMP(datetime_str, 'YYYY-MM-DD HH24:MI:SS') >= NOW() - INTERVAL '5 days'
            ),
            active_players AS (
                SELECT DISTINCT player_id
                FROM player_match_stats
                WHERE match_id IN (SELECT match_id FROM active_matches)
                  AND "time" > 0
            ),
            """

        sql = f"""
            WITH {active_cte}
            ranked AS (
                SELECT
                    pms.player_id, pms.player, pms.match_id,
                    pms.goals, pms.assists, pms.xg, pms.xa, pms.xg_chain, pms.shots, pms.key_passes, pms."time",
                    ROW_NUMBER() OVER (
                        PARTITION BY pms.player_id
                        ORDER BY pms.match_id DESC
                    ) AS rn
                FROM player_match_stats pms
                {"INNER JOIN active_players ap ON pms.player_id = ap.player_id" if not force else ""}
                WHERE pms.league_id = %(league)s AND pms."time" >= 30
            )
            SELECT player_id, player,
                   array_agg(goals ORDER BY rn ASC) AS goals_arr,
                   array_agg(assists ORDER BY rn ASC) AS assists_arr,
                   array_agg(xg ORDER BY rn ASC) AS xg_arr,
                   array_agg(xa ORDER BY rn ASC) AS xa_arr,
                   array_agg(shots ORDER BY rn ASC) AS shots_arr,
                   array_agg(key_passes ORDER BY rn ASC) AS kp_arr,
                   COUNT(*) AS match_count
            FROM ranked
            WHERE rn <= 5
            GROUP BY player_id, player
            HAVING COUNT(*) >= 3
            ORDER BY player
        """
        cur.execute(sql, {"league": league})

        rows = cur.fetchall()
        cur.close()
        conn.close()

        log.info("📊 Analyzing trends for %d players in %s...", len(rows), league)

        # Phase 1: Calculate trends for ALL players (pure math, no LLM)
        all_players = []
        for row in rows:
            player_id, player_name = row[0], row[1]
            goals = list(row[2])
            assists = list(row[3])
            xg_arr = [float(x or 0) for x in row[4]]
            xa_arr = [float(x or 0) for x in row[5]]
            shots = list(row[6])
            key_passes = list(row[7])
            match_count = row[8]

            trend, score = _calculate_trend(goals, assists, xg_arr, xa_arr, shots, key_passes)

            # Step 4: Dirty Check (Skip unchanged players)
            if not force and player_id in existing_insights:
                old = existing_insights[player_id]
                if old["trend"] == trend and abs(old["score"] - score) < 0.01:
                    continue  # Skip: Structural data identical

            all_players.append({
                "player_id": player_id,
                "player_name": player_name,
                "league_id": league,
                "trend": trend,
                "score": score,
                "goals": goals,
                "assists": assists,
                "xg_arr": xg_arr,
                "xa_arr": xa_arr,
                "match_count": match_count,
                "insight_text": "",
            })

        if not all_players:
             log.info("⚡ All player trends are up-to-date for %s (0 changes)", league)
             return []

        # Phase 2: Enqueue top players vào queue thay vì gọi LLM trực tiếp
        LLM_BUDGET = 15
        green_top = sorted(
            [p for p in all_players if p["trend"] == "GREEN"],
            key=lambda x: -x["score"]
        )[:LLM_BUDGET]
        red_top = sorted(
            [p for p in all_players if p["trend"] == "RED"],
            key=lambda x: x["score"]
        )[:LLM_BUDGET]
        llm_candidates = {p["player_id"] for p in green_top + red_top}

        log.info("  📥 Enqueueing %d notable players into job queue...", len(llm_candidates))

        from services.insight_producer import enqueue_player_trend_batch

        enqueued_count = 0
        insights = []
        total_players = len(all_players)
        
        batch_candidates = []
        for idx, p in enumerate(all_players):
            if (idx + 1) % 100 == 0:
                log.info("    ... processed %d/%d players", idx + 1, total_players)
                
            # Với notable players: collect into batch
            if p["player_id"] in llm_candidates:
                batch_candidates.append(p)
                p["insight_text"] = _static_insight(p)
            else:
                p["insight_text"] = _static_insight(p)

            insights.append({
                "player_id": p["player_id"],
                "player_name": p["player_name"],
                "league_id": p["league_id"],
                "trend": p["trend"],
                "score": p["score"],
                "insight_text": p["insight_text"],
            })

        if batch_candidates:
            enqueued_count = enqueue_player_trend_batch(batch_candidates)

        log.info("  ✓ Enqueued %d player_trend jobs for async LLM processing", enqueued_count)
        return insights

    except Exception as exc:
        log.error("Failed to analyze player trends: %s", exc)
        return []


def _calculate_trend(
    goals: list, assists: list,
    xg: list, xa: list,
    shots: list, key_passes: list
) -> tuple[str, int]:
    """
    Calculate trend direction and momentum score.
    
    Compares the average of the last 2 matches vs the first 2-3 matches.
    Returns: (trend: "GREEN" | "RED" | "NEUTRAL", score: -100 to 100)
    """
    n = len(goals)
    if n < 3:
        return "NEUTRAL", 0

    # Split into "recent" (last 2) vs "earlier" (first matches)
    split = max(1, n - 2)
    
    def avg(arr, start, end):
        chunk = arr[start:end]
        return sum(chunk) / max(len(chunk), 1)

    # Weighted composite score: xG is most important indicator
    recent_score = (
        avg(xg, split, n) * 4.0 +
        avg(goals, split, n) * 3.0 +
        avg(xa, split, n) * 2.0 +
        avg(assists, split, n) * 2.0 +
        avg(key_passes, split, n) * 1.0 +
        avg(shots, split, n) * 0.5
    )
    earlier_score = (
        avg(xg, 0, split) * 4.0 +
        avg(goals, 0, split) * 3.0 +
        avg(xa, 0, split) * 2.0 +
        avg(assists, 0, split) * 2.0 +
        avg(key_passes, 0, split) * 1.0 +
        avg(shots, 0, split) * 0.5
    )

    if earlier_score == 0:
        delta_pct = 100 if recent_score > 0 else 0
    else:
        delta_pct = int(((recent_score - earlier_score) / earlier_score) * 100)

    # Clamp
    delta_pct = max(-100, min(100, delta_pct))

    if delta_pct >= 15:
        trend = "GREEN"
    elif delta_pct <= -15:
        trend = "RED"
    else:
        trend = "NEUTRAL"

    return trend, delta_pct


def _generate_insight_text(
    player_name: str, trend: str,
    goals: list, assists: list,
    xg: list, xa: list,
    match_count: int
) -> str:
    """Generate a short AI-powered insight about the player's form."""
    recent_goals = sum(goals[-2:])
    recent_assists = sum(assists[-2:])
    recent_xg = sum(xg[-2:])
    total_goals = sum(goals)
    total_assists = sum(assists)
    total_xg = sum(xg)

    trend_label = {
        "GREEN": "rising form",
        "RED": "falling form",
        "NEUTRAL": "stable form"
    }[trend]

    prompt = f"""Player: {player_name}
Form: {trend_label}
Stats last {match_count} matches: {total_goals} goals, {total_assists} assists, total xG {total_xg:.2f}
Last 2 matches: {recent_goals} goals, {recent_assists} assists, xG {recent_xg:.2f}

Write ONE concise comment (under 20 words)."""

    from services.text_utils import clean_insight
    text = llm.generate_insight(prompt, system_instruction=SYSTEM_PROMPT)
    if not text:
        text = f"{player_name} is in {trend_label} with {total_goals} goals in the last {match_count} matches."
        return text
    return clean_insight(text, max_sentences=1)


def _static_insight(player: dict) -> str:
    """Generate a static template insight for non-notable players (no LLM call)."""
    name = player["player_name"]
    trend = player["trend"]
    goals = sum(player["goals"])
    assists = sum(player["assists"])
    n = player["match_count"]

    if trend == "GREEN":
        return f"{name} is in good form with {goals} goals and {assists} assists in the last {n} matches."
    elif trend == "RED":
        return f"{name} has not found their best form in the last {n} matches."
    else:
        return f"{name} maintains a stable form with {goals} goals in the last {n} matches."


def run_and_save(league: str, force: bool = False) -> int:
    """Run trend analysis for all players in a league and save to DB."""
    insights = analyze_all_players(league, force=force)
    if not insights:
        return 0

    saved = 0
    try:
        from db.config_db import get_connection
        from psycopg2.extras import execute_batch
        conn = get_connection()
        try:
            cur = conn.cursor()
            
            sql = """
                INSERT INTO player_insights
                    (player_id, player_name, league_id, trend, trend_score, insight_text)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, league_id) DO UPDATE SET
                    player_name = EXCLUDED.player_name,
                    trend = EXCLUDED.trend,
                    trend_score = EXCLUDED.trend_score,
                    insight_text = EXCLUDED.insight_text,
                    loaded_at = NOW()
                WHERE player_insights.trend IS DISTINCT FROM EXCLUDED.trend
                   OR player_insights.trend_score IS DISTINCT FROM EXCLUDED.trend_score
            """
            
            args_list = [
                (ins["player_id"], ins["player_name"], ins["league_id"],
                 ins["trend"], ins["score"], ins["insight_text"])
                for ins in insights
            ]
            
            if args_list:
                execute_batch(cur, sql, args_list, page_size=200)
                conn.commit()
                saved = len(args_list)
            log.info("✓ Saved %d player insights for %s", saved, league)
        finally:
            conn.close()
    except Exception as exc:
        log.error("Failed to save player insights: %s", exc)

    return saved


if __name__ == "__main__":
    # Quick Test Block
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    
    print("\n" + "="*60)
    print("      VERTEX FOOTBALL - PLAYER TREND QUICK TEST")
    print("="*60)
    
    # Mock data: Jude Bellingham (Rising form)
    mock_p_name = "Jude Bellingham"
    mock_trend = "GREEN"
    mock_goals = [0, 0, 1, 1, 1]  # 3 goals in last 3
    mock_assists = [0, 1, 0, 1, 0] # 2 assists in 5
    mock_xg = [0.12, 0.45, 0.68, 0.55, 0.92]
    mock_xa = [0.05, 0.35, 0.10, 0.40, 0.15]
    
    # Manually calculate scoring momentum score
    from services.player_trend import _calculate_trend
    # Just for illustration, mock needs list of shots/keypass
    mock_shots = [1, 2, 3, 2, 4]
    mock_kp = [1, 3, 1, 4, 1]
    calculated_trend, score = _calculate_trend(mock_goals, mock_assists, mock_xg, mock_xa, mock_shots, mock_kp)
    
    print(f"Analyzing player: {mock_p_name}")
    print(f"Calculated Momentum: {calculated_trend} ({score}%)")
    
    insight = _generate_insight_text(
        mock_p_name, mock_trend,
        mock_goals, mock_assists, mock_xg, mock_xa, len(mock_goals)
    )
    
    print("-" * 60)
    if insight:
        print(f"✨ AI PLAYER INSIGHT:\n{insight}")
    else:
        print("❌ Test failed.")
    print("="*60 + "\n")
