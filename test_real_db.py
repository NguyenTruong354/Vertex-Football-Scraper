import asyncio
import sys
from db.config_db import get_async_pool
from services.agent_orchestrator import AgentOrchestrator
from services.llm_client import LLMClient
import json

# Ensure UTF-8 output for Windows terminals
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


async def main():
    print("--- 1. Fetching Real Match Data ---")
    pool = await get_async_pool()
    async with pool.acquire() as conn:
        stats = await conn.fetchrow("SELECT * FROM match_stats WHERE h_xg > 1.5 AND h_goals < 1 LIMIT 1")
        if not stats:
            stats = await conn.fetchrow("SELECT * FROM match_stats WHERE h_xg IS NOT NULL LIMIT 1")
            
        if not stats:
            print("No match stats found.")
            return
            
        stats_dict = dict(stats)
        
        # In the DB the home team stats are prefixed with h_, away with a_
        print(f"Match: {stats_dict.get('h_team')} {stats_dict.get('h_goals')} - {stats_dict.get('a_goals')} {stats_dict.get('a_team')}")
        print(f"xG: {stats_dict.get('h_xg'):.2f} - {stats_dict.get('a_xg'):.2f}")
        
        # Assume match_id or event_id links the incidents
        match_id = stats_dict.get('match_id') or stats_dict.get('id') or stats_dict.get('event_id')
        incidents = []
        if match_id:
            try:
                inc_rows = await conn.fetch("SELECT * FROM live_incidents WHERE event_id = $1 LIMIT 10", match_id)
                incidents = [dict(i) for i in inc_rows]
            except Exception as e:
                inc_rows = await conn.fetch("SELECT * FROM live_incidents LIMIT 10")
                incidents = [dict(i) for i in inc_rows]
        
    print(f"Extracted {len(incidents)} incidents.")
    
    # Map to what the orchestrator/agents expect
    mapped_stats = {
        "expected_goals_home": stats_dict.get('h_xg', 0),
        "goals_home": stats_dict.get('h_goals', 0),
        "possession_home": 50, # Assumption if not in DB row
        "expected_goals_away": stats_dict.get('a_xg', 0),
        "goals_away": stats_dict.get('a_goals', 0),
        "possession_away": 50
    }
    
    for i in incidents:
        if 'loaded_at' in i:
            i['loaded_at'] = str(i['loaded_at'])
            
    ctx = {
        "stats": mapped_stats,
        "incidents": incidents,
        "standings": {"context": "Both teams are fighting for crucial points."},
        "momentum_score": 0.82
    }
    
    print("\n--- 2. Running AgentOrchestrator ---")
    llm = LLMClient()
    orchestrator = AgentOrchestrator(llm)
    
    print("Initiating multi-agent LLM analysis (live_badge variant)...")
    result = await orchestrator.run_pipeline("live_badge", ctx)
    
    print("\n--- 3. Final Multi-Agent Insight ---")
    if result:
        print(json.dumps(result, indent=2, ensure_ascii=False))
    else:
        print("Pipeline returned None.")

if __name__ == "__main__":
    asyncio.run(main())
