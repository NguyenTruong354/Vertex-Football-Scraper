import sys, json
sys.path.insert(0, "d:/Vertex_Football_Scraper2")
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

# 1. Live snapshot
cur.execute("""
SELECT home_team, away_team, home_score, away_score, status, minute, 
       poll_count, insight_text, loaded_at,
       statistics_json
FROM live_snapshots
WHERE event_id = 15631363
""")
snap = cur.fetchone()
if snap:
    print("=" * 60)
    print(f"  LIVE: {snap[0]} {snap[2]}-{snap[3]} {snap[1]}")
    print(f"  Status: {snap[4]} | Minute: {snap[5]}' | Polls: {snap[6]}")
    print(f"  Last update: {snap[8]}")
    print("=" * 60)
    
    if snap[7]:
        print(f"\n  AI INSIGHT: {snap[7]}")
    
    # Parse statistics
    stats = snap[9] if isinstance(snap[9], dict) else (json.loads(snap[9]) if snap[9] else {})
    if stats:
        print(f"\n  {'STATISTIC':<30} {'HOME':>8} {'AWAY':>8}")
        print(f"  {'-'*46}")
        important = ['Ball possession', 'Total shots', 'Shots on target', 
                     'Expected goals (xG)', 'Corner kicks', 'Fouls', 
                     'Passes', 'Accurate passes']
        for key in important:
            if key in stats:
                h = stats[key].get('home', '-')
                a = stats[key].get('away', '-')
                print(f"  {key:<30} {str(h):>8} {str(a):>8}")

# 2. Incidents
cur.execute("""
SELECT minute, added_time, incident_type, player_name, detail, is_home
FROM live_incidents
WHERE event_id = 15631363
ORDER BY minute, added_time
""")
incs = cur.fetchall()
if incs:
    print(f"\n{'='*60}")
    print("  MATCH EVENTS")
    print(f"{'='*60}")
    for inc in incs:
        m = f"{inc[0]}'"
        if inc[1]:
            m += f"+{inc[1]}'"
        side = snap[0] if inc[5] else snap[1]
        icon = {"goal": "GOAL", "card": "CARD", "substitution": "SUB", "varDecision": "VAR"}.get(inc[2], inc[2])
        player = inc[3] or ""
        detail = inc[4] or ""
        print(f"  {m:>7} | {icon:<5} | {side} | {player} {detail}")

# 3. Live match state
cur.execute("""
SELECT home_score, away_score, status, minute, loaded_at
FROM live_match_state
WHERE event_id = 15631363
""")
lms = cur.fetchone()
if lms:
    print(f"\n{'='*60}")
    print("  LIVE_MATCH_STATE (dual-write check)")
    print(f"{'='*60}")
    print(f"  Score: {lms[0]}-{lms[1]} | Status: {lms[2]} | Min: {lms[3]}' | Updated: {lms[4]}")

conn.close()
