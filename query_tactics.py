import sys, json
sys.path.insert(0, "d:/Vertex_Football_Scraper2")
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

def get_tactical_data(event_id):
    # 1. Final Stats & AI Insight
    cur.execute("SELECT home_team, away_team, home_score, away_score, insight_text, statistics_json FROM live_snapshots WHERE event_id = %s", (event_id,))
    snap = cur.fetchone()
    
    # 2. Player Ratings - Who dominated?
    cur.execute("""
        SELECT team_name, player_name, rating, minutes_played, position 
        FROM match_lineups 
        WHERE event_id = %s AND rating IS NOT NULL
        ORDER BY rating DESC LIMIT 10
    """, (event_id,))
    top_players = cur.fetchall()
    
    # 3. Defensive discipline (Cards/Fouls)
    cur.execute("""
        SELECT team_side, COUNT(*) 
        FROM match_lineups 
        WHERE event_id = %s AND rating < 6.5
        GROUP BY team_side
    """, (event_id,))
    underperformers = cur.fetchall()

    return snap, top_players, underperformers

event_id = 15631363
snap, top_players, underperformers = get_tactical_data(event_id)

print("="*70)
print(f"  TACTICAL ANTI-STRATEGY REPORT: {snap[0]} vs {snap[1]}")
print("="*70)
print(f"  FINAL SCORE: {snap[0]} {snap[2]} - {snap[3]} {snap[1]}")
print(f"  AI INSIGHT: {snap[4]}")

print("\n  [STRATEGY 1: NEUTRALIZING KEY MEN]")
print("  ------------------------------------------------")
for p in top_players:
    side = "HOME" if p[0] == snap[0] else "AWAY"
    print(f"  Target to mark: {p[1]:<20} | Rating: {p[2]:<4} | Side: {side}")

print("\n  [STRATEGY 2: EXPLOITING WEAK LINKS]")
print("  ------------------------------------------------")
print("  Leverkusen defensive lapses: Look at players with rating < 6.5")
cur.execute("SELECT player_name, position, rating FROM match_lineups WHERE event_id = %s AND team_name = %s AND rating < 6.5", (event_id, snap[0]))
for p in cur.fetchall():
    print(f"  Weak spot: {p[0]:<20} ({p[1]}) | Rating: {p[2]}")

print("\n  [STRATEGY 3: BREAKING THE PRESS]")
stats = snap[5] if isinstance(snap[5], dict) else json.loads(snap[5])
poss_h = stats.get('Ball possession', {}).get('home', '50%')
poss_a = stats.get('Ball possession', {}).get('away', '50%')
print(f"  Control: LEV {poss_h} - {poss_a} ARS")
print(f"  Arsenal's efficiency: {stats.get('Shots on target', {}).get('away', 0)} shots on target resulted in {snap[3]} goals.")

conn.close()
