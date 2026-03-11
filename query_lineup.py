import sys
sys.path.insert(0, "d:/Vertex_Football_Scraper2")
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

# Full lineup for both teams
cur.execute("""
SELECT team_name, player_name, position, jersey_number, is_substitute, formation, status
FROM match_lineups
WHERE event_id = 15631363
ORDER BY team_name, is_substitute, 
  CASE position WHEN 'G' THEN 1 WHEN 'D' THEN 2 WHEN 'M' THEN 3 WHEN 'F' THEN 4 ELSE 5 END
""")
rows = cur.fetchall()

current_team = ""
printed_bench = False
for r in rows:
    team, name, pos, num, sub, formation, status = r
    if team != current_team:
        current_team = team
        printed_bench = False
        print(f"\n{'='*55}")
        print(f"  {team} | Formation: {formation} | Status: {status}")
        print(f"{'='*55}")
        print("  STARTING XI:")
    
    if sub and not printed_bench:
        print("\n  BENCH:")
        printed_bench = True
    
    pos_tag = {"G": "GK", "D": "DEF", "M": "MID", "F": "FWD"}.get(pos, pos or "?")
    num_str = f"#{num}" if num else "#?"
    print(f"    {num_str:>4} {name:<28} [{pos_tag}]")

# Count
cur.execute("""
SELECT team_name, 
       COUNT(*) FILTER (WHERE NOT is_substitute) AS starters,
       COUNT(*) FILTER (WHERE is_substitute) AS subs,
       COUNT(*) AS total
FROM match_lineups
WHERE event_id = 15631363
GROUP BY team_name
""")
print(f"\n{'='*55}")
print("  SUMMARY")
print(f"{'='*55}")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]} starters + {r[2]} subs = {r[3]} total")

# Check live_snapshots
cur.execute("""
SELECT home_team, away_team, home_score, away_score, status, minute, loaded_at
FROM live_snapshots
WHERE event_id = 15631363
""")
snap = cur.fetchone()
if snap:
    print(f"\n{'='*55}")
    print("  LIVE STATUS")
    print(f"{'='*55}")
    print(f"  {snap[0]} {snap[2]}-{snap[3]} {snap[1]}")
    print(f"  Status: {snap[4]} | Minute: {snap[5]}")
    print(f"  Last update: {snap[6]}")

conn.close()
