"""
SCOUT REPORT: Leverkusen vs Arsenal (LIVE)
Analyst: Barcelona FC Scouting Department
Objective: Tactical analysis + player evaluation for UCL preparation
"""
import sys, json
sys.path.insert(0, "d:/Vertex_Football_Scraper2")
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

print("=" * 65)
print("  BARCELONA FC - SCOUT REPORT")
print("  Leverkusen vs Arsenal | UCL Round of 16")
print("=" * 65)

# 1. LIVE MATCH STATUS
cur.execute("""
SELECT home_team, away_team, home_score, away_score, status, minute,
       poll_count, insight_text, statistics_json
FROM live_snapshots WHERE event_id = 15631363
""")
snap = cur.fetchone()
if snap:
    stats = snap[8] if isinstance(snap[8], dict) else (json.loads(snap[8]) if snap[8] else {})
    print(f"\n  {'[LIVE]':^61}")
    print(f"  {snap[0]} {snap[2]} - {snap[3]} {snap[1]}")
    print(f"  Status: {snap[4]} | Minute: {snap[5]}' | Polls: {snap[6]}")
    if snap[7]:
        print(f"\n  AI Insight: {snap[7]}")
    if stats:
        print(f"\n  {'MATCH STATISTICS':^61}")
        print(f"  {'':30} {'LEV':>8} {'ARS':>8}")
        print(f"  {'-'*46}")
        for key in sorted(stats.keys()):
            h = stats[key].get('home', '-')
            a = stats[key].get('away', '-')
            print(f"  {key:<30} {str(h):>8} {str(a):>8}")

# 2. MATCH EVENTS
cur.execute("""
SELECT minute, added_time, incident_type, player_name, detail, is_home
FROM live_incidents WHERE event_id = 15631363
ORDER BY minute, added_time
""")
incs = cur.fetchall()
if incs:
    print(f"\n  {'MATCH EVENTS':^61}")
    print(f"  {'-'*61}")
    for inc in incs:
        m = f"{inc[0]}'"
        if inc[1]: m += f"+{inc[1]}'"
        side = "LEV" if inc[5] else "ARS"
        icons = {"goal": "GOAL!!!", "card": "CARD", "substitution": "SUB", "varDecision": "VAR"}
        print(f"  {m:>7} | {icons.get(inc[2], inc[2]):<8} | {side} | {inc[3] or ''} ({inc[4] or ''})")

# 3. ARSENAL SEASON FORM (from standings)
print(f"\n{'=' * 65}")
print(f"  ARSENAL - SEASON PROFILE (EPL 2024-25)")
print(f"{'=' * 65}")
cur.execute("""
SELECT team_name, wins, draws, losses, goals_for, goals_against,
       goal_difference, points, position, top_scorer, form_last5
FROM standings
WHERE league_id = 'EPL' AND team_name ILIKE '%arsenal%'
ORDER BY season DESC LIMIT 1
""")
row = cur.fetchone()
if row:
    print(f"  Rank: #{row[8]} | W{row[1]} D{row[2]} L{row[3]} | {row[7]} pts")
    print(f"  Goals: {row[4]} scored, {row[5]} conceded (GD: {row[6]})")
    if row[9]: print(f"  Top Scorer: {row[9]}")
    if row[10]: print(f"  Form (last 5): {row[10]}")

# 4. ARSENAL TOP PERFORMERS (player season stats)
print(f"\n  {'ARSENAL KEY PLAYERS (Season Stats)':^61}")
print(f"  {'-'*61}")
cur.execute("""
SELECT player_name, position, matches_played, goals, assists, minutes_90s
FROM player_season_stats
WHERE league_id = 'EPL' AND team_id IN (
    SELECT team_id FROM standings WHERE league_id = 'EPL' AND team_name ILIKE '%arsenal%'
)
ORDER BY COALESCE(goals, 0) + COALESCE(assists, 0) DESC
LIMIT 8
""")
rows = cur.fetchall()
if rows:
    print(f"  {'Player':<22} {'Pos':<4} {'MP':>3} {'G':>3} {'A':>3} {'90s':>5}")
    for r in rows:
        print(f"  {(r[0] or ''):<22} {(r[1] or ''):4} {r[2] or 0:3} {r[3] or 0:3} {r[4] or 0:3} {r[5] or 0:5.1f}")

# 5. ARSENAL MARKET VALUES (Transfermarkt)
print(f"\n  {'ARSENAL SQUAD VALUE (Transfermarkt)':^61}")
print(f"  {'-'*61}")
cur.execute("""
SELECT player_name, position, market_value_numeric, age
FROM market_values
WHERE league_id = 'EPL' AND team_name ILIKE '%arsenal%'
ORDER BY market_value_numeric DESC NULLS LAST
LIMIT 8
""")
rows = cur.fetchall()
if rows:
    print(f"  {'Player':<22} {'Pos':<5} {'Age':>4} {'Value':>15}")
    for r in rows:
        val = r[2]
        if val and val >= 1_000_000:
            val_str = f"{val/1_000_000:.0f}M EUR"
        elif val:
            val_str = f"{val:,.0f} EUR"
        else:
            val_str = "N/A"
        print(f"  {(r[0] or ''):<22} {(r[1] or ''):5} {r[3] or 0:4} {val_str:>15}")

# 6. LEVERKUSEN DEFENSIVE WEAKNESSES (from FBref defensive stats)
print(f"\n{'=' * 65}")
print(f"  LEVERKUSEN - DEFENSIVE ANALYSIS")
print(f"{'=' * 65}")
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='player_defensive_stats' ORDER BY ordinal_position")
def_cols = [r[0] for r in cur.fetchall()]
# Change LEVERKUSEN table to player_defensive_stats
cur.execute("""
SELECT player_name, tackles_won, interceptions, blocks, pressures, errors
FROM player_defensive_stats
WHERE league_id IN ('EPL', 'BUNDESLIGA') AND team_name ILIKE '%%leverkusen%%'
ORDER BY COALESCE(pressures, 0) DESC
LIMIT 6
""")
rows = cur.fetchall()
if rows:
    print(f"  {'Player':<22} {'Tkl':>4} {'Int':>4} {'Blk':>4} {'Press':>6} {'Err':>4}")
    for r in rows:
        print(f"  {(r[0] or ''):<22} {r[1] or 0:4} {r[2] or 0:4} {r[3] or 0:4} {r[4] or 0:6} {r[5] or 0:4}")
else:
    print("  No defensive data found for Leverkusen (may be in BUNDESLIGA league).")

# 7. HEAD-TO-HEAD from match_crossref + fixtures
print(f"\n{'=' * 65}")
print(f"  RECENT FIXTURES (Arsenal)")
print(f"{'=' * 65}")
cur.execute("""
SELECT date, home_team, away_team, score
FROM fixtures
WHERE league_id = 'EPL' 
  AND (home_team ILIKE '%arsenal%' OR away_team ILIKE '%arsenal%')
ORDER BY date DESC
LIMIT 5
""")
rows = cur.fetchall()
if rows:
    for r in rows:
        print(f"  {r[0]} | {r[1]} {r[3]} {r[2]}")

conn.close()
print(f"\n{'=' * 65}")
print(f"  END OF SCOUT REPORT")
print(f"{'=' * 65}")
