import sys, json
sys.path.insert(0, "d:/Vertex_Football_Scraper2")
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

def print_section(title):
    print(f"\n{'='*70}")
    print(f"  TACTICAL ANTI-STRATEGY: {title}")
    print(f"{'='*70}")

# 1. ARSENAL: Who to "Man-Mark"? (Most touches/Passes)
print_section("ARSENAL - DISRUPTING THE BUILD-UP")
cur.execute("""
    SELECT player_name, position, 
           SUM(passes_completed) as total_passes, 
           AVG(pass_accuracy_pct) as avg_acc,
           SUM(touches) as total_touches
    FROM player_match_stats
    WHERE team_name ILIKE '%arsenal%'
    GROUP BY 1, 2
    ORDER BY total_touches DESC
    LIMIT 5
""")
rows = cur.fetchall()
print("  Top Ball Distributors (Targets for Pressing):")
print(f"  {'Player':<22} {'Pos':<5} {'Passes':>8} {'Acc%':>8} {'Touches':>8}")
for r in rows:
    print(f"  {r[0]:<22} {r[1]:<5} {r[2]:>8.0f} {r[3]:>8.1f} {r[4]:>8.0f}")

# 2. LEVERKUSEN: Defensive Weak Links (Dribbled past / Errors)
print_section("LEVERKUSEN - EXPLOITING DEFENSIVE GAPS")
cur.execute("""
    SELECT player_name, 
           SUM(dribbled_past) as got_beat, 
           SUM(errors) as critical_errors,
           SUM(fouls) as fouls_committed
    FROM player_defensive_stats
    WHERE team_name ILIKE '%leverkusen%'
    GROUP BY 1
    ORDER BY got_beat DESC, critical_errors DESC
    LIMIT 5
""")
rows = cur.fetchall()
print("  Vulnerable Defenders (Target for 1v1 Dribbling):")
print(f"  {'Player':<22} {'Beat':>8} {'Errors':>8} {'Fouls':>8}")
for r in rows:
    print(f"  {r[0]:<22} {r[1]:>8.0f} {r[2]:>8.0f} {r[3]:>8.0f}")

# 3. SHOOTING PATTERNS: Where do they miss?
print_section("MATCH ANALYSIS - SHOT PRECISION")
cur.execute("""
    SELECT team_name, 
           COUNT(*) FILTER (WHERE result = 'Goal') as goals,
           COUNT(*) FILTER (WHERE result = 'Saved' OR result = 'Blocked') as stopped,
           AVG(xg) as avg_shot_quality
    FROM shots
    WHERE team_name IN ('Arsenal', 'Bayer Leverkusen')
    GROUP BY 1
""")
rows = cur.fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} Goals | {r[2]} Stopped | Avg xG/Shot: {r[3]:.3f}")

# 4. RECENT MATCH INSIGHTS (The "Final Minutes" fatigue)
print_section("POST-MATCH RECAP: LEVERKUSEN VS ARSENAL")
cur.execute("""
    SELECT insight_text, statistics_json
    FROM live_snapshots
    WHERE event_id = 15631363
""")
row = cur.fetchone()
if row:
    print(f"  AI Verdict: {row[0]}")
    stats = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    print(f"\n  Final Key Stats:")
    print(f"    - Accurate Crosses: {stats.get('Accurate crosses', {}).get('home')} vs {stats.get('Accurate crosses', {}).get('away')}")
    print(f"    - Big Chances: {stats.get('Big chances', {}).get('home')} vs {stats.get('Big chances', {}).get('away')}")

conn.close()
