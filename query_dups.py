from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT position, team_name, points, season, team_id, matches_played FROM standings WHERE league_id='EPL' AND season='2025-2026' ORDER BY position")
for r in cur.fetchall():
    print(r)
