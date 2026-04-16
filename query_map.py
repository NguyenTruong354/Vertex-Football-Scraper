import csv
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT fbref_team_id, sofascore_team_id, canonical_name FROM team_canonical WHERE league_id='EPL'")
res = cur.fetchall()
cur.close()
conn.close()
for r in res:
    print(r)
