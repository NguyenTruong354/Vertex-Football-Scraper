import csv
from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

valid_ids = []
with open("output/fbref/epl/dataset_epl_standings.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        valid_ids.append(row["team_id"])

cur.execute("SELECT team_name, team_id FROM standings WHERE league_id='EPL' AND season='2025-2026'")
all_db_teams = cur.fetchall()

to_delete = []
for name, tid in all_db_teams:
    if tid not in valid_ids:
        print(f"Deleting: {name} ({tid})")
        to_delete.append(tid)

if to_delete:
    format_strings = ','.join(['%s'] * len(to_delete))
    cur.execute(f"DELETE FROM standings WHERE league_id='EPL' AND season='2025-2026' AND team_id IN ({format_strings})", tuple(to_delete))
    conn.commit()
    print("Deleted", cur.rowcount)
else:
    print("No extra.")
