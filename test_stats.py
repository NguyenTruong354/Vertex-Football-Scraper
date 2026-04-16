import requests
import json
headers = {"User-Agent": "Mozilla/5.0"}
# Liverpool vs Man City event id ? Let's just find any event id from DB
from db.config_db import get_connection
conn = get_connection()
cur = conn.cursor()
cur.execute("SELECT event_id FROM live_snapshots LIMIT 1")
row = cur.fetchone()
if not row:
    # Just use a known event id, 11352467 - random example
    event_id = 11352467
else:
    event_id = row[0]

res = requests.get(f"https://api.sofascore.com/api/v1/event/{event_id}/statistics", headers=headers)
print(json.dumps(res.json(), indent=2))
