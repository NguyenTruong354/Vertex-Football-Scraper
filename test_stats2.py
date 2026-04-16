import asyncio
from network.http_client import CurlCffiClient
from core.utils import setup_logging, Notifier
import json

async def main():
    log = setup_logging()
    browser = CurlCffiClient(log, Notifier(log), dry_run=False)
    await browser.start()
    
    from db.config_db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT event_id FROM live_snapshots WHERE status='finished' LIMIT 1")
    row = cur.fetchone()
    event_id = row[0] if row else 12513470 # Example event ID
    print("Event ID:", event_id)
    
    data = await browser.get_json(f"/event/{event_id}/statistics")
    print(json.dumps(data, indent=2)[:1000])
    await browser.stop()

asyncio.run(main())
