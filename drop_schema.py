import sys
from pathlib import Path
ROOT = Path(".").resolve()
sys.path.insert(0, str(ROOT))
import db.config_db as cfg
conn = cfg.get_connection()
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
conn.close()
print("Schema public dropped and recreated.")
