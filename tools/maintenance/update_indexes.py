import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import db.config_db as cfg
conn = cfg.get_connection()
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("DROP INDEX IF EXISTS idx_shots_match;")
    cur.execute("DROP INDEX IF EXISTS idx_pms_match;")
    print("Indexes drop executed.")
conn.close()
