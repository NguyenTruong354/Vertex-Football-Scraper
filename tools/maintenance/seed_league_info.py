import sys
from pathlib import Path

# Thêm root vào sys.path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_connection
from core.config import TOURNAMENT_IDS

def seed_leagues():
    conn = get_connection()
    cur = conn.cursor()
    
    print("Syncing league IDs from config to Database...")
    
    for league_id, ss_id in TOURNAMENT_IDS.items():
        try:
            # Cập nhật sofascore_id cho các giải đấu đang có
            cur.execute(
                "UPDATE leagues SET sofascore_id = %s WHERE league_id = %s",
                (ss_id, league_id)
            )
            print(f"Updated {league_id} with SofaScore ID: {ss_id}")
        except Exception as e:
            print(f"Error seeding {league_id}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print("Seeding complete!")

if __name__ == "__main__":
    seed_leagues()
