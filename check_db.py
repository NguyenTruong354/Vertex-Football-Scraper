import sys
from pathlib import Path

ROOT = Path(".").resolve()
sys.path.insert(0, str(ROOT))

import db.config_db as cfg

def main():
    try:
        conn = cfg.get_connection()
        print("Successfully connected to the PostgreSQL database!")
        conn.close()
    except Exception as e:
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    main()
