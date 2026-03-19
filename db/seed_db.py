import sys
from pathlib import Path
from dotenv import load_dotenv

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.league_registry import LEAGUES
import db.config_db as cfg

load_dotenv()

def seed_db():
    conn = cfg.get_connection()
    if not conn:
        print("Failed to get database connection.")
        return

    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            print("Creating master tables first to avoid FK validation errors...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS leagues (
                    league_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
                    country TEXT, understat_name TEXT, fbref_comp_id INTEGER, fbref_slug TEXT,
                    tm_comp_id TEXT, tm_slug TEXT, sofascore_id INTEGER,
                    is_active BOOLEAN DEFAULT TRUE, priority INTEGER DEFAULT 1,
                    loaded_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ
                );
                CREATE TABLE IF NOT EXISTS seasons (
                    season_id TEXT PRIMARY KEY, display_name TEXT NOT NULL,
                    year_start INTEGER, year_end INTEGER, is_current BOOLEAN DEFAULT FALSE,
                    loaded_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
            print("Seeding master tables...")

            # 1. Seed Leagues
            for lg_id, lg in LEAGUES.items():
                cur.execute("""
                    INSERT INTO leagues (
                        league_id, display_name, country, understat_name,
                        fbref_comp_id, fbref_slug, tm_comp_id, tm_slug,
                        sofascore_id, is_active, priority
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (league_id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        country = EXCLUDED.country,
                        understat_name = EXCLUDED.understat_name,
                        fbref_comp_id = EXCLUDED.fbref_comp_id,
                        fbref_slug = EXCLUDED.fbref_slug,
                        tm_comp_id = EXCLUDED.tm_comp_id,
                        tm_slug = EXCLUDED.tm_slug,
                        sofascore_id = EXCLUDED.sofascore_id,
                        is_active = EXCLUDED.is_active,
                        priority = EXCLUDED.priority,
                        updated_at = NOW();
                """, (
                lg.league_id, lg.display_name, lg.country, lg.understat_name,
                lg.fbref_comp_id, lg.fbref_slug, lg.tm_comp_id, lg.tm_slug,
                None, lg.active, lg.priority))
                print(f"  Upserted league: {lg.league_id}")
                
            print("Seeding default seasons...")
            # 2. Seed Default Seasons
            seasons_data = [
                ("2024-2025", "2024-2025", 2024, 2025, True),
                ("2023-2024", "2023-2024", 2023, 2024, False),
                ("2025-2026", "2025-2026", 2025, 2026, False),
                ("2024", "2024", 2024, 2024, True),
                ("2025", "2025", 2025, 2025, False),
                ("2023", "2023", 2023, 2023, False)
            ]
            
            for s_id, d_name, y_start, y_end, is_curr in seasons_data:
                cur.execute("""
                    INSERT INTO seasons (
                        season_id, display_name, year_start, year_end, is_current
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (season_id) DO UPDATE SET
                        display_name = EXCLUDED.display_name,
                        year_start = EXCLUDED.year_start,
                        year_end = EXCLUDED.year_end,
                        is_current = EXCLUDED.is_current;
                """, (s_id, d_name, y_start, y_end, is_curr))
                print(f"  Upserted season: {s_id}")

            print("Executing remaining schema components...")
            schema_path = Path(__file__).resolve().parent / "schema.sql"
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            cur.execute(schema_sql)
            print("Schema execution complete.")

        conn.commit()
        print("Seeding complete.")
    except Exception as e:
        conn.rollback()
        print(f"Error during seeding: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    seed_db()
