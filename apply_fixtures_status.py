from db.config_db import get_connection

conn = get_connection()
cur = conn.cursor()

try:
    cur.execute("ALTER TABLE fixtures ADD COLUMN IF NOT EXISTS status VARCHAR(50)")
    cur.execute("""
        UPDATE fixtures
        SET status = CASE
            WHEN score IS NOT NULL AND score != '' THEN 'finished'
            WHEN date::date < CURRENT_DATE THEN 'finished'
            WHEN date::date = CURRENT_DATE THEN 'live'
            WHEN date::date > CURRENT_DATE THEN 'upcoming'
            ELSE 'upcoming'
        END
        WHERE status IS NULL;
    """)
    cur.execute("ALTER TABLE fixtures ALTER COLUMN status SET DEFAULT 'upcoming';")
    conn.commit()
    print("Fixtures table altered and status backfilled successfully.")
except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
finally:
    cur.close()
    conn.close()
