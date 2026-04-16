from db.config_db import get_connection
with open("db/schema.sql", "r", encoding="utf-8") as f:
    sql = f.read()

conn = get_connection()
cur = conn.cursor()
try:
    cur.execute(sql)
    conn.commit()
    print("Schema applied successfully.")
except Exception as e:
    conn.rollback()
    print(f"Error: {e}")
finally:
    cur.close()
    conn.close()
