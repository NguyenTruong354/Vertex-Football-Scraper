import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.config_db import get_connection

def populate_tm_crossref():
    """
    Sử dụng Layer 2 (Fuzzy Name Match) để tự động cập nhật `tm_player_id`
    vào bảng `player_crossref` cho Layer 1 (chính xác 100%).
    Giúp tăng tốc độ truy xuất và build dần player master.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Tìm danh sách cầu thủ có match bằng Layer 2 nhưng tm_player_id còn trống trong crossref
            cur.execute("""
            SELECT r.player_id, r.league_id, max(mv2.player_id)
            FROM squad_rosters r
            JOIN player_crossref c ON c.fbref_player_id = r.player_id AND c.league_id = r.league_id
            JOIN market_values mv2 ON mv2.league_id = r.league_id 
                AND lower(regexp_replace(mv2.player_name, '\\s+', '', 'g')) = lower(regexp_replace(r.player_name, '\\s+', '', 'g'))
            WHERE c.tm_player_id IS NULL
            GROUP BY r.player_id, r.league_id;
            """)
            rows = cur.fetchall()
            print(f"Tìm thấy {len(rows)} record có thể fill tm_player_id từ Layer 2.")
            
            updated = 0
            for fb_id, league, tm_id in rows:
                cur.execute("""
                UPDATE player_crossref 
                SET tm_player_id = %s 
                WHERE fbref_player_id = %s AND league_id = %s
                """, (tm_id, fb_id, league))
                updated += cur.rowcount
            
            conn.commit()
            print(f"Đã cập nhật thành công {updated} records vào player_crossref.")
    finally:
        conn.close()

if __name__ == "__main__":
    populate_tm_crossref()
