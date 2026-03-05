import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db.config_db import get_connection
try:
    from thefuzz import fuzz
    from thefuzz import process
except ImportError:
    print("Vui lòng cài đặt thefuzz: pip install thefuzz python-Levenshtein")
    sys.exit(1)

def run_layer2_exact_sql(cur):
    """
    Match bằng SQL Replace (loại bỏ khoảng trắng, chuyển lowercase)
    """
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
    
    updated = 0
    for fb_id, league, tm_id in rows:
        cur.execute("""
        UPDATE player_crossref 
        SET tm_player_id = %s, matched_by = 'layer2_sql_exact'
        WHERE fbref_player_id = %s AND league_id = %s
        """, (tm_id, fb_id, league))
        updated += cur.rowcount
    
    print(f"[Layer 2 - SQL Exact] Đã cập nhật {updated} records.")


def run_layer3_fuzzy_python(cur):
    """
    Match bằng thuât toán Levenshtein Distance (thefuzz) 
    giữa FBref (squad_rosters) và Transfermarkt (market_values).
    """
    # 1. Lấy tất cả các cầu thủ từ FBref chưa có tm_player_id trong crossref
    # (hoặc thậm chí chưa tồn tại bản ghi trong crossref).
    cur.execute("""
        SELECT r.player_id, r.player_name, r.league_id
        FROM squad_rosters r
        LEFT JOIN player_crossref c ON c.fbref_player_id = r.player_id AND c.league_id = r.league_id
        WHERE c.tm_player_id IS NULL OR c.fbref_player_id IS NULL
        GROUP BY r.player_id, r.player_name, r.league_id
    """)
    unmapped_fbref_players = cur.fetchall()
    
    if not unmapped_fbref_players:
        print("[Layer 3 - Fuzzy] Không còn cầu thủ nào cần map.")
        return

    # 2. Lấy tất cả player từ Transfermarkt phân loại theo league
    cur.execute("""
        SELECT player_id, player_name, league_id
        FROM market_values
        GROUP BY player_id, player_name, league_id
    """)
    tm_players_raw = cur.fetchall()
    
    # Gom nhóm tm_players theo league_id để tìm kiếm nhanh hơn
    tm_players_by_league = {}
    for tm_id, tm_name, league_id in tm_players_raw:
        if league_id not in tm_players_by_league:
            tm_players_by_league[league_id] = []
        tm_players_by_league[league_id].append({"id": tm_id, "name": tm_name})

    inserted_or_updated = 0
    
    print(f"[Layer 3 - Fuzzy] Đang chạy thefuzz cho {len(unmapped_fbref_players)} cầu thủ FBref chưa có ảnh.")
    for fb_id, fb_name, league_id in unmapped_fbref_players:
        tm_pool = tm_players_by_league.get(league_id, [])
        if not tm_pool:
            continue
            
        best_match = None
        best_score = 0
        
        for tm in tm_pool:
            # Dùng token_sort_ratio để xử lý đảo họ tên (VD: "Pape Matar Sarr" vs "Pape Sarr")
            score = fuzz.token_sort_ratio(fb_name, tm['name'])
            if score > best_score:
                best_score = score
                best_match = tm
        
        # Ngưỡng (Threshold): 82 trở lên được coi là match hợp lệ
        if best_match and best_score >= 82:
            # Upsert vào player_crossref
            cur.execute("""
                INSERT INTO player_crossref (fbref_player_id, tm_player_id, league_id, matched_by, canonical_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (fbref_player_id, league_id) 
                DO UPDATE SET 
                    tm_player_id = EXCLUDED.tm_player_id,
                    matched_by = EXCLUDED.matched_by
            """, (fb_id, best_match['id'], league_id, f'layer3_fuzzy_{best_score}', fb_name))
            inserted_or_updated += 1

    print(f"[Layer 3 - Fuzzy] Đã tìm và lưu {inserted_or_updated} maps mới bằng thefuzz.")

def populate_tm_crossref():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            run_layer2_exact_sql(cur)
            run_layer3_fuzzy_python(cur)
        conn.commit()
    finally:
        conn.close()

if __name__ == "__main__":
    populate_tm_crossref()
