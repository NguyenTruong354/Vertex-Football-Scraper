import sys
from pathlib import Path
ROOT = Path('.').resolve()
sys.path.insert(0, str(ROOT))
import db.config_db as cfg

try:
    conn = cfg.get_connection()
    cur = conn.cursor()
    
    print('--- Thống kê bảng news_feed ---')
    cur.execute('SELECT COUNT(*) FROM news_feed;')
    total = cur.fetchone()[0]
    print(f'Tổng số tin tức: {total}')
    
    if total > 0:
        cur.execute('''
            SELECT source, published_at, title 
            FROM news_feed 
            ORDER BY published_at DESC 
            LIMIT 5;
        ''')
        rows = cur.fetchall()
        print('\n--- 5 tin mới nhất vừa cào được ---')
        for row in rows:
            source, pub_date, title = row
            if pub_date:
                date_str = pub_date.strftime('%Y-%m-%d %H:%M') if hasattr(pub_date, 'strftime') else str(pub_date)
            else:
                date_str = 'N/A'
            print(f'[{source}] {date_str} | {title}')
    
    conn.close()
except Exception as e:
    print(f'Lỗi kết nối hoặc truy vấn: {e}')
