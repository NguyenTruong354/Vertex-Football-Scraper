import logging
import sys
from pathlib import Path

# Thêm root vào sys.path
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

from db.config_db import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def update_logos():
    """Cập nhật logo cho các giải đấu từ SofaScore CDN."""
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Lấy danh sách giải đấu có sofascore_id
    cur.execute("SELECT league_id, sofascore_id, display_name FROM leagues WHERE sofascore_id IS NOT NULL")
    leagues = cur.fetchall()
    
    if not leagues:
        log.warning("Chưa có giải đấu nào được định nghĩa sofascore_id trong DB.")
        return

    updated_count = 0
    log.info(f"Đang cập nhật logo cho {len(leagues)} giải đấu...")

    for league_id, ss_id, name in leagues:
        # SofaScore Unique Tournament Image CDN
        # Đây là nguồn ảnh PNG chất lượng cao, hỗ trợ cả nền trong suốt.
        logo_url = f"https://api.sofascore.app/api/v1/unique-tournament/{ss_id}/image"
        
        try:
            cur.execute(
                "UPDATE leagues SET logo_url = %s, updated_at = NOW() WHERE league_id = %s",
                (logo_url, league_id)
            )
            updated_count += 1
            log.info(f"Done updated logo for {name} ({league_id}) -> {logo_url}")
        except Exception as e:
            log.error(f"Error updating {name}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Finished! Updated {updated_count} logos.")

if __name__ == "__main__":
    update_logos()
