import sys
from pathlib import Path
ROOT = Path(".").resolve()
sys.path.insert(0, str(ROOT))
import db.config_db as cfg

conn = cfg.get_connection()
cur = conn.cursor()

def fetch(q):
    cur.execute(q)
    return cur.fetchall()

print("BƯỚC 5: Kiểm tra số lượng records hợp lý")
print("COUNT defensive_stats:", fetch("SELECT COUNT(*) FROM player_defensive_stats;")[0][0])
print("COUNT possession_stats:", fetch("SELECT COUNT(*) FROM player_possession_stats;")[0][0])

print("\nBƯỚC 6: Kiểm tra không có NULL ở cột quan trọng")
print("NULL player_name/team_id in defensive:", fetch("SELECT COUNT(*) FROM player_defensive_stats WHERE player_name IS NULL OR team_id IS NULL;")[0][0])

print("\nBƯỚC 7: Spot-check 1 cầu thủ cụ thể (Salah)")
salah = fetch("SELECT player_name, tackles, interceptions, pressures FROM player_defensive_stats WHERE player_name ILIKE '%Salah%';")
print(salah)

print("\nBƯỚC 8: Kiểm tra không còn 'squad total' rows lọt vào")
squads = fetch("SELECT COUNT(*) FROM player_defensive_stats WHERE player_name ILIKE '%squad%' OR minutes_90s = 0;")
print("Squad totals or 0 mins:", squads[0][0])
