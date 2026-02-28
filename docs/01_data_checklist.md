# Data Checklist — Vertex Football Scraper
> Cập nhật: 01/03/2026 | Trạng thái: 4 nguồn scraping + PostgreSQL + Live Tracker
> File output theo league: `output/{source}/{league}/dataset_{league}_*.csv`

---

## Tổng quan nguồn dữ liệu

| # | Nguồn | Pipeline | Tables trong DB | Trạng thái |
|---|-------|----------|-----------------|-----------|
| 1 | **Understat** | `understat/async_scraper.py` | `match_stats`, `shots`, `player_match_stats` | ✅ Production |
| 2 | **FBref** | `fbref/fbref_scraper.py` | `standings`, `squad_rosters`, `squad_stats`, `player_season_stats`, `fixtures`, `gk_stats` | ✅ Production |
| 3 | **SofaScore** | `sofascore/sofascore_client.py` | `ss_events`, `player_avg_positions`, `heatmaps` | ✅ Production |
| 4 | **Transfermarkt** | `transfermarkt/tm_scraper.py` | `team_metadata`, `market_values` | ✅ Production |
| 5 | **Live Tracker** | `live_match.py` | `live_snapshots`, `live_incidents` | ✅ Production |
| — | **Cross-source** | `db/loader.py` (auto-build) | `player_crossref` | ✅ Production |

---

## 📦 Nguồn 1: Understat

*Script: `understat/async_scraper.py` | Kỹ thuật: aiohttp async JSON API*

### 1. Shot-level xG — bảng `shots`

| Field | Kiểu | Mô tả |
|---|---|---|
| `id` | BIGINT | Shot ID (Understat) — PK cùng `league_id` |
| `match_id` | BIGINT | ID trận đấu |
| `player_id` | BIGINT | ID cầu thủ sút |
| `player` | TEXT | Tên cầu thủ |
| `player_assisted` | TEXT | Cầu thủ kiến tạo (nếu có) |
| `h_team` / `a_team` | TEXT | Tên đội sân nhà / sân khách |
| `h_goals` / `a_goals` | INTEGER | Tỉ số trận đấu |
| `date` | TEXT | Ngày thi đấu |
| `season` | INTEGER | Mùa giải (VD: 2025) |
| `minute` | INTEGER | Phút sút (0–90+) |
| `result` | TEXT | `Goal`, `SavedShot`, `MissedShots`, `BlockedShot`, `OwnGoal` |
| `situation` | TEXT | `OpenPlay`, `SetPiece`, `FromCorner`, `DirectFreekick`, `Penalty` |
| `shot_type` | TEXT | `LeftFoot`, `RightFoot`, `Head`, `OtherBodyPart` |
| `last_action` | TEXT | Hành động trước khi sút: `Pass`, `Cross`, `TakeOn`… |
| `x` / `y` | REAL | Tọa độ trên sân [0–1] |
| `xg` | REAL | Expected Goals của cú sút |
| `h_a` | TEXT | `h` (home) hoặc `a` (away) |
| `league_id` | TEXT | EPL / LALIGA / BUNDESLIGA / SERIEA / LIGUE1 |

**Scale:** ~9,120 shots/mùa/giải — ~3.2 MB/mùa

---

### 2. Player xG per Match — bảng `player_match_stats`

| Field | Kiểu | Mô tả |
|---|---|---|
| `id` | BIGINT | PK cùng `league_id` |
| `match_id` | BIGINT | ID trận đấu |
| `player_id` | BIGINT | ID cầu thủ |
| `player` | TEXT | Tên cầu thủ |
| `team_id` | BIGINT | ID đội bóng (Understat internal) |
| `position` | TEXT | Vị trí thi đấu trong trận |
| `time` | INTEGER | Số phút ra sân |
| `goals` / `own_goals` | INTEGER | Bàn thắng / Phản lưới |
| `shots` / `assists` / `key_passes` | INTEGER | Sút / Kiến tạo / Chuyền tạo cơ hội |
| `xg` / `xa` | REAL | Expected Goals / Assists |
| `xg_chain` | REAL | xG tổng của chuỗi tấn công cầu thủ tham gia |
| `xg_buildup` | REAL | xG tình huống xây dựng (không tính xa/xg) |
| `league_id` | TEXT | League ID |

**Scale:** ~10,640 records/mùa/giải

---

### 3. Match Aggregates — bảng `match_stats`

| Field | Kiểu | Mô tả |
|---|---|---|
| `match_id` | BIGINT | PK cùng `league_id` |
| `h_team` / `a_team` | TEXT | Đội nhà / Đội khách |
| `h_goals` / `a_goals` | INTEGER | Tỉ số thực |
| `h_xg` / `a_xg` | REAL | Tổng xG đội nhà / đội khách |
| `datetime_str` | TEXT | Thời gian thi đấu |
| `league` | TEXT | Tên giải đấu |
| `season` | INTEGER | Mùa giải |
| `league_id` | TEXT | League ID |

**Scale:** 380 matches/mùa EPL

---

## 📦 Nguồn 2: FBref

*Script: `fbref/fbref_scraper.py` | Kỹ thuật: nodriver (Chrome headed) — bypass Cloudflare*

### 4. League Standings — bảng `standings`

| Field | Kiểu | Mô tả |
|---|---|---|
| `team_id` | TEXT | FBref team ID (VD: `18bb7c10`) — PK cùng `league_id` |
| `position` | INTEGER | Hạng trên BXH |
| `team_name` / `team_url` | TEXT | Tên & URL đội |
| `matches_played` | INTEGER | Số trận đã thi đấu |
| `wins` / `draws` / `losses` | INTEGER | Thắng / Hoà / Thua |
| `goals_for` / `goals_against` / `goal_difference` | INTEGER | Bàn ghi / Thủng / Hiệu số |
| `points` | INTEGER | Số điểm |
| `points_avg` | REAL | Điểm TB/trận |
| `form_last5` | TEXT | Form 5 trận gần nhất (VD: `W W D L W`) |
| `attendance_per_g` | TEXT | Khán giả TB/trận |
| `top_scorer` / `top_keeper` | TEXT | Cầu thủ ghi bàn / Thủ môn chính |

---

### 5. Squad Stats — bảng `squad_stats`

| Field | Kiểu | Mô tả |
|---|---|---|
| `team_id` / `team_name` / `season` | TEXT | PK kép cùng `league_id` |
| `players_used` | INTEGER | Số cầu thủ đã ra sân |
| `avg_age` | REAL | Tuổi trung bình |
| `possession` | REAL | % kiểm soát bóng TB |
| `matches_played` | INTEGER | Số trận đã đấu |
| `goals` / `assists` | INTEGER | Tổng bàn / kiến tạo |
| `pens_made` / `pens_att` | INTEGER | Phạt đền thành công / thực hiện |
| `yellow_cards` / `red_cards` | INTEGER | Thẻ vàng / thẻ đỏ |
| `goals_per90` / `assists_per90` | REAL | Chỉ số per 90 phút |

---

### 6. Squad Rosters — bảng `squad_rosters`

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` | TEXT | FBref player ID — PK cùng `team_id` + `league_id` |
| `player_name` / `player_url` | TEXT | Tên & URL cầu thủ |
| `nationality` | TEXT | Quốc tịch (ISO 3-letter, VD: `ENG`) |
| `position` | TEXT | Vị trí (VD: `GK`, `DF`, `MF`, `FW`) |
| `age` | TEXT | Tuổi dạng FBref: `30-165` (năm-ngày) |
| `age_years` | INTEGER | Tuổi (năm) đã parse |
| `team_name` / `team_id` | TEXT | Đội & ID đội |
| `season` | TEXT | Mùa giải |

**Scale:** ~560 players/mùa EPL

---

### 7. Player Season Stats — bảng `player_season_stats`

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` / `team_id` / `season` | TEXT | PK kép cùng `league_id` |
| `player_name` / `team_name` | TEXT | Tên cầu thủ / đội |
| `nationality` / `position` / `age` | TEXT | Metadata cầu thủ |
| `matches_played` / `starts` | INTEGER | Trận ra sân / đá chính |
| `minutes` | INTEGER | Tổng số phút thi đấu |
| `minutes_90s` | REAL | Số 90 phút tương đương |
| `goals` / `assists` / `goals_assists` | INTEGER | Bàn / Kiến tạo / Tổng |
| `goals_non_pen` | INTEGER | Bàn ngoài phạt đền |
| `pens_made` / `pens_att` | INTEGER | Phạt đền thành công / thực hiện |
| `shots` / `shots_on_target` / `shots_on_target_pct` | REAL | Thống kê sút |
| `goals_per90` / `assists_per90` / `goals_assists_per90` | REAL | Per 90 phút |
| `yellow_cards` / `red_cards` | INTEGER | Thẻ vàng / thẻ đỏ |

---

### 8. Fixtures — bảng `fixtures`

| Field | Kiểu | Mô tả |
|---|---|---|
| `match_id` | TEXT | FBref match ID — PK cùng `league_id` |
| `gameweek` | INTEGER | Vòng đấu |
| `date` / `start_time` / `dayofweek` | TEXT | Thời gian thi đấu |
| `home_team` / `away_team` | TEXT | Đội nhà / khách |
| `score` | TEXT | Tỉ số (nếu đã diễn ra) |
| `home_xg` / `away_xg` | REAL | xG (nếu đã diễn ra) |
| `attendance` | TEXT | Số khán giả |
| `venue` / `referee` | TEXT | Sân / Trọng tài |
| `match_report_url` | TEXT | URL báo cáo trận đấu FBref |
| `home_team_id` / `away_team_id` | TEXT | Team IDs |

---

### 9. Goalkeeper Stats — bảng `gk_stats`

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` / `team_id` | TEXT | PK cùng `league_id` |
| `player_name` / `team_name` | TEXT | Tên thủ môn / đội |
| `gk_games` / `gk_games_starts` | INTEGER | Trận ra sân / đá chính |
| `minutes_gk` | INTEGER | Tổng phút |
| `gk_goals_against` / `gk_goals_against_per90` | INTEGER/REAL | Bàn thủng lưới |
| `gk_saves` / `gk_save_pct` | INTEGER/REAL | Cứu thua / % cứu thua |
| `gk_clean_sheets` / `gk_clean_sheets_pct` | INTEGER/REAL | Sạch lưới / % |
| `gk_psxg` / `gk_psxg_per_shot_on_target` | REAL | Post-shot xG |
| `gk_passes` / `gk_passes_launched` / `gk_passes_pct_launched` | INTEGER/REAL | Chuyền bóng GK |
| `gk_crosses_faced` / `gk_crosses_stopped` / `gk_crosses_stopped_pct` | ... | Bắt phạt góc |
| `gk_def_actions_outside_pen_area` / `gk_avg_distance_def_actions` | ... | Ngoài vùng cấm |

---

## 📦 Nguồn 3: SofaScore

*Script: `sofascore/sofascore_client.py` | Kỹ thuật: nodriver — SofaScore API private*

### 10. Match Events — bảng `ss_events`

| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` | BIGINT | SofaScore event ID — PK cùng `league_id` |
| `tournament_id` / `season_id` | INTEGER | IDs giải đấu / mùa giải |
| `round_num` | INTEGER | Vòng đấu |
| `home_team` / `home_team_id` | TEXT/INTEGER | Đội nhà & ID |
| `away_team` / `away_team_id` | TEXT/INTEGER | Đội khách & ID |
| `home_score` / `away_score` | INTEGER | Tỉ số |
| `status` | TEXT | `finished`, `inprogress`, `notstarted` |
| `start_timestamp` | BIGINT | Unix timestamp |
| `match_date` | TEXT | Ngày thi đấu (YYYY-MM-DD) |
| `slug` | TEXT | URL slug |

---

### 11. Player Average Positions — bảng `player_avg_positions`

| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` / `player_id` | BIGINT | PK kép cùng `league_id` |
| `player_name` / `team_name` | TEXT | Tên cầu thủ / đội |
| `position` | TEXT | Vị trí thi đấu |
| `jersey_number` | INTEGER | Số áo |
| `avg_x` / `avg_y` | REAL | Vị trí trung bình trên sân [0–1] |
| `minutes_played` | INTEGER | Số phút ra sân |
| `rating` | REAL | Điểm số SofaScore |

---

### 12. Heatmaps — bảng `heatmaps`

| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` / `player_id` | BIGINT | PK kép cùng `league_id` |
| `player_name` / `team_name` | TEXT | Tên cầu thủ / đội |
| `num_points` | INTEGER | Số điểm heatmap |
| `avg_x` / `avg_y` | REAL | Trung tâm hoạt động trên sân |
| `heatmap_points_json` | TEXT | JSON array: `[{"x": 0.5, "y": 0.3, "v": 2}, ...]` |

> ⚠️ Bảng `heatmaps` là bảng lớn nhất (~47 MB/mùa) do lưu JSON thô.

---

## 📦 Nguồn 4: Transfermarkt

*Script: `transfermarkt/tm_scraper.py` | Kỹ thuật: nodriver (Chrome headed)*

### 13. Team Metadata — bảng `team_metadata`

| Field | Kiểu | Mô tả |
|---|---|---|
| `team_id` | TEXT | Transfermarkt team ID — PK cùng `league_id` |
| `team_name` / `team_url` | TEXT | Tên & URL đội |
| `logo_url` | TEXT | URL logo đội |
| `stadium_name` / `stadium_capacity` / `stadium_url` | TEXT | Thông tin sân vận động |
| `manager_name` / `manager_url` | TEXT | Tên & URL HLV |
| `manager_since` / `manager_contract_until` | TEXT | Thời hạn hợp đồng HLV |
| `squad_size` | INTEGER | Kích thước đội hình |
| `avg_age` | REAL | Tuổi trung bình |
| `num_foreigners` | INTEGER | Số cầu thủ nước ngoài |
| `total_market_value` | TEXT | Tổng giá trị chuyển nhượng (VD: `€893.60m`) |
| `formation` | TEXT | Sơ đồ chiến thuật (VD: `4-3-3`) |

---

### 14. Player Market Values — bảng `market_values`

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` | TEXT | Transfermarkt player ID — PK cùng `team_id` + `league_id` |
| `player_name` / `player_url` | TEXT | Tên & URL cầu thủ |
| `player_image_url` | TEXT | URL ảnh cầu thủ |
| `position` | TEXT | Vị trí thi đấu |
| `shirt_number` | TEXT | Số áo |
| `date_of_birth` / `age` | TEXT | Ngày sinh / Tuổi |
| `nationality` / `second_nationality` | TEXT | Quốc tịch chính / phụ |
| `height_cm` | TEXT | Chiều cao (cm) |
| `foot` | TEXT | Chân thuận (`left`, `right`, `both`) |
| `joined` / `contract_until` | TEXT | Ngày ký / Hết hạn hợp đồng |
| `market_value` | TEXT | Giá trị thị trường (VD: `€180.00m`) |
| `market_value_numeric` | REAL | Giá trị dạng số (triệu EUR) |

---

## 📦 Nguồn 5: Live Match Tracker

*Script: `live_match.py` | Kỹ thuật: nodriver — SofaScore live API — polling 60-90s*

### 15. Live Snapshots — bảng `live_snapshots`

Mỗi poll là 1 upsert vào bảng này (PK = `event_id`). Dữ liệu được cập nhật liên tục.

| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` | BIGINT | SofaScore event ID — PK |
| `home_team` / `away_team` | TEXT | Tên 2 đội |
| `home_score` / `away_score` | INTEGER | Tỉ số hiện tại |
| `status` | TEXT | `notstarted`, `inprogress`, `finished` |
| `minute` | INTEGER | Phút thi đấu hiện tại |
| `statistics_json` | JSONB | 40+ thống kê live (possession, xG, shots, passes, duels…) |
| `incidents_json` | JSONB | Toàn bộ sự kiện trận đấu dạng JSON |
| `poll_count` | INTEGER | Số lần đã poll |
| `loaded_at` | TIMESTAMPTZ | Lần cuối cập nhật |

**Queryable JSONB:**
```sql
SELECT
    statistics_json->'Ball possession'->>'home'  AS poss_home,
    statistics_json->'Expected goals'->>'away'   AS xg_away,
    statistics_json->'Total shots'->>'home'      AS shots_home
FROM live_snapshots WHERE event_id = 14023979;
```

---

### 16. Live Incidents — bảng `live_incidents`

Mỗi sự kiện là 1 row riêng biệt. Upsert dựa trên `(event_id, incident_type, minute, player_name)`.

| Field | Kiểu | Mô tả |
|---|---|---|
| `id` | BIGSERIAL | PK tự tăng |
| `event_id` | BIGINT | SofaScore event ID |
| `incident_type` | TEXT | `goal`, `card`, `substitution`, `varDecision` |
| `minute` | INTEGER | Phút xảy ra |
| `added_time` | INTEGER | Phút bù giờ |
| `player_name` | TEXT | Cầu thủ gây ra sự kiện |
| `player_in_name` / `player_out_name` | TEXT | Cầu thủ vào / ra (thay người) |
| `is_home` | BOOLEAN | Thuộc đội sân nhà không |
| `detail` | TEXT | `penalty`, `ownGoal`, `yellow`, `red`, `yellowRed`… |

---

## 🔗 Cross-Source Mapping

### 17. Player Crossref — bảng `player_crossref`

Ánh xạ Understat player_id (INTEGER) ↔ FBref player_id (TEXT slug). Được build tự động sau khi load data bằng fuzzy name matching.

| Field | Kiểu | Mô tả |
|---|---|---|
| `understat_player_id` | BIGINT | ID từ Understat (VD: 8260) |
| `fbref_player_id` | TEXT | Slug từ FBref (VD: `a23b4c5d/Bukayo-Saka`) |
| `canonical_name` | TEXT | Tên chuẩn hóa |
| `league_id` | TEXT | League ID |
| `matched_by` | TEXT | `name_exact`, `name_fuzzy`, `manual` |

**Cross-source query ví dụ:**
```sql
SELECT s.player, SUM(s.xg) AS understat_xg, p.goals, p.shots_on_target_pct
FROM shots s
JOIN player_crossref cx ON s.player_id = cx.understat_player_id AND s.league_id = cx.league_id
JOIN player_season_stats p ON cx.fbref_player_id = p.player_id AND cx.league_id = p.league_id
WHERE s.league_id = 'EPL' AND s.season = 2025
GROUP BY s.player, p.goals, p.shots_on_target_pct
ORDER BY understat_xg DESC LIMIT 20;
```

---

## 📊 Scale ước tính (1 mùa EPL)

| Bảng | Rows | Kích thước |
|------|------|-----------|
| `shots` | ~9,120 | ~3.2 MB |
| `player_match_stats` | ~10,640 | ~2.7 MB |
| `player_avg_positions` | ~10,640 | ~3.2 MB |
| `heatmaps` | ~8,500 | ~47 MB (JSON thô) |
| `player_season_stats` | ~560 | ~0.2 MB |
| `fixtures` | 380 | ~0.1 MB |
| `match_stats` | 380 | ~0.1 MB |
| `standings` | 20 | ~0.01 MB |
| `live_snapshots` | ~100/mùa | ~0.35 MB |
| `live_incidents` | ~1,500/mùa | ~0.3 MB |
| Còn lại | — | ~1 MB |
| **Tổng 1 mùa EPL** | | **~58 MB** |
| *Bỏ heatmap JSON* | | *~11 MB* |

> Bottleneck lớn nhất: `heatmaps.heatmap_points_json` (~82% tổng dung lượng)
