# Data Checklist — Vertex Football Scraper
> Cập nhật: 01/03/2026 | Giải: Multi-League (EPL, La Liga, Bundesliga, Serie A, Ligue 1, …)
> File CSV output tự động theo league: `dataset_{league}_*.csv`

---

## ✅ DỮ LIỆU ĐÃ CÓ

### 📦 Nguồn: Understat
*Pipeline: `understat/async_scraper.py` → 3 CSVs*

---

#### 1. Shot-level xG (`dataset_epl_xg.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `id` | str | Shot ID duy nhất |
| `match_id` | int | ID trận đấu |
| `player_id` | int | ID cầu thủ sút |
| `player` | str | Tên cầu thủ |
| `player_assisted` | str | Cầu thủ kiến tạo (nếu có) |
| `h_team` / `a_team` | str | Tên đội sân nhà / sân khách |
| `h_goals` / `a_goals` | int | Tỉ số trận đấu |
| `date` | str | Ngày thi đấu |
| `season` | int | Mùa giải (VD: 2025) |
| `minute` | int | Phút sút (0–90+) |
| `result` | str | Kết quả: `Goal`, `SavedShot`, `MissedShots`, `BlockedShot`, `OwnGoal` |
| `situation` | str | Tình huống: `OpenPlay`, `SetPiece`, `FromCorner`, `DirectFreekick`, `Penalty` |
| `shot_type` | str | Chân/đầu: `LeftFoot`, `RightFoot`, `Head`, `OtherBodyPart` |
| `last_action` | str | Hành động trước khi sút: `Pass`, `Cross`, `TakeOn`, `None`… |
| `x` | float | Tọa độ X trên sân [0–1] |
| `y` | float | Tọa độ Y trên sân [0–1] |
| `xg` | float | Expected Goals của cú sút |
| `h_a` | str | Cầu thủ thuộc đội nào: `h` hoặc `a` |

**Phạm vi hiện tại:** 127 shots / 5 matches (test limit)
**Phạm vi đầy đủ:** ~3,400 shots / 380 matches (full season)

---

#### 2. Player xG per Match (`dataset_epl_player_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `id` | int | Row ID |
| `match_id` | int | ID trận đấu |
| `player_id` | int | ID cầu thủ |
| `player` | str | Tên cầu thủ |
| `team_id` | int | ID đội bóng |
| `position` | str | Vị trí thi đấu trong trận |
| `time` | int | Số phút ra sân |
| `goals` | int | Số bàn thắng |
| `own_goals` | int | Bàn phản lưới |
| `shots` | int | Số lần sút |
| `assists` | int | Số đường kiến tạo |
| `key_passes` | int | Số đường chuyền tạo cơ hội |
| `xg` | float | Expected Goals |
| `xa` | float | Expected Assists |
| `xg_chain` | float | xG của mọi lần cầu thủ có bóng dẫn đến cú sút |
| `xg_buildup` | float | xG của những tình huống cầu thủ liên quan (không tính xa/xg) |

**Phạm vi hiện tại:** 147 player-match records / 5 matches
**Phạm vi đầy đủ:** ~8,000–10,000 records / full season

---

#### 3. Match Aggregates (`dataset_epl_match_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `match_id` | int | ID trận đấu |
| `h_team` / `a_team` | str | Đội nhà / Đội khách |
| `h_goals` / `a_goals` | int | Tỉ số thực |
| `h_xg` / `a_xg` | float | Tổng xG đội nhà / đội khách |
| `datetime_str` | str | Thời gian thi đấu |
| `league` | str | Tên giải đấu |
| `season` | int | Mùa giải |

**Phạm vi hiện tại:** 5 matches (test)
**Phạm vi đầy đủ:** 380 matches / full season

---

### 📦 Nguồn: FBref
*Pipeline: `fbref/fbref_scraper.py` → 4 CSVs*

---

#### 4. League Standings (`dataset_epl_standings.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `position` | int | Hạng trên bảng xếp hạng |
| `team_name` | str | Tên đội bóng |
| `team_id` | str | FBref team ID (e.g. `18bb7c10`) |
| `matches_played` | int | Số trận đã thi đấu |
| `wins` / `draws` / `losses` | int | Thắng / Hoà / Thua |
| `goals_for` | int | Số bàn thắng ghi được |
| `goals_against` | int | Số bàn thủng lưới |
| `goal_difference` | str | Hiệu số bàn thắng |
| `points` | int | Số điểm |
| `points_avg` | float | Điểm trung bình/trận |
| `form_last5` | str | Form 5 trận gần nhất (VD: `W W D L W`) |
| `attendance_per_g` | str | Lượng khán giả TB/trận |
| `top_scorer` | str | Cầu thủ ghi nhiều bàn nhất đội |
| `top_keeper` | str | Thủ môn chính |

**Phạm vi:** 20 đội / snapshot hiện tại

---

#### 5. Squad Stats (`dataset_epl_squad_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `team_name` / `team_id` | str | Tên & ID đội |
| `season` | str | Mùa giải |
| `players_used` | int | Số cầu thủ đã ra sân |
| `avg_age` | float | Tuổi trung bình của đội |
| `possession` | float | % kiểm soát bóng trung bình |
| `matches_played` | int | Số trận đã đấu |
| `goals` / `assists` | int | Tổng bàn / kiến tạo |
| `pens_made` / `pens_att` | int | Phạt đền thành công / thực hiện |
| `yellow_cards` / `red_cards` | int | Thẻ vàng / thẻ đỏ |
| `goals_per90` / `assists_per90` | float | Chỉ số ghi bàn/kiến tạo per 90 phút |

**Phạm vi:** 20 đội / snapshot hiện tại

---

#### 6. Squad Rosters / Player Profiles (`dataset_epl_squad_rosters.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` | str | Tên cầu thủ |
| `player_id` | str | FBref player ID |
| `player_url` | str | URL trang cầu thủ trên FBref |
| `nationality` | str | Quốc tịch (ISO 3-letter, VD: `ENG`) |
| `position` | str | Vị trí (VD: `GK`, `DF`, `MF`, `FW`, `MF,FW`) |
| `age` | str | Tuổi dạng FBref: `30-165` (năm-ngày) |
| `age_years` | int | Tuổi (năm) đã parse |
| `team_name` / `team_id` | str | Đội & ID đội |
| `season` | str | Mùa giải |

**Phạm vi hiện tại:** 64 players / 2 teams (test limit)
**Phạm vi đầy đủ:** ~550–600 players / 20 teams

---

#### 7. Player Season Stats (`dataset_epl_player_season_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` / `player_id` | str | Tên & ID cầu thủ |
| `team_name` / `team_id` | str | Đội & ID đội |
| `season` / `nationality` / `position` / `age` | str | Metadata |
| `matches_played` / `starts` | int | Số trận ra sân / đá chính |
| `minutes` | int | Tổng số phút thi đấu |
| `minutes_90s` | float | Số 90 phút tương đương |
| `goals` / `assists` | int | Bàn thắng / Kiến tạo |
| `goals_assists` | int | Bàn + Kiến tạo |
| `goals_non_pen` | int | Bàn thắng ngoài phạt đền |
| `pens_made` / `pens_att` | int | Phạt đền thành công / thực hiện |
| `shots` / `shots_on_target` | int | Tổng sút / Sút trúng đích |
| `shots_on_target_pct` | float | % sút trúng đích |
| `goals_per90` / `assists_per90` / `goals_assists_per90` | float | Chỉ số per 90 phút |
| `yellow_cards` / `red_cards` | int | Thẻ vàng / thẻ đỏ |

**Phạm vi hiện tại:** 64 player-season records / 2 teams (test)
**Phạm vi đầy đủ:** ~550–600 records / 20 teams

---

#### 8. Defensive Stats (`dataset_{league}_defensive_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` / `player_id` | str | Tên & ID cầu thủ |
| `team_name` / `team_id` | str | Đội & ID đội |
| `season` / `nationality` / `position` / `age` | str | Metadata |
| `minutes_90s` | float | Số 90 phút tương đương |
| `tackles` | int | Tổng số pha tắc bóng |
| `tackles_won` | int | Tắc bóng thành công |
| `tackles_def_3rd` / `tackles_mid_3rd` / `tackles_att_3rd` | int | Tắc bóng theo khu vực |
| `challenge_tackles` | int | Dribblers tackled |
| `challenges` | int | Số lần đối đầu 1v1 |
| `challenge_tackles_pct` | float | % tackle thành công khi 1v1 |
| `blocks` | int | Tổng blocks |
| `blocked_shots` / `blocked_passes` | int | Chặn sút / chặn chuyền |
| `interceptions` | int | Số lần cắt bóng |
| `tackles_interceptions` | int | Tkl + Int tổng hợp |
| `clearances` | int | Phá bóng |
| `errors` | int | Lỗi dẫn đến cơ hội đối thủ |
| `pressures` | int | Tổng số lần pressing |
| `pressure_regains` | int | Pressing thành công (đoạt bóng) |
| `pressure_regain_pct` | float | % pressing thành công |
| `pressures_def_3rd` / `pressures_mid_3rd` / `pressures_att_3rd` | int | Pressing theo khu vực |

**Phạm vi đầy đủ:** ~550–600 records / 20 teams

---

#### 9. Possession & Carry Stats (`dataset_{league}_possession_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` / `player_id` | str | Tên & ID cầu thủ |
| `team_name` / `team_id` | str | Đội & ID đội |
| `season` / `nationality` / `position` / `age` | str | Metadata |
| `minutes_90s` | float | Số 90 phút tương đương |
| `touches` | int | Tổng số lần chạm bóng |
| `touches_def_pen_area` | int | Chạm bóng trong vòng cấm nhà |
| `touches_def_3rd` / `touches_mid_3rd` / `touches_att_3rd` | int | Chạm bóng theo khu vực |
| `touches_att_pen_area` | int | Chạm bóng trong vòng cấm đối phương |
| `touches_live_ball` | int | Chạm bóng trực tiếp (không đặt) |
| `take_ons` | int | Tổng lần rê bóng qua người |
| `take_ons_won` | int | Rê bóng thành công |
| `take_ons_won_pct` | float | % rê bóng thành công |
| `take_ons_tackled` | int | Bị tắc khi rê |
| `carries` | int | Tổng lần mang bóng |
| `carries_distance` | float | Tổng quãng đường mang bóng (yards) |
| `carries_progressive_distance` | float | Quãng đường mang bóng tiến lên |
| `progressive_carries` | int | Số lần mang bóng tiến vào phần sân đối phương |
| `carries_into_final_third` | int | Mang bóng vào 1/3 cuối sân |
| `carries_into_penalty_area` | int | Mang bóng vào vòng cấm |
| `miscontrols` | int | Mất kiểm soát bóng |
| `dispossessed` | int | Bị đoạt bóng |
| `passes_received` | int | Số lần nhận bóng |
| `progressive_passes_received` | int | Nhận đường chuyền tiến lên |

**Phạm vi đầy đủ:** ~550–600 records / 20 teams

---

#### 10. GK Stats (`dataset_{league}_gk_stats.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` / `player_id` | str | Tên & ID thủ môn |
| `team_name` / `team_id` | str | Đội & ID đội |
| `season` / `nationality` / `position` / `age` | str | Metadata |
| `gk_games` / `gk_games_starts` | int | Số trận ra sân / đá chính |
| `minutes_gk` | int | Tổng phút thi đấu |
| `gk_goals_against` | int | Số bàn thủng lưới |
| `gk_goals_against_per90` | float | Bàn thủng/90 phút |
| `gk_shots_on_target_against` | int | Sút trúng đích đối mặt |
| `gk_saves` | int | Số pha cứu thua |
| `gk_save_pct` | float | % cứu thua |
| `gk_wins` / `gk_ties` / `gk_losses` | int | Thắng / Hòa / Thua |
| `gk_clean_sheets` | int | Số trận giữ sạch lưới |
| `gk_clean_sheets_pct` | float | % trận giữ sạch lưới |
| `gk_pens_att` / `gk_pens_allowed` / `gk_pens_saved` | int | PK: đối mặt / thủng / cản |
| `gk_psxg` | float | Post-Shot Expected Goals |
| `gk_psxg_per_shot_on_target` | float | PSxG / sút trúng đích |
| `gk_passes_completed_launched` / `gk_passes_launched` | int | Phát bóng dài hoàn thành / thực hiện |
| `gk_passes_pct_launched` | float | % phát bóng dài thành công |
| `gk_passes` / `gk_passes_throws` | int | Tổng chuyền / chuyền tay |
| `gk_goal_kicks` | int | Số lần phát bóng |
| `gk_crosses_faced` / `gk_crosses_stopped` | int | Tạt bóng đối mặt / cắt |
| `gk_crosses_stopped_pct` | float | % cắt tạt bóng |
| `gk_def_actions_outside_pen_area` | int | Hành động ngoài vòng cấm |
| `gk_avg_distance_def_actions` | float | Khoảng cách TB hành động (yards) |

**Phạm vi đầy đủ:** ~40–60 thủ môn / 20 teams

---

#### 11. Fixture Schedule (`dataset_{league}_fixtures.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `gameweek` | str | Vòng đấu |
| `date` | str | Ngày thi đấu (YYYY-MM-DD) |
| `start_time` | str | Giờ kick-off (local) |
| `dayofweek` | str | Thứ trong tuần |
| `home_team` / `home_team_id` | str | Đội nhà & FBref ID |
| `away_team` / `away_team_id` | str | Đội khách & FBref ID |
| `score` | str | Tỉ số (VD: `2–1`) |
| `home_xg` / `away_xg` | float | xG đội nhà / khách |
| `attendance` | str | Lượng khán giả |
| `venue` | str | Sân vận động |
| `referee` | str | Trọng tài |
| `match_report_url` | str | URL match report trên FBref |
| `match_id` | str | FBref match ID |

**Phạm vi đầy đủ:** 380 matches / full season

---

#### 12. Match Passing Stats (`dataset_{league}_match_passing.csv`)
| Field | Kiểu | Mô tả |
|---|---|---|
| `match_id` / `match_date` | str | Match ID & ngày thi đấu |
| `home_team` / `away_team` | str | Đội nhà / khách |
| `player_name` / `player_id` | str | Cầu thủ & ID |
| `team_name` / `team_id` | str | Đội của cầu thủ |
| `nationality` / `age` / `minutes` | str/int | Metadata |
| `passes_completed` / `passes` | int | Chuyền thành công / thực hiện |
| `passes_pct` | float | % chuyền chính xác |
| `passes_total_distance` | float | Tổng quãng đường chuyền (yards) |
| `passes_progressive_distance` | float | Quãng đường chuyền tiến lên |
| `passes_short_completed` / `passes_short` | int | Chuyền ngắn |
| `passes_medium_completed` / `passes_medium` | int | Chuyền trung bình |
| `passes_long_completed` / `passes_long` | int | Chuyền dài |
| `passes_pct_short` / `passes_pct_medium` / `passes_pct_long` | float | % chính xác theo loại |
| `assists` / `xa` | int/float | Kiến tạo / xA |
| `key_passes` | int | Số đường chuyền tạo cơ hội |
| `passes_into_final_third` | int | Chuyền vào 1/3 cuối sân |
| `passes_into_penalty_area` | int | Chuyền vào vòng cấm |
| `crosses_into_penalty_area` | int | Tạt vào vòng cấm |
| `progressive_passes` | int | Đường chuyền tiến lên |

**Phạm vi đầy đủ:** ~8,000–10,000 player-match records / full season

---

### 📦 Nguồn: Transfermarkt
*Pipeline: `transfermarkt/tm_scraper.py` → 2 CSVs*

---

#### 13. Team Metadata (`dataset_{league}_team_metadata.csv`) ✅ **MỚI**
| Field | Kiểu | Mô tả |
|---|---|---|
| `team_name` | str | Tên CLB |
| `team_id` | str | Transfermarkt team ID |
| `team_url` | str | URL trang CLB trên TM |
| `league_id` | str | League ID từ registry |
| `season` | str | Mùa giải |
| `logo_url` | str | URL logo CLB |
| `stadium_name` | str | Tên sân vận động |
| `stadium_capacity` | int | Sức chứa sân |
| `stadium_url` | str | URL trang sân trên TM |
| `manager_name` | str | Tên HLV trưởng |
| `manager_url` | str | URL trang HLV trên TM |
| `squad_size` | int | Số cầu thủ trong đội |
| `avg_age` | float | Tuổi trung bình |
| `num_foreigners` | int | Số cầu thủ ngoại |
| `total_market_value` | str | Tổng giá trị đội hình (VD: €1.23bn) |
| `formation` | str | Đội hình (VD: 4-3-3, 3-4-2-1) |

**Phạm vi đầy đủ:** 20 đội / giải

---

#### 14. Player Market Values (`dataset_{league}_market_values.csv`) ✅ **MỚI**
| Field | Kiểu | Mô tả |
|---|---|---|
| `player_name` | str | Tên cầu thủ |
| `player_id` | str | Transfermarkt player ID |
| `player_url` | str | URL trang cầu thủ trên TM |
| `player_image_url` | str | URL ảnh cầu thủ |
| `team_name` / `team_id` | str | Đội & ID đội |
| `league_id` | str | League ID từ registry |
| `season` | str | Mùa giải |
| `position` | str | Vị trí (VD: Centre-Forward, Goalkeeper) |
| `shirt_number` | int | Số áo |
| `date_of_birth` | str | Ngày sinh |
| `age` | int | Tuổi |
| `nationality` | str | Quốc tịch chính |
| `second_nationality` | str | Quốc tịch thứ hai |
| `height_cm` | int | Chiều cao (cm) |
| `foot` | str | Chân thuận (left/right/both) |
| `joined` | str | Ngày gia nhập CLB |
| `contract_until` | str | Hết hạn hợp đồng |
| `market_value` | str | Giá trị chuyển nhượng (VD: €65.00m) |
| `market_value_numeric` | float | Giá trị quy ra EUR (VD: 65000000.0) |

**Phạm vi đầy đủ:** ~550–600 cầu thủ / 20 đội

---

### 📦 Nguồn: SofaScore
*Pipeline: `sofascore/sofascore_client.py` → 3 CSVs*

---

#### 15. Heatmaps (`dataset_{league}_heatmaps.csv`) ✅ **MỚI**
| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` | int | SofaScore event ID |
| `match_date` | str | Ngày thi đấu (YYYY-MM-DD) |
| `home_team` / `away_team` | str | Đội nhà / Đội khách |
| `score` | str | Tỉ số (VD: 2-1) |
| `player_id` | int | SofaScore player ID |
| `player_name` | str | Tên cầu thủ |
| `team_name` | str | Đội của cầu thủ |
| `position` | str | Vị trí (M, F, D, G) |
| `jersey_number` | int | Số áo |
| `num_points` | int | Số điểm heatmap |
| `avg_x` | float | Vị trí X trung bình (0–100) |
| `avg_y` | float | Vị trí Y trung bình (0–100) |
| `heatmap_points_json` | str | JSON array tọa độ [{x, y, v}, ...] |
| `league_id` | str | League ID |
| `season` | str | Mùa giải |

**Phạm vi:** ~22 cầu thủ × N trận (default 5 trận = ~110 rows)

---

#### 16. Player Average Positions (`dataset_{league}_player_avg_positions.csv`) ✅ **MỚI**
| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` | int | SofaScore event ID |
| `match_date` | str | Ngày thi đấu |
| `home_team` / `away_team` | str | Đội nhà / Đội khách |
| `player_id` | int | SofaScore player ID |
| `player_name` | str | Tên cầu thủ |
| `team_name` | str | Đội của cầu thủ |
| `position` | str | Vị trí |
| `jersey_number` | int | Số áo |
| `avg_x` | float | Vị trí trung bình X (0–100) |
| `avg_y` | float | Vị trí trung bình Y (0–100) |
| `minutes_played` | int | Số phút thi đấu |
| `rating` | float | SofaScore rating (0–10) |
| `league_id` | str | League ID |
| `season` | str | Mùa giải |

**Phạm vi:** ~30 cầu thủ × N trận (default 5 trận = ~150 rows)

---

## ❌ DỮ LIỆU CHƯA CÓ

### 🟢 Nice-to-have
| Loại dữ liệu | Nguồn | Ghi chú |
|---|---|---|
| Player Photo | FBref / Wikipedia | URL ảnh đại diện (hiện đã có `player_image_url` từ TM) |

---

## 📊 Tổng kết

| # | File CSV | Nguồn | Rows (full season ước tính) | Trạng thái |
|---|---|---|---|---|
| 1 | `dataset_{league}_xg.csv` | Understat | ~3,400 shots | ✅ Sẵn sàng |
| 2 | `dataset_{league}_player_stats.csv` | Understat | ~10,000 records | ✅ Sẵn sàng |
| 3 | `dataset_{league}_match_stats.csv` | Understat | 380 matches | ✅ Sẵn sàng |
| 4 | `dataset_{league}_standings.csv` | FBref | 20 rows | ✅ Sẵn sàng |
| 5 | `dataset_{league}_squad_stats.csv` | FBref | 20 rows | ✅ Sẵn sàng |
| 6 | `dataset_{league}_squad_rosters.csv` | FBref | ~580 players | ✅ Sẵn sàng |
| 7 | `dataset_{league}_player_season_stats.csv` | FBref | ~580 records | ✅ Sẵn sàng |
| 8 | `dataset_{league}_defensive_stats.csv` | FBref | ~580 records | ✅ Sẵn sàng |
| 9 | `dataset_{league}_possession_stats.csv` | FBref | ~580 records | ✅ Sẵn sàng |
| 10 | `dataset_{league}_gk_stats.csv` | FBref | ~50 thủ môn | ✅ Sẵn sàng |
| 11 | `dataset_{league}_fixtures.csv` | FBref | 380 matches | ✅ Sẵn sàng |
| 12 | `dataset_{league}_match_passing.csv` | FBref | ~10,000 records | ✅ Sẵn sàng |
| 13 | `dataset_{league}_team_metadata.csv` | Transfermarkt | 20 đội | ✅ **MỚI** |
| 14 | `dataset_{league}_market_values.csv` | Transfermarkt | ~580 cầu thủ | ✅ **MỚI** |
| 15 | `dataset_{league}_heatmaps.csv` | SofaScore | ~110–8,000 records | ✅ **MỚI** |
| 16 | `dataset_{league}_player_avg_positions.csv` | SofaScore | ~150–10,000 records | ✅ **MỚI** |
