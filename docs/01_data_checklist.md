# Data Checklist — Vertex Football Scraper
> Cập nhật: 28/02/2026 | Giải: Premier League 2025–2026

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

## ❌ DỮ LIỆU CHƯA CÓ

### 🔴 Critical (ưu tiên cao)
| Loại dữ liệu | Nguồn | Ghi chú |
|---|---|---|
| Pass Network Data | FBref match reports | Origin→destination, progressive passes, pass type |

### 🟡 Important (ưu tiên vừa)
| Loại dữ liệu | Nguồn | Ghi chú |
|---|---|---|
| Defensive Stats | FBref | Tackles, interceptions, blocks, pressures |
| Possession & Carry | FBref | Progressive carries, take-ons, touches by zone |
| GK Stats | FBref | PSxG, saves%, distribution |
| Fixture Schedule | FBref | Lịch thi đấu, kick-off time, venue |
| Team Metadata | Transfermarkt | Logo, stadium, manager, formation |

### 🟢 Nice-to-have
| Loại dữ liệu | Nguồn | Ghi chú |
|---|---|---|
| Market Value | Transfermarkt | Giá trị chuyển nhượng |
| Heatmaps / Touch Maps | WhoScored / SofaScore | Cần scraper riêng |
| Player Photo | FBref / Wikipedia | URL ảnh đại diện |

---

## 📊 Tổng kết

| # | File CSV | Nguồn | Rows (full season ước tính) | Trạng thái |
|---|---|---|---|---|
| 1 | `dataset_epl_xg.csv` | Understat | ~3,400 shots | ✅ Sẵn sàng |
| 2 | `dataset_epl_player_stats.csv` | Understat | ~10,000 records | ✅ Sẵn sàng |
| 3 | `dataset_epl_match_stats.csv` | Understat | 380 matches | ✅ Sẵn sàng |
| 4 | `dataset_epl_standings.csv` | FBref | 20 rows | ✅ Sẵn sàng |
| 5 | `dataset_epl_squad_stats.csv` | FBref | 20 rows | ✅ Sẵn sàng |
| 6 | `dataset_epl_squad_rosters.csv` | FBref | ~580 players | ✅ Sẵn sàng |
| 7 | `dataset_epl_player_season_stats.csv` | FBref | ~580 records | ✅ Sẵn sàng |
