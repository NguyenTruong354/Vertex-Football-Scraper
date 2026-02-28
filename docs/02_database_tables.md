# Database Tables — Vertex Football Scraper
> Cập nhật: 01/03/2026 | PostgreSQL 18.2 | 17 bảng + indexes + FK constraints

---

## Kiến trúc tổng quan

```
NHÓM A: PARENT TABLES           NHÓM E: FBREF EXTRA
  match_stats  ←── shots          fixtures
  standings    ←── squad_rosters  gk_stats
               ←── squad_stats
               ←── player_season_stats   NHÓM F: SOFASCORE
                                           ss_events
NHÓM B: UNDERSTAT CHILDREN                player_avg_positions
  shots                                   heatmaps
  player_match_stats
                                 NHÓM G: TRANSFERMARKT
NHÓM C: FBREF CHILDREN            team_metadata
  squad_rosters                   market_values
  squad_stats
  player_season_stats            NHÓM H: LIVE TRACKING
                                   live_snapshots
NHÓM D: CROSS-SOURCE               live_incidents
  player_crossref
```

---

## Load order (FK-safe)

```
1. match_stats       ← parent
2. standings         ← parent
3. shots             → FK → match_stats
4. player_match_stats→ FK → match_stats
5. squad_rosters     → FK → standings
6. squad_stats       → FK → standings
7. player_season_stats→ FK → standings
8. player_crossref   ← build sau khi có shots + player_season_stats
9–17. Các bảng còn lại (không FK phức tạp)
```

---

## Chi tiết từng bảng

### 1. `match_stats` — Match aggregates (Understat) [PARENT]

```sql
PRIMARY KEY (match_id, league_id)
```

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `match_id` | BIGINT NOT NULL | Understat match ID |
| `league_id` | TEXT NOT NULL | EPL / LALIGA / … DEFAULT 'EPL' |
| `h_team` / `a_team` | TEXT | Tên đội |
| `h_goals` / `a_goals` | INTEGER | Tỉ số thực |
| `h_xg` / `a_xg` | REAL | xG tổng |
| `datetime_str` | TEXT | Thời gian thi đấu |
| `league` | TEXT | Tên giải |
| `season` | INTEGER | Mùa (VD: 2025) |
| `loaded_at` | TIMESTAMPTZ | DEFAULT NOW() |

**Indexes:** `idx_ms_season ON match_stats (season)`

---

### 2. `standings` — BXH giải đấu (FBref) [PARENT]

```sql
PRIMARY KEY (team_id, league_id)
```

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `team_id` | TEXT NOT NULL | FBref team ID |
| `league_id` | TEXT NOT NULL | DEFAULT 'EPL' |
| `position` | INTEGER | Hạng trên BXH |
| `team_name` / `team_url` | TEXT | |
| `matches_played` / `wins` / `draws` / `losses` | INTEGER | |
| `goals_for` / `goals_against` / `goal_difference` | INTEGER | |
| `points` | INTEGER | |
| `points_avg` | REAL | |
| `form_last5` | TEXT | VD: `W W D L W` |
| `attendance_per_g` / `top_scorer` / `top_keeper` | TEXT | |

---

### 3. `shots` — Shot-level xG (Understat)

```sql
PRIMARY KEY (id, league_id)
FK: (match_id, league_id) → match_stats ON DELETE CASCADE
```

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `id` | BIGINT NOT NULL | Shot ID |
| `league_id` | TEXT NOT NULL | |
| `match_id` / `player_id` | BIGINT | |
| `player` / `player_assisted` | TEXT | |
| `h_team` / `a_team` | TEXT | |
| `h_goals` / `a_goals` | INTEGER | |
| `date` | TEXT | |
| `season` | INTEGER | |
| `minute` | INTEGER | |
| `result` | TEXT | Goal / SavedShot / … |
| `situation` | TEXT | OpenPlay / Penalty / … |
| `shot_type` | TEXT | LeftFoot / Head / … |
| `last_action` | TEXT | |
| `x` / `y` / `xg` | REAL | |
| `h_a` | TEXT | h / a |

**Indexes:**
- `idx_shots_match ON shots (match_id, league_id)`
- `idx_shots_player ON shots (player_id)`
- `idx_shots_season ON shots (season)`
- `idx_shots_result ON shots (result)`

---

### 4. `player_match_stats` — Player xG per match (Understat)

```sql
PRIMARY KEY (id, league_id)
FK: (match_id, league_id) → match_stats ON DELETE CASCADE
```

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `id` | BIGINT NOT NULL | |
| `match_id` / `player_id` / `team_id` | BIGINT | |
| `player` | TEXT | |
| `position` | TEXT | |
| `time` | INTEGER | Phút thi đấu |
| `goals` / `own_goals` / `shots` / `assists` / `key_passes` | INTEGER | |
| `xg` / `xa` / `xg_chain` / `xg_buildup` | REAL | |

**Indexes:**
- `idx_pms_match ON player_match_stats (match_id, league_id)`
- `idx_pms_player ON player_match_stats (player_id)`
- `idx_pms_team ON player_match_stats (team_id)`

---

### 5. `squad_rosters` — Hồ sơ cầu thủ (FBref)

```sql
PRIMARY KEY (player_id, team_id, league_id)
FK: (team_id, league_id) → standings ON DELETE CASCADE
```

| Cột | Kiểu |
|-----|------|
| `player_id` / `player_name` / `player_url` | TEXT |
| `nationality` / `position` / `age` | TEXT |
| `age_years` | INTEGER |
| `team_name` / `team_id` | TEXT |
| `season` | TEXT |

**Index:** `idx_rosters_team ON squad_rosters (team_id, league_id)`

---

### 6. `squad_stats` — Thống kê đội (FBref)

```sql
PRIMARY KEY (team_id, season, league_id)
FK: (team_id, league_id) → standings ON DELETE CASCADE
```

| Cột | Kiểu |
|-----|------|
| `team_id` / `team_name` / `season` | TEXT |
| `players_used` / `matches_played` / `goals` / `assists` | INTEGER |
| `pens_made` / `pens_att` / `yellow_cards` / `red_cards` | INTEGER |
| `avg_age` / `possession` / `goals_per90` / `assists_per90` | REAL |

---

### 7. `player_season_stats` — Thống kê cầu thủ cả mùa (FBref)

```sql
PRIMARY KEY (player_id, team_id, season, league_id)
FK: (team_id, league_id) → standings ON DELETE CASCADE
```

| Cột | Kiểu |
|-----|------|
| `player_id` / `player_name` / `team_id` / `team_name` / `season` | TEXT |
| `nationality` / `position` / `age` | TEXT |
| `matches_played` / `starts` / `minutes` | INTEGER |
| `goals` / `assists` / `goals_assists` / `goals_non_pen` | INTEGER |
| `pens_made` / `pens_att` / `shots` / `shots_on_target` | INTEGER/REAL |
| `shots_on_target_pct` / `goals_per90` / `assists_per90` | REAL |
| `yellow_cards` / `red_cards` | INTEGER |
| `minutes_90s` / `goals_assists_per90` | REAL |

**Indexes:**
- `idx_pss_team ON player_season_stats (team_id, league_id)`
- `idx_pss_season ON player_season_stats (season)`

---

### 8. `player_crossref` — Ánh xạ Understat ↔ FBref ID

```sql
PRIMARY KEY (understat_player_id, fbref_player_id, league_id)
```

> Understat dùng INTEGER ID, FBref dùng TEXT slug → không JOIN trực tiếp được.
> Bảng này build tự động bằng fuzzy name matching sau khi load data.

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `understat_player_id` | BIGINT NOT NULL | VD: 8260 |
| `fbref_player_id` | TEXT NOT NULL | VD: `a23b4c5d/Bukayo-Saka` |
| `canonical_name` | TEXT | |
| `league_id` | TEXT NOT NULL | |
| `matched_by` | TEXT | `name_exact` / `name_fuzzy` / `manual` |

**Indexes:**
- `idx_crossref_us ON player_crossref (understat_player_id, league_id)`
- `idx_crossref_fb ON player_crossref (fbref_player_id, league_id)`

---

### 9. `fixtures` — Lịch thi đấu (FBref)

```sql
PRIMARY KEY (match_id, league_id)
```

| Cột | Kiểu |
|-----|------|
| `match_id` | TEXT NOT NULL |
| `gameweek` | INTEGER |
| `date` / `start_time` / `dayofweek` | TEXT |
| `home_team` / `away_team` | TEXT |
| `home_xg` / `away_xg` | REAL |
| `score` | TEXT |
| `attendance` / `venue` / `referee` | TEXT |
| `match_report_url` | TEXT |
| `home_team_id` / `away_team_id` | TEXT |

**Indexes:**
- `idx_fix_date ON fixtures (date)`
- `idx_fix_home ON fixtures (home_team_id, league_id)`
- `idx_fix_away ON fixtures (away_team_id, league_id)`

---

### 10. `gk_stats` — Thống kê thủ môn (FBref)

```sql
PRIMARY KEY (player_id, team_id, league_id)
```

40+ cột thống kê GK: games, saves, clean_sheets, PSxG, passes, crosses, def_actions…

---

### 11. `ss_events` — Danh sách trận (SofaScore)

```sql
PRIMARY KEY (event_id, league_id)
```

| Cột | Kiểu |
|-----|------|
| `event_id` | BIGINT NOT NULL |
| `tournament_id` / `season_id` | INTEGER |
| `round_num` | INTEGER |
| `home_team` / `home_team_id` / `away_team` / `away_team_id` | TEXT/INTEGER |
| `home_score` / `away_score` | INTEGER |
| `status` | TEXT |
| `start_timestamp` | BIGINT |
| `match_date` | TEXT |
| `slug` | TEXT |

**Index:** `idx_sse_date ON ss_events (match_date)`

---

### 12. `player_avg_positions` — Vị trí trung bình (SofaScore)

```sql
PRIMARY KEY (event_id, player_id, league_id)
```

| Cột | Kiểu |
|-----|------|
| `event_id` / `player_id` | BIGINT NOT NULL |
| `match_date` / `home_team` / `away_team` | TEXT |
| `player_name` / `team_name` / `position` | TEXT |
| `jersey_number` | INTEGER |
| `avg_x` / `avg_y` | REAL |
| `minutes_played` | INTEGER |
| `rating` | REAL |
| `season` | TEXT |

---

### 13. `heatmaps` — Dữ liệu nhiệt độ (SofaScore)

```sql
PRIMARY KEY (event_id, player_id, league_id)
```

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `event_id` / `player_id` | BIGINT NOT NULL | |
| `player_name` / `team_name` / `position` | TEXT | |
| `jersey_number` | INTEGER | |
| `num_points` | INTEGER | Số điểm heatmap |
| `avg_x` / `avg_y` | REAL | Trung tâm hoạt động |
| `score` | TEXT | Tỉ số trận |
| `heatmap_points_json` | TEXT | Raw JSON array `[{x,y,v}]` — ⚠️ lớn |
| `season` | TEXT | |

> ⚠️ Bảng lớn nhất (~47 MB/mùa EPL). Nếu muốn tiết kiệm dung lượng, dùng `JSONB`
> thay `TEXT` để PostgreSQL tự nén qua TOAST.

---

### 14. `team_metadata` — Thông tin đội (Transfermarkt)

```sql
PRIMARY KEY (team_id, league_id)
```

| Cột | Kiểu |
|-----|------|
| `team_id` / `team_name` / `team_url` | TEXT |
| `logo_url` / `stadium_name` / `stadium_capacity` / `stadium_url` | TEXT |
| `manager_name` / `manager_url` / `manager_since` / `manager_contract_until` | TEXT |
| `squad_size` | INTEGER |
| `avg_age` / `num_foreigners` | REAL/INTEGER |
| `total_market_value` | TEXT |
| `formation` / `season` | TEXT |

---

### 15. `market_values` — Giá trị cầu thủ (Transfermarkt)

```sql
PRIMARY KEY (player_id, team_id, league_id)
```

| Cột | Kiểu |
|-----|------|
| `player_id` / `player_name` / `player_url` / `player_image_url` | TEXT |
| `team_name` / `team_id` | TEXT |
| `position` / `shirt_number` | TEXT |
| `date_of_birth` / `age` / `nationality` / `second_nationality` | TEXT |
| `height_cm` / `foot` | TEXT |
| `joined` / `contract_until` | TEXT |
| `market_value` | TEXT | VD: `€180.00m` |
| `market_value_numeric` | REAL | Triệu EUR |
| `season` | TEXT | |

**Index:** `idx_mv_team ON market_values (team_id, league_id)`

---

### 16. `live_snapshots` — Live match state (Live Tracker)

```sql
PRIMARY KEY (event_id)
```

Upsert mỗi poll cycle. Luôn chứa trạng thái **mới nhất** của trận đấu.

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `event_id` | BIGINT NOT NULL | SofaScore event ID |
| `home_team` / `away_team` | TEXT | |
| `home_score` / `away_score` | INTEGER | DEFAULT 0 |
| `status` | TEXT | notstarted / inprogress / finished |
| `minute` | INTEGER | DEFAULT 0 |
| `statistics_json` | JSONB | 40+ stats: possession, xG, shots, passes… |
| `incidents_json` | JSONB | Toàn bộ sự kiện dạng JSON |
| `poll_count` | INTEGER | Số lần poll |
| `loaded_at` | TIMESTAMPTZ | DEFAULT NOW() |

**Index:** `idx_live_snap_status ON live_snapshots (status)`

---

### 17. `live_incidents` — Live match events (Live Tracker)

```sql
PRIMARY KEY (id BIGSERIAL)
UNIQUE INDEX: (event_id, incident_type, minute, COALESCE(player_name, ''))
```

Mỗi sự kiện là 1 row. Upsert theo unique index để tránh trùng.

| Cột | Kiểu | Ghi chú |
|-----|------|---------|
| `id` | BIGSERIAL | PK tự tăng |
| `event_id` | BIGINT NOT NULL | |
| `incident_type` | TEXT | goal / card / substitution / varDecision |
| `minute` / `added_time` | INTEGER | |
| `player_name` | TEXT | |
| `player_in_name` / `player_out_name` | TEXT | (thay người) |
| `is_home` | BOOLEAN | |
| `detail` | TEXT | penalty / ownGoal / yellow / red / yellowRed |

**Indexes:**
- `idx_live_inc_event ON live_incidents (event_id)`
- `uq_live_inc ON live_incidents (event_id, incident_type, minute, COALESCE(player_name, ''))`

---

## Foreign Key Constraints

| Constraint | From | To |
|-----------|------|----|
| `fk_shots_match` | `shots (match_id, league_id)` | `match_stats (match_id, league_id)` |
| `fk_pms_match` | `player_match_stats (match_id, league_id)` | `match_stats (match_id, league_id)` |
| `fk_rosters_team` | `squad_rosters (team_id, league_id)` | `standings (team_id, league_id)` |
| `fk_squad_stats_team` | `squad_stats (team_id, league_id)` | `standings (team_id, league_id)` |
| `fk_pss_team` | `player_season_stats (team_id, league_id)` | `standings (team_id, league_id)` |

> Tất cả FK đều `DEFERRABLE INITIALLY DEFERRED` — chỉ check khi COMMIT, an toàn khi bulk load.

---

## Upsert Strategy

Tất cả bảng dùng pattern:
```sql
INSERT INTO table (...) VALUES (...)
ON CONFLICT (...) DO UPDATE SET
    col1 = EXCLUDED.col1,
    ...
    loaded_at = NOW();
```

→ An toàn khi chạy lại pipeline nhiều lần. Không tạo duplicate.

---

## Khởi tạo Schema

```bash
# Chạy 1 lần để tạo toàn bộ 17 bảng + indexes + FK
python db/setup_db.py --schema-only

# Hoặc chạy trực tiếp SQL
psql -U postgres -d vertex_football -f db/schema.sql
```

---

## Quick Reference — Query mẫu

```sql
-- Top 10 xG nhất mùa EPL 2025
SELECT player, SUM(xg) AS total_xg, COUNT(*) AS shots
FROM shots WHERE league_id = 'EPL' AND season = 2025
GROUP BY player ORDER BY total_xg DESC LIMIT 10;

-- Live match status
SELECT home_team, home_score, away_score, away_team, minute, status,
       statistics_json->'Ball possession'->>'home' AS poss_home
FROM live_snapshots WHERE status = 'inprogress';

-- Market value vs xG (cross-source)
SELECT mv.player_name, mv.market_value_numeric,
       SUM(s.xg) AS season_xg, p.goals
FROM market_values mv
JOIN player_crossref cx  ON mv.player_id = cx.fbref_player_id
JOIN shots s             ON cx.understat_player_id = s.player_id
JOIN player_season_stats p ON cx.fbref_player_id = p.player_id
WHERE mv.league_id = 'EPL'
GROUP BY mv.player_name, mv.market_value_numeric, p.goals
ORDER BY mv.market_value_numeric DESC LIMIT 20;
```
