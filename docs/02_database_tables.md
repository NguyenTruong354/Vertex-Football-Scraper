# Database Tables — Vertex Football Scraper
> Các bảng có thể xây dựng từ dữ liệu hiện có | Premier League 2025–2026

---

## Kiến trúc tổng quan

```
┌─────────────────────────────────────────────────────────┐
│                    DIMENSION TABLES                      │
│  dim_teams │ dim_players │ dim_matches │ dim_seasons     │
└─────────────────────────────────────────────────────────┘
                           │
                    FACT TABLES
┌─────────────────────────────────────────────────────────┐
│  fact_shots │ fact_player_match │ fact_player_season     │
└─────────────────────────────────────────────────────────┘
                           │
                 AGGREGATE / VIEW TABLES
┌─────────────────────────────────────────────────────────┐
│  agg_standings │ agg_squad_stats │ agg_shot_zones        │
│  agg_xg_timeline │ agg_player_rankings                   │
└─────────────────────────────────────────────────────────┘
```

---

## DIMENSION TABLES (Bảng tra cứu)

### `dim_teams` — Thông tin đội bóng
**Nguồn:** `dataset_epl_standings.csv` + `dataset_epl_squad_stats.csv`

```sql
CREATE TABLE dim_teams (
    team_id         TEXT PRIMARY KEY,     -- FBref team ID (e.g. '18bb7c10')
    team_name       TEXT NOT NULL,        -- 'Arsenal'
    season          TEXT NOT NULL,        -- '2025-2026'
    -- Squad Profile
    players_used    INTEGER,              -- Số cầu thủ đã ra sân
    avg_age         REAL,                 -- Tuổi trung bình đội
    possession_avg  REAL,                 -- % kiểm soát bóng TB
    -- Form & Standing
    position        INTEGER,              -- Hạng BXH
    matches_played  INTEGER,
    wins            INTEGER,
    draws           INTEGER,
    losses          INTEGER,
    goals_for       INTEGER,
    goals_against   INTEGER,
    goal_difference TEXT,
    points          INTEGER,
    points_avg      REAL,
    form_last5      TEXT,                 -- 'W W D L W'
    -- Aggregates
    goals_season    INTEGER,
    assists_season  INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    goals_per90     REAL,
    -- Info
    top_scorer      TEXT,
    top_keeper      TEXT,
    attendance_avg  TEXT,
    team_url        TEXT
);
```

**Dùng cho:**
- Trang profile đội bóng
- Bảng xếp hạng (league table)
- So sánh giữa các đội

---

### `dim_players` — Hồ sơ cầu thủ
**Nguồn:** `dataset_epl_squad_rosters.csv`

```sql
CREATE TABLE dim_players (
    player_id       TEXT PRIMARY KEY,     -- FBref player ID
    player_name     TEXT NOT NULL,
    nationality     TEXT,                 -- 'ENG', 'ESP', 'BRA'...
    position        TEXT,                 -- 'GK', 'DF', 'MF', 'FW', 'MF,FW'
    age             INTEGER,              -- Tuổi (years)
    age_raw         TEXT,                 -- FBref raw: '30-165'
    -- Team (tại thời điểm scrape)
    team_id         TEXT REFERENCES dim_teams(team_id),
    team_name       TEXT,
    season          TEXT,
    -- Links
    player_url      TEXT                  -- FBref URL
);
```

**Dùng cho:**
- Trang profile cầu thủ
- Tìm kiếm cầu thủ theo quốc tịch / vị trí / độ tuổi
- Filter cầu thủ trong shot map

---

### `dim_matches` — Thông tin trận đấu
**Nguồn:** `dataset_epl_match_stats.csv`

```sql
CREATE TABLE dim_matches (
    match_id        INTEGER PRIMARY KEY,  -- Understat match ID
    h_team          TEXT,                 -- Đội nhà
    a_team          TEXT,                 -- Đội khách
    h_goals         INTEGER,              -- Bàn thắng đội nhà
    a_goals         INTEGER,              -- Bàn thắng đội khách
    h_xg            REAL,                 -- xG đội nhà
    a_xg            REAL,                 -- xG đội khách
    datetime_str    TEXT,                 -- Thời gian thi đấu
    league          TEXT,                 -- 'EPL'
    season          INTEGER               -- 2025
);
```

---

## FACT TABLES (Bảng sự kiện / chi tiết)

### `fact_shots` — Mỗi dòng = 1 cú sút
**Nguồn:** `dataset_epl_xg.csv`

```sql
CREATE TABLE fact_shots (
    shot_id         TEXT PRIMARY KEY,     -- Understat shot ID
    match_id        INTEGER REFERENCES dim_matches(match_id),
    player_id       INTEGER REFERENCES dim_players(player_id),
    player_name     TEXT,
    player_assisted TEXT,                 -- Người kiến tạo
    -- Match context
    h_team          TEXT,
    a_team          TEXT,
    h_a             TEXT,                 -- 'h' | 'a'
    date            TEXT,
    season          INTEGER,
    minute          INTEGER,              -- Phút sút
    -- Shot details
    result          TEXT,                 -- 'Goal','SavedShot','MissedShots','BlockedShot','OwnGoal'
    situation       TEXT,                 -- 'OpenPlay','SetPiece','FromCorner','DirectFreekick','Penalty'
    shot_type       TEXT,                 -- 'LeftFoot','RightFoot','Head','OtherBodyPart'
    last_action     TEXT,                 -- 'Pass','Cross','TakeOn'...
    -- Spatial data
    x               REAL,                 -- Tọa độ X [0–1]
    y               REAL,                 -- Tọa độ Y [0–1]
    xg              REAL                  -- Expected Goals value
);
```

**Dùng cho:**
- **Shot Map** — vẽ bản đồ các cú sút trên sân (scatter plot)
- **xG Timeline** — biểu đồ xG tích lũy theo phút
- **Shot Zone Analysis** — phân tích theo khu vực sân
- **xG over/underperformance** — G vs xG

---

### `fact_player_match` — Thống kê cầu thủ theo từng trận
**Nguồn:** `dataset_epl_player_stats.csv`

```sql
CREATE TABLE fact_player_match (
    id              INTEGER PRIMARY KEY,
    match_id        INTEGER REFERENCES dim_matches(match_id),
    player_id       INTEGER REFERENCES dim_players(player_id),
    player_name     TEXT,
    team_id         INTEGER,
    -- Playing time
    position        TEXT,                 -- Vị trí đá trong trận đó
    time            INTEGER,              -- Số phút ra sân
    -- Output
    goals           INTEGER,
    own_goals       INTEGER,
    shots           INTEGER,
    assists         INTEGER,
    key_passes      INTEGER,              -- Đường chuyền tạo cơ hội
    -- xG metrics (Understat)
    xg              REAL,                 -- Expected Goals
    xa              REAL,                 -- Expected Assists
    xg_chain        REAL,                 -- xG Chain
    xg_buildup      REAL                  -- xG Buildup
);
```

**Dùng cho:**
- **Player form chart** — biểu đồ phong độ theo từng trận
- **xG vs G** — so sánh xG thực vs bàn ghi được theo trận
- **Radar chart** — so sánh cầu thủ trong trận cụ thể

---

### `fact_player_season` — Thống kê cầu thủ toàn mùa
**Nguồn:** `dataset_epl_player_season_stats.csv`

```sql
CREATE TABLE fact_player_season (
    player_id       TEXT REFERENCES dim_players(player_id),
    team_id         TEXT REFERENCES dim_teams(team_id),
    season          TEXT,
    nationality     TEXT,
    position        TEXT,
    age             TEXT,
    PRIMARY KEY (player_id, season),
    -- Playing time
    matches_played  INTEGER,
    starts          INTEGER,
    minutes         INTEGER,
    minutes_90s     REAL,
    -- Goals & Assists
    goals           INTEGER,
    assists         INTEGER,
    goals_assists   INTEGER,
    goals_non_pen   INTEGER,
    pens_made       INTEGER,
    pens_att        INTEGER,
    -- Shooting
    shots           INTEGER,
    shots_on_target INTEGER,
    shots_on_target_pct REAL,
    -- Per 90
    goals_per90     REAL,
    assists_per90   REAL,
    goals_assists_per90 REAL,
    -- Discipline
    yellow_cards    INTEGER,
    red_cards       INTEGER
);
```

**Dùng cho:**
- **Leaderboards** — bảng xếp hạng ghi bàn / kiến tạo
- **Player comparison** — so sánh 2 cầu thủ cùng vị trí
- **Efficiency charts** — goals per 90, shots on target %
- **Squad depth view** — phân tích chiều sâu đội hình

---

## AGGREGATE TABLES / VIEWS (Bảng tổng hợp)

### `agg_standings` — Bảng xếp hạng
**Nguồn:** `dim_teams` | **Loại:** Materialized View

| Cột | Mô tả |
|---|---|
| position, team_name | Hạng, đội |
| MP, W, D, L | Số trận, thắng, hoà, thua |
| GF, GA, GD | Bàn, thủng lưới, hiệu số |
| Pts, Pts/G | Điểm, điểm TB |
| Form | 5 trận gần nhất |

**Hiển thị trên:** Trang chủ website / Dashboard

---

### `agg_squad_xg` — So sánh xG giữa các đội
**Nguồn:** `fact_shots` + `dim_teams` | **Loại:** Query / View

```sql
SELECT
    h_team AS team,
    COUNT(*) AS shots,
    ROUND(SUM(xg), 2) AS total_xg,
    SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END) AS goals,
    ROUND(SUM(xg) - SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END), 2) AS xg_diff
FROM fact_shots
WHERE season = 2025
GROUP BY h_team
ORDER BY total_xg DESC;
```

**Hiển thị:** Bar chart xG vs Actual Goals theo đội

---

### `agg_shot_zones` — Phân tích khu vực sút
**Nguồn:** `fact_shots` | **Loại:** View

Phân vùng sân theo tọa độ x/y:
- **Zone 1** — Trong vòng 6 yard (x > 0.88, 0.35 < y < 0.65)
- **Zone 2** — Trong vòng cấm (x > 0.78)
- **Zone 3** — Ngoài vòng cấm (x < 0.78)
- **Zone 4** — Góc hẹp (y < 0.25 or y > 0.75)

```sql
SELECT
    CASE
        WHEN x > 0.88 AND y BETWEEN 0.35 AND 0.65 THEN '6-yard box'
        WHEN x > 0.78 THEN 'Penalty area'
        WHEN x > 0.60 THEN 'Outside box'
        ELSE 'Long range'
    END AS zone,
    COUNT(*) AS shots,
    ROUND(AVG(xg), 4) AS avg_xg,
    SUM(CASE WHEN result = 'Goal' THEN 1 ELSE 0 END) AS goals
FROM fact_shots
GROUP BY zone;
```

**Hiển thị:** Pitch heatmap / Zone breakdown chart

---

### `agg_xg_timeline` — xG tích lũy theo phút
**Nguồn:** `fact_shots` | **Loại:** Query per match

```sql
SELECT
    match_id, h_a, minute,
    xg,
    SUM(xg) OVER (PARTITION BY match_id, h_a ORDER BY minute) AS cumulative_xg
FROM fact_shots
ORDER BY match_id, minute;
```

**Hiển thị:** Line chart xG timeline (như Understat UI)

---

### `agg_player_rankings` — Bảng xếp hạng cầu thủ
**Nguồn:** `fact_player_season` + `dim_players` | **Loại:** View

Có thể sort theo bất kỳ metric nào:

| Metric | Dùng cho |
|---|---|
| `goals` | Vua phá lưới (Golden Boot) |
| `assists` | Xếp hạng kiến tạo |
| `goals_assists` | Đóng góp ghi bàn tổng |
| `xg` (từ fact_player_match) | xG tổng mùa |
| `xa` | xA tổng mùa |
| `xg_chain` | Đóng góp xG chain |
| `goals_per90` | Hiệu suất per 90 phút |
| `shots_on_target_pct` | Độ chính xác sút |
| `minutes` | Số phút thi đấu |

---

### `agg_player_comparison` — So sánh 2 cầu thủ (Radar)
**Nguồn:** `fact_player_season` + `fact_player_match` | **Loại:** Query

Normalize mọi metric về thang [0–100] trong cùng position group:

```sql
-- Ví dụ so sánh 2 tiền đạo:
SELECT
    p.player_name,
    -- Normalize về per-90 để công bằng
    goals / NULLIF(minutes_90s, 0) AS goals_p90,
    assists / NULLIF(minutes_90s, 0) AS assists_p90,
    shots_on_target_pct,
    xg / NULLIF(minutes_90s, 0) AS xg_p90,
    xa / NULLIF(minutes_90s, 0) AS xa_p90
FROM fact_player_season fps
JOIN dim_players p ON fps.player_id = p.player_id
WHERE p.position LIKE '%FW%';
```

**Hiển thị:** Radar/Spider chart

---

## Entity Relationship Diagram

```
dim_seasons ──────────────────────────────────────┐
                                                   │
dim_teams ──────────────────┐                     │
     │                      │                     │
     │              fact_player_season ────────────┤
     │                      │                     │
dim_players ────────────────┤                     │
     │                      │                     │
     │              fact_player_match ─────────────┤
     │                      │                     │
     │               fact_shots ──────────────────┤
     │                      │                     │
     └─────── dim_matches ──┘                     │
                    │                              │
                    └──────────────────────────────┘
```

---

## Ghi chú

- **player_id** giữa Understat (integer) và FBref (string) là **khác nhau**. Để join 2 nguồn, cần match bằng tên cầu thủ (`player_name`) hoặc xây dựng bảng mapping riêng `player_id_mapping(understat_id, fbref_id)`.
- **team_id** cũng tương tự: Understat dùng integer, FBref dùng hex string.
- Hầu hết các framework web (Next.js, Django REST) có thể dùng trực tiếp CSV với pandas, hoặc import vào SQLite / PostgreSQL.
