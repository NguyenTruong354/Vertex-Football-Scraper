-- ============================================================
-- schema.sql — Vertex Football Scraper · PostgreSQL DDL
-- ============================================================
-- Chạy một lần để tạo database schema.
-- Dùng IF NOT EXISTS + DO-blocks → an toàn khi chạy lại nhiều lần.
--
-- Nguồn dữ liệu & player_id:
--   • Understat  → player_id = INTEGER (VD: 8260)
--   • FBref      → player_id = TEXT slug (VD: "a23b4c5d/Bukayo-Saka")
--   → Không thể JOIN trực tiếp! Dùng bảng player_crossref để ánh xạ.
--
-- Upsert strategy (tất cả bảng):
--   INSERT ... ON CONFLICT (...) DO UPDATE SET ...
--   → An toàn khi cào lại dữ liệu hàng tuần.
--
-- Load order (phải theo thứ tự để FK không bị lỗi):
--   1. match_stats          ← parent cho shots, player_match_stats
--   2. standings            ← parent cho squad_rosters, squad_stats, player_season_stats
--   3. shots                → FK → match_stats
--   4. player_match_stats   → FK → match_stats
--   5. squad_rosters        → FK → standings
--   6. squad_stats          → FK → standings
--   7. player_season_stats  → FK → standings
--   8. player_crossref      ← built from shots + player_season_stats
--
-- Tables:
--   1. match_stats          — match-level aggregate (Understat)   [PARENT]
--   2. standings            — league table standings (FBref)       [PARENT]
--   3. shots                — shot-level xG data (Understat)
--   4. player_match_stats   — player xG stats per match (Understat)
--   5. squad_rosters        — player profiles per team (FBref)
--   6. squad_stats          — team summary stats (FBref)
--   7. player_season_stats  — player season totals (FBref)
--   8. player_crossref      — ánh xạ Understat ↔ FBref ↔ Transfermarkt IDs
--   9. fixtures             — match schedule (FBref)
--  10. gk_stats             — goalkeeper stats (FBref)
--  11. player_defensive_stats — defensive actions (FBref)
--  12. player_possession_stats — touches, carries, take-ons (FBref)
--  13. ss_events            — match events list (SofaScore)
--  14. player_avg_positions — average positions (SofaScore)
--  15. heatmaps             — heatmap summaries (SofaScore)
--  16. match_lineups        — match lineups (SofaScore)
--  17. team_metadata        — team info (Transfermarkt)
--  18. market_values        — player market values (Transfermarkt)
--  19. live_snapshots       — 24/7 live match polling (Shared Browser)
--  20. match_summaries      — AI-generated match stories
--  21. player_insights       — AI-generated performance trends
--  22. news_feed            — RSS news aggregator
--  23. live_incidents      — live match incidents (goals, cards...)
-- ============================================================

-- ============================================================
-- NHÓM A: PARENT TABLES (phải tạo trước)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 1. MATCH STATS (Understat) — parent của shots + player_match_stats
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_stats (
    match_id      BIGINT  NOT NULL,
    h_team        TEXT,
    a_team        TEXT,
    h_goals       INTEGER,
    a_goals       INTEGER,
    h_xg          REAL,
    a_xg          REAL,
    datetime_str  TEXT,
    league        TEXT,
    season        INTEGER,
    league_id     TEXT NOT NULL DEFAULT 'EPL',
    loaded_at     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (match_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_ms_season  ON match_stats (season);

-- ──────────────────────────────────────────────────────────
-- 2. STANDINGS (FBref) — parent của squad_rosters, squad_stats, player_season_stats
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS standings (
    position         INTEGER,
    team_name        TEXT,
    team_id          TEXT,
    team_url         TEXT,
    matches_played   INTEGER,
    wins             INTEGER,
    draws            INTEGER,
    losses           INTEGER,
    goals_for        INTEGER,
    goals_against    INTEGER,
    goal_difference  INTEGER,
    points           INTEGER,
    points_avg       REAL,
    form_last5       TEXT,
    attendance_per_g TEXT,
    top_scorer       TEXT,
    top_keeper       TEXT,
    league_id        TEXT NOT NULL DEFAULT 'EPL',
    season           TEXT NOT NULL DEFAULT '2024-2025',
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, league_id, season)
);

-- ============================================================
-- NHÓM B: CHILD TABLES (Understat)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 3. SHOTS (xG data từ Understat)
--    FK: (match_id, league_id) → match_stats
--    INDEX: match_id, player_id (2 cột query nhiều nhất khi xem trang trận đấu)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS shots (
    id               BIGINT       NOT NULL,
    match_id         BIGINT,
    player_id        BIGINT,
    player           TEXT,
    player_assisted  TEXT,
    h_team           TEXT,
    a_team           TEXT,
    h_goals          INTEGER,
    a_goals          INTEGER,
    date             TEXT,
    season           INTEGER,
    minute           INTEGER,
    result           TEXT,   -- Goal, SavedShot, MissedShots, BlockedShot, OwnGoal
    situation        TEXT,   -- OpenPlay, SetPiece, FromCorner, DirectFreekick, Penalty
    shot_type        TEXT,   -- LeftFoot, RightFoot, Head, OtherBodyPart
    last_action      TEXT,
    x                REAL,
    y                REAL,
    xg               REAL,
    h_a              TEXT,   -- h (home) or a (away)
    league_id        TEXT    NOT NULL DEFAULT 'EPL',
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, league_id)
);

-- Index match_id + league_id: query "tất cả shot của trận X"
-- Index player_id: query "tất cả shot của cầu thủ Y"
CREATE INDEX IF NOT EXISTS idx_shots_match_cov
    ON shots (match_id, league_id)
    INCLUDE (player_id, player, minute, result, situation, shot_type, x, y, xg, h_a);
CREATE INDEX IF NOT EXISTS idx_shots_player_cov
    ON shots (player_id, season, league_id)
    INCLUDE (match_id, xg, result, situation, minute, x, y);

-- ──────────────────────────────────────────────────────────
-- 4. PLAYER MATCH STATS (per-player xG per match, Understat)
--    FK: (match_id, league_id) → match_stats
--    INDEX: match_id, player_id (query nhiều nhất khi xem trang trận đấu)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_match_stats (
    id          BIGINT  NOT NULL,
    match_id    BIGINT,
    player_id   BIGINT,
    player      TEXT,
    team_id     BIGINT,
    position    TEXT,
    time        INTEGER,  -- phút thi đấu
    goals       INTEGER,
    own_goals   INTEGER,
    shots       INTEGER,
    assists     INTEGER,
    key_passes  INTEGER,
    xg          REAL,
    xa          REAL,
    xg_chain    REAL,
    xg_buildup  REAL,
    league_id   TEXT NOT NULL DEFAULT 'EPL',
    loaded_at   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (id, league_id)
);

-- Index match_id + league_id: query "tất cả player stats của trận X"
-- Index player_id: query timeline của 1 cầu thủ qua nhiều trận
CREATE INDEX IF NOT EXISTS idx_pms_match_cov
    ON player_match_stats (match_id, league_id)
    INCLUDE (player_id, player, goals, assists, xg, xa, position, time);
CREATE INDEX IF NOT EXISTS idx_pms_player_cov
    ON player_match_stats (player_id, league_id)
    INCLUDE (match_id, goals, assists, xg, xa, xg_chain, time);

-- ============================================================
-- NHÓM C: CHILD TABLES (FBref)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 5. SQUAD ROSTERS (danh sách cầu thủ, FBref)
--    FK: (team_id, league_id) → standings
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS squad_rosters (
    player_id    TEXT,
    player_name  TEXT,
    player_url   TEXT,
    nationality  TEXT,
    position     TEXT,
    age          TEXT,
    age_years    INTEGER,
    team_name    TEXT,
    team_id      TEXT,
    season       TEXT,
    league_id    TEXT NOT NULL DEFAULT 'EPL',
    loaded_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, team_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_rosters_team  ON squad_rosters (team_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 6. SQUAD STATS (thống kê đội bóng, FBref)
--    FK: (team_id, league_id) → standings
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS squad_stats (
    team_id         TEXT,
    team_name       TEXT,
    season          TEXT,
    players_used    INTEGER,
    avg_age         REAL,
    possession      REAL,
    matches_played  INTEGER,
    goals           INTEGER,
    assists         INTEGER,
    pens_made       INTEGER,
    pens_att        INTEGER,
    yellow_cards    INTEGER,
    red_cards       INTEGER,
    goals_per90     REAL,
    assists_per90   REAL,
    league_id       TEXT NOT NULL DEFAULT 'EPL',
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, season, league_id)
);

-- ──────────────────────────────────────────────────────────
-- 7. PLAYER SEASON STATS (thống kê cầu thủ cả mùa, FBref)
--    FK: (team_id, league_id) → standings
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_season_stats (
    player_id             TEXT,
    player_name           TEXT,
    team_id               TEXT,
    team_name             TEXT,
    season                TEXT,
    nationality           TEXT,
    position              TEXT,
    age                   TEXT,
    matches_played        INTEGER,
    starts                INTEGER,
    minutes               INTEGER,
    minutes_90s           REAL,
    goals                 INTEGER,
    assists               INTEGER,
    goals_assists         INTEGER,
    goals_non_pen         INTEGER,
    pens_made             INTEGER,
    pens_att              INTEGER,
    shots                 REAL,
    shots_on_target       REAL,
    shots_on_target_pct   REAL,
    goals_per90           REAL,
    assists_per90         REAL,
    goals_assists_per90   REAL,
    yellow_cards          INTEGER,
    red_cards             INTEGER,
    league_id             TEXT NOT NULL DEFAULT 'EPL',
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, team_id, season, league_id)
);

CREATE INDEX IF NOT EXISTS idx_pss_team    ON player_season_stats (team_id, league_id);

-- ============================================================
-- NHÓM D: CROSS-SOURCE MAPPING
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 8. PLAYER CROSSREF — ánh xạ Understat ↔ FBref ↔ Transfermarkt IDs
--
--    Vấn đề: Understat dùng INTEGER id (VD: 8260),
--            FBref dùng TEXT slug (VD: "a23b4c5d/Bukayo-Saka").
--    Không thể JOIN trực tiếp shots.player_id = player_season_stats.player_id.
--
--    Giải pháp: Bảng này được build tự động sau khi load data,
--    bằng cách match player_name (có normalize: lower + strip).
--    matched_by: 'name_exact' | 'name_fuzzy' | 'manual'
--
--    Query mẫu (cross-source xG comparison):
--      SELECT s.player, s.xg AS understat_xg, p.goals
--      FROM shots s
--      JOIN player_crossref cx
--        ON s.player_id = cx.understat_player_id AND s.league_id = cx.league_id
--      JOIN player_season_stats p
--        ON cx.fbref_player_id = p.player_id AND cx.league_id = p.league_id
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_crossref (
    understat_player_id  BIGINT,
    fbref_player_id      TEXT    NOT NULL,
    tm_player_id         TEXT,
    canonical_name       TEXT,
    league_id            TEXT    NOT NULL DEFAULT 'EPL',
    matched_by           TEXT    DEFAULT 'name_exact',
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (fbref_player_id, league_id),
    UNIQUE (understat_player_id, fbref_player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_crossref_us    ON player_crossref (understat_player_id, league_id);
CREATE INDEX IF NOT EXISTS idx_crossref_fb    ON player_crossref (fbref_player_id, league_id);

-- ============================================================
-- NHÓM E: FBREF EXTRA TABLES
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 9. FIXTURES (lịch thi đấu, FBref)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fixtures (
    match_id         TEXT    NOT NULL,
    gameweek         INTEGER,
    date             TEXT,
    start_time       TEXT,
    dayofweek        TEXT,
    home_team        TEXT,
    home_xg          REAL,
    score            TEXT,
    away_xg          REAL,
    away_team        TEXT,
    attendance       TEXT,
    venue            TEXT,
    referee          TEXT,
    match_report_url TEXT,
    home_team_id     TEXT,
    away_team_id     TEXT,
    league_id        TEXT    NOT NULL DEFAULT 'EPL',
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (match_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_fix_date     ON fixtures (date);
CREATE INDEX IF NOT EXISTS idx_fix_home     ON fixtures (home_team_id, league_id);
CREATE INDEX IF NOT EXISTS idx_fix_away     ON fixtures (away_team_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 10. GK STATS (thống kê thủ môn, FBref)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS gk_stats (
    player_name                   TEXT,
    player_id                     TEXT    NOT NULL,
    team_name                     TEXT,
    team_id                       TEXT,
    season                        TEXT,
    nationality                   TEXT,
    position                      TEXT,
    age                           TEXT,
    gk_games                      INTEGER,
    gk_games_starts               INTEGER,
    minutes_gk                    INTEGER,
    gk_goals_against              INTEGER,
    gk_goals_against_per90        REAL,
    gk_shots_on_target_against    INTEGER,
    gk_saves                      INTEGER,
    gk_save_pct                   REAL,
    gk_wins                       INTEGER,
    gk_ties                       INTEGER,
    gk_losses                     INTEGER,
    gk_clean_sheets               INTEGER,
    gk_clean_sheets_pct           REAL,
    gk_pens_att                   INTEGER,
    gk_pens_allowed               INTEGER,
    gk_pens_saved                 INTEGER,
    gk_pens_missed                INTEGER,
    gk_psxg                       REAL,
    gk_psxg_per_shot_on_target    REAL,
    gk_passes_completed_launched  INTEGER,
    gk_passes_launched            INTEGER,
    gk_passes_pct_launched        REAL,
    gk_passes                     INTEGER,
    gk_passes_throws              INTEGER,
    gk_pct_passes_launched        REAL,
    gk_passes_length_avg          REAL,
    gk_goal_kicks                 INTEGER,
    gk_pct_goal_kicks_launched    REAL,
    gk_goal_kick_length_avg       REAL,
    gk_crosses_faced              INTEGER,
    gk_crosses_stopped            INTEGER,
    gk_crosses_stopped_pct        REAL,
    gk_def_actions_outside_pen_area INTEGER,
    gk_avg_distance_def_actions   REAL,
    league_id                     TEXT NOT NULL DEFAULT 'EPL',
    loaded_at                     TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, team_id, league_id)
);


-- ──────────────────────────────────────────────────────────
-- 11. PLAYER DEFENSIVE STATS (FBref)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_defensive_stats (
    player_id               TEXT,
    player_name             TEXT NOT NULL,
    team_id                 TEXT,
    team_name               TEXT,
    league_id               TEXT NOT NULL DEFAULT 'EPL',
    season                  TEXT NOT NULL,
    
    nationality             TEXT,
    position                TEXT,
    age                     TEXT,
    minutes_90s             NUMERIC,
    tackles                 INTEGER DEFAULT 0,
    tackles_won             INTEGER DEFAULT 0,
    tackles_def_3rd         INTEGER DEFAULT 0,
    tackles_mid_3rd         INTEGER DEFAULT 0,
    tackles_att_3rd         INTEGER DEFAULT 0,
    challenge_tackles       INTEGER DEFAULT 0,
    challenges              INTEGER DEFAULT 0,
    challenge_tackles_pct   NUMERIC,
    blocks                  INTEGER DEFAULT 0,
    blocked_shots           INTEGER DEFAULT 0,
    blocked_passes          INTEGER DEFAULT 0,
    interceptions           INTEGER DEFAULT 0,
    tackles_interceptions   INTEGER DEFAULT 0,
    clearances              INTEGER DEFAULT 0,
    errors                  INTEGER DEFAULT 0,
    pressures               INTEGER DEFAULT 0,
    pressure_regains        INTEGER DEFAULT 0,
    pressure_regain_pct     NUMERIC,
    pressures_def_3rd       INTEGER DEFAULT 0,
    pressures_mid_3rd       INTEGER DEFAULT 0,
    pressures_att_3rd       INTEGER DEFAULT 0,

    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, league_id, season)
);

CREATE INDEX IF NOT EXISTS idx_player_defensive_team ON player_defensive_stats (team_id);


-- ──────────────────────────────────────────────────────────
-- 12. PLAYER POSSESSION STATS (FBref)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_possession_stats (
    player_id               TEXT,
    player_name             TEXT NOT NULL,
    team_id                 TEXT,
    team_name               TEXT,
    league_id               TEXT NOT NULL DEFAULT 'EPL',
    season                  TEXT NOT NULL,
    
    nationality             TEXT,
    position                TEXT,
    age                     TEXT,
    minutes_90s             NUMERIC,
    touches                 INTEGER DEFAULT 0,
    touches_def_pen_area    INTEGER DEFAULT 0,
    touches_def_3rd         INTEGER DEFAULT 0,
    touches_mid_3rd         INTEGER DEFAULT 0,
    touches_att_3rd         INTEGER DEFAULT 0,
    touches_att_pen_area    INTEGER DEFAULT 0,
    touches_live_ball       INTEGER DEFAULT 0,
    take_ons                INTEGER DEFAULT 0,
    take_ons_won            INTEGER DEFAULT 0,
    take_ons_won_pct        NUMERIC,
    take_ons_tackled        INTEGER DEFAULT 0,
    take_ons_tackled_pct    NUMERIC,
    carries                 INTEGER DEFAULT 0,
    carries_distance        NUMERIC,
    carries_progressive_distance NUMERIC,
    progressive_carries     INTEGER DEFAULT 0,
    carries_into_final_third INTEGER DEFAULT 0,
    carries_into_penalty_area INTEGER DEFAULT 0,
    miscontrols             INTEGER DEFAULT 0,
    dispossessed            INTEGER DEFAULT 0,
    passes_received         INTEGER DEFAULT 0,
    progressive_passes_received INTEGER DEFAULT 0,

    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, league_id, season)
);

CREATE INDEX IF NOT EXISTS idx_player_possession_team ON player_possession_stats (team_id);


-- ============================================================
-- NHÓM F: SOFASCORE TABLES
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 13. SS_EVENTS (danh sách trận đấu SofaScore)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ss_events (
    event_id        BIGINT  NOT NULL,
    tournament_id   INTEGER,
    season_id       INTEGER,
    round_num       INTEGER,
    home_team       TEXT,
    home_team_id    INTEGER,
    away_team       TEXT,
    away_team_id    INTEGER,
    home_score      INTEGER,
    away_score      INTEGER,
    status          TEXT,
    start_timestamp BIGINT,
    match_date      TEXT,
    slug            TEXT,
    league_id       TEXT    NOT NULL DEFAULT 'EPL',
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_sse_date ON ss_events (match_date);

-- ──────────────────────────────────────────────────────────
-- 14. PLAYER AVG POSITIONS (SofaScore)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_avg_positions (
    event_id        BIGINT  NOT NULL,
    match_date      TEXT,
    home_team       TEXT,
    away_team       TEXT,
    player_id       BIGINT  NOT NULL,
    player_name     TEXT,
    team_name       TEXT,
    position        TEXT,
    jersey_number   INTEGER,
    avg_x           REAL,
    avg_y           REAL,
    minutes_played  INTEGER,
    rating          REAL,
    league_id       TEXT    NOT NULL DEFAULT 'EPL',
    season          TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_pap_event ON player_avg_positions (event_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 15. HEATMAPS (SofaScore — summary per player per match)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS heatmaps (
    event_id            BIGINT  NOT NULL,
    match_date          TEXT,
    home_team           TEXT,
    away_team           TEXT,
    score               TEXT,
    player_id           BIGINT  NOT NULL,
    player_name         TEXT,
    team_name           TEXT,
    position            TEXT,
    jersey_number       INTEGER,
    num_points          INTEGER,
    avg_x               REAL,
    avg_y               REAL,
    league_id           TEXT    NOT NULL DEFAULT 'EPL',
    season              TEXT,
    heatmap_points      JSONB,   -- JSON array of {x, y, v}
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, player_id, league_id)
);

ALTER TABLE heatmaps ALTER COLUMN heatmap_points SET STORAGE EXTENDED;

CREATE INDEX IF NOT EXISTS idx_hm_event ON heatmaps (event_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 16. MATCH LINEUPS (SofaScore — starting XI + subs + formation)
--      3-phase fetch: -60min (publish), -15min (refresh), post-match (stats)
--      No avg_x/avg_y → JOIN player_avg_positions when drawing pitch
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_lineups (
    event_id        BIGINT   NOT NULL,
    player_id       BIGINT   NOT NULL,
    player_name     TEXT,
    team_side       TEXT     NOT NULL,          -- 'home' | 'away'
    team_name       TEXT,
    position        TEXT,                       -- 'G', 'D', 'M', 'F'
    jersey_number   INTEGER,
    is_substitute   BOOLEAN  DEFAULT FALSE,
    minutes_played  INTEGER,                    -- NULL pre-match, filled post-match
    rating          REAL,                       -- NULL pre-match, filled post-match
    formation       TEXT,                       -- '4-3-3', '3-5-2' (denormalized per row)
    status          TEXT     DEFAULT 'confirmed',  -- future: 'predicted'
    league_id       TEXT     NOT NULL DEFAULT 'EPL',
    season          TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_lineups_event ON match_lineups (event_id, league_id);

-- ============================================================
-- NHÓM G: TRANSFERMARKT TABLES
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 17. TEAM METADATA (Transfermarkt)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_metadata (
    team_name               TEXT,
    team_id                 TEXT    NOT NULL,
    team_url                TEXT,
    league_id               TEXT    NOT NULL DEFAULT 'EPL',
    season                  TEXT,
    logo_url                TEXT,
    stadium_name            TEXT,
    stadium_capacity        TEXT,
    stadium_url             TEXT,
    manager_name            TEXT,
    manager_url             TEXT,
    manager_since           TEXT,
    manager_contract_until  TEXT,
    squad_size              INTEGER,
    avg_age                 REAL,
    num_foreigners          INTEGER,
    total_market_value      TEXT,
    formation               TEXT,
    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, league_id)
);

-- ──────────────────────────────────────────────────────────
-- 18. MARKET VALUES (Transfermarkt)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_values (
    player_name             TEXT,
    player_id               TEXT    NOT NULL,
    player_url              TEXT,
    player_image_url        TEXT,
    team_name               TEXT,
    team_id                 TEXT    NOT NULL,
    league_id               TEXT    NOT NULL DEFAULT 'EPL',
    season                  TEXT,
    position                TEXT,
    shirt_number            TEXT,
    date_of_birth           TEXT,
    age                     TEXT,
    nationality             TEXT,
    second_nationality      TEXT,
    height_cm               TEXT,
    foot                    TEXT,
    joined                  TEXT,
    contract_until          TEXT,
    market_value            TEXT,
    market_value_numeric    REAL,
    loaded_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, team_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_mv_team ON market_values (team_id, league_id);

-- ============================================================
-- FOREIGN KEY CONSTRAINTS
-- Dùng DO block để an toàn khi chạy lại (IF NOT EXISTS cho constraint).
-- Tất cả FK đều DEFERRABLE INITIALLY DEFERRED:
--   → FK chỉ được check tại COMMIT, không check từng statement.
--   → Linh hoạt hơn khi bulk load nhiều bảng trong 1 transaction.
-- ============================================================

-- ── Understat: shots → match_stats ──────────────────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_shots_match'
    ) THEN
        ALTER TABLE shots
            ADD CONSTRAINT fk_shots_match
            FOREIGN KEY (match_id, league_id)
            REFERENCES match_stats (match_id, league_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

-- ── Understat: player_match_stats → match_stats ─────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_pms_match'
    ) THEN
        ALTER TABLE player_match_stats
            ADD CONSTRAINT fk_pms_match
            FOREIGN KEY (match_id, league_id)
            REFERENCES match_stats (match_id, league_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

-- ── FBref: squad_rosters → standings ────────────────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_rosters_team'
    ) THEN
        ALTER TABLE squad_rosters
            ADD CONSTRAINT fk_rosters_team
            FOREIGN KEY (team_id, league_id, season)
            REFERENCES standings (team_id, league_id, season)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

-- ============================================================
-- NHÓM H: LIVE TRACKING & AI
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 19. LIVE SNAPSHOTS — Current match state (SofaScore)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS live_snapshots (
    event_id          BIGINT   NOT NULL PRIMARY KEY,
    home_team         TEXT,
    away_team         TEXT,
    home_score        INTEGER  DEFAULT 0,
    away_score        INTEGER  DEFAULT 0,
    status            TEXT,            -- notstarted, inprogress, finished
    minute            INTEGER  DEFAULT 0,
    statistics_json   JSONB,
    incidents_json    JSONB,
    insight_text      TEXT,
    poll_count        INTEGER  DEFAULT 0,
    loaded_at         TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
-- 20. MATCH SUMMARIES — AI-generated post-match narrative (30-second story)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_summaries (
    event_id        BIGINT   NOT NULL PRIMARY KEY,
    league_id       TEXT     NOT NULL,
    home_team       TEXT,
    away_team       TEXT,
    home_score      INTEGER,
    away_score      INTEGER,
    summary_text    TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
-- 21. PLAYER INSIGHTS — AI-generated nightly performance trends
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS player_insights (
    player_id       BIGINT   NOT NULL,
    player_name     TEXT,
    league_id       TEXT     NOT NULL,
    trend           TEXT,            -- GREEN, RED, NEUTRAL
    trend_score     INTEGER,         -- -100 to 100
    insight_text    TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (player_id, league_id)
);

-- ──────────────────────────────────────────────────────────
-- 22. NEWS & INJURY RADAR — RSS Feed Aggregator
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS news_feed (
    id              BIGSERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    link            TEXT UNIQUE NOT NULL,
    summary         TEXT,
    published_at    TIMESTAMPTZ,
    source          TEXT,
    league_id       TEXT DEFAULT 'EPL',
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- ──────────────────────────────────────────────────────────
-- 23. LIVE INCIDENTS — goals, cards, subs
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS live_incidents (
    id                BIGSERIAL  PRIMARY KEY,
    event_id          BIGINT     NOT NULL,
    incident_type     TEXT,              -- goal, card, substitution, varDecision
    minute            INTEGER,
    added_time        INTEGER,
    player_name       TEXT,
    player_in_name    TEXT,
    player_out_name   TEXT,
    is_home           BOOLEAN,
    detail            TEXT,              -- penalty, ownGoal, yellow, red, ...
    loaded_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_live_snap_status ON live_snapshots (status);
CREATE INDEX IF NOT EXISTS idx_live_inc_event   ON live_incidents (event_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_live_inc
    ON live_incidents (event_id, incident_type, minute, COALESCE(player_name, ''));

CREATE OR REPLACE FUNCTION cleanup_live_data(keep_days INTEGER DEFAULT 7)
RETURNS TABLE (deleted_snapshots INT, deleted_incidents INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_snap INT;
    v_inc  INT;
BEGIN
    DELETE FROM live_incidents
    WHERE loaded_at < NOW() - make_interval(days => keep_days)
      AND event_id NOT IN (
          SELECT event_id FROM live_snapshots WHERE status = 'inprogress'
      );
    GET DIAGNOSTICS v_inc = ROW_COUNT;
    DELETE FROM live_snapshots
    WHERE status = 'finished'
      AND loaded_at < NOW() - make_interval(days => keep_days);
    GET DIAGNOSTICS v_snap = ROW_COUNT;
    RETURN QUERY SELECT v_snap, v_inc;
END;
$$;

-- ============================================================
-- FK CONSTRAINTS (cuối cùng, sau khi tất cả bảng tồn tại)
-- ============================================================

-- ── FBref: squad_stats → standings ──────────────────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_squad_stats_team'
    ) THEN
        ALTER TABLE squad_stats
            ADD CONSTRAINT fk_squad_stats_team
            FOREIGN KEY (team_id, league_id, season)
            REFERENCES standings (team_id, league_id, season)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

-- ── FBref: player_season_stats → standings ───────────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_pss_team'
    ) THEN
        ALTER TABLE player_season_stats
            ADD CONSTRAINT fk_pss_team
            FOREIGN KEY (team_id, league_id, season)
            REFERENCES standings (team_id, league_id, season)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

ALTER DATABASE defaultdb SET random_page_cost    = 2.0;
ALTER DATABASE defaultdb SET effective_cache_size = '512MB';
ALTER DATABASE defaultdb SET work_mem             = '8MB';

-- ============================================================
-- VIEWS & MATERIALIZED VIEWS
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 24. mv_player_profiles: kết hợp 2 tầng matching để lấy ảnh từ Transfermarkt
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_profiles AS
SELECT DISTINCT ON (r.player_id, r.league_id)
    r.player_id AS fbref_player_id,
    r.player_name,
    r.team_id AS fbref_team_id,
    r.league_id,
    r.position,
    r.nationality,
    -- 2-Layer Match cho Ảnh và Giá trị
    COALESCE(mv1.player_image_url, mv2.player_image_url) AS player_image_url,
    COALESCE(mv1.market_value_numeric, mv2.market_value_numeric) AS market_value_numeric
FROM squad_rosters r
-- Tầng 1: Match qua ID (Crossref)
LEFT JOIN player_crossref c 
    ON c.fbref_player_id = r.player_id AND c.league_id = r.league_id
LEFT JOIN market_values mv1 
    ON mv1.player_id = c.tm_player_id AND mv1.league_id = r.league_id
-- Tầng 2: Fallback qua tên (Normalized)
LEFT JOIN market_values mv2
    ON mv2.league_id = r.league_id
    AND lower(regexp_replace(mv2.player_name, '\s+', '', 'g')) = lower(regexp_replace(r.player_name, '\s+', '', 'g'))
ORDER BY r.player_id, r.league_id, mv1.market_value_numeric DESC NULLS LAST, mv2.market_value_numeric DESC NULLS LAST;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_profiles_id ON mv_player_profiles(fbref_player_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 25. mv_team_profiles: kết hợp bảng xếp hạng với Transfermarkt để lấy logo
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_team_profiles AS
SELECT DISTINCT ON (t.team_id, t.league_id)
    t.team_id AS fbref_team_id,
    t.team_name,
    t.league_id,
    tm.logo_url,
    tm.stadium_name,
    tm.manager_name
FROM standings t
LEFT JOIN team_metadata tm 
    ON tm.league_id = t.league_id
    AND lower(regexp_replace(tm.team_name, '\s+', '', 'g')) = lower(regexp_replace(t.team_name, '\s+', '', 'g'))
ORDER BY t.team_id, t.league_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_team_profiles_id ON mv_team_profiles(fbref_team_id, league_id);
