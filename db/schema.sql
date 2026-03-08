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
--  12b. match_passing_stats   — per-match passing data (SofaScore)
--  12c. match_player_advanced_stats — per-match advanced stats (SofaScore)
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
--  26. team_registry       — anchor table seeded from standings
--  27. team_canonical      — cross-source team name mapping
--  28. match_crossref      — cross-source match bridging
--  30. ai_insight_jobs     — AI insight pipeline queue
--  31. ai_insight_feedback — AI insight quality feedback
--  32. mv_player_complete_stats — player comparison MV (all sources)
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
-- 3. SHOTS (xG data từ Understat)  — RANGE-partitioned by season
--    PK: (season, league_id, id)
--    Partitions: shots_2024, shots_2025, shots_2026 (add more as needed)
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
    season           INTEGER      NOT NULL,
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
    PRIMARY KEY (season, league_id, id)
) PARTITION BY RANGE (season);

CREATE TABLE IF NOT EXISTS shots_2024 PARTITION OF shots
    FOR VALUES FROM (2024) TO (2025);
CREATE TABLE IF NOT EXISTS shots_2025 PARTITION OF shots
    FOR VALUES FROM (2025) TO (2026);
CREATE TABLE IF NOT EXISTS shots_2026 PARTITION OF shots
    FOR VALUES FROM (2026) TO (2027);

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
    updated_at   TIMESTAMPTZ,
    PRIMARY KEY (player_id, team_id, league_id, season)
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
    PRIMARY KEY (player_id, team_id, season, league_id),
    CONSTRAINT chk_season_format CHECK (season ~ '^\d{4}-\d{4}$')
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

-- Prevent many-FBref → 1-Understat mapping (allow NULL understat_player_id)
CREATE UNIQUE INDEX IF NOT EXISTS uq_crossref_understat_one
    ON player_crossref (understat_player_id, league_id)
    WHERE understat_player_id IS NOT NULL;

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
    updated_at                    TIMESTAMPTZ,
    PRIMARY KEY (player_id, team_id, league_id, season)
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


-- ──────────────────────────────────────────────────────────
-- 12b. MATCH PASSING STATS (SofaScore — per-player per-match passing)
--      Source: SofaScore lineups API (/event/{id}/lineups)
--      PK: (event_id, player_id, league_id)
--      Incremental: chỉ cào match mới, skip match đã có trong CSV
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_passing_stats (
    event_id                    BIGINT  NOT NULL,
    match_date                  TEXT,
    home_team                   TEXT,
    away_team                   TEXT,

    player_id                   BIGINT  NOT NULL,
    player_name                 TEXT    NOT NULL,
    team_name                   TEXT,
    position                    TEXT,
    minutes_played              INTEGER DEFAULT 0,

    -- Total passes
    total_pass                  INTEGER DEFAULT 0,
    accurate_pass               INTEGER DEFAULT 0,

    -- Long balls
    total_long_balls            INTEGER DEFAULT 0,
    accurate_long_balls         INTEGER DEFAULT 0,

    -- Crosses
    total_cross                 INTEGER DEFAULT 0,
    accurate_cross              INTEGER DEFAULT 0,

    -- Key passes
    key_pass                    INTEGER DEFAULT 0,

    -- Pass distribution
    total_own_half_passes       INTEGER DEFAULT 0,
    accurate_own_half_passes    INTEGER DEFAULT 0,
    total_opp_half_passes       INTEGER DEFAULT 0,
    accurate_opp_half_passes    INTEGER DEFAULT 0,

    -- Other
    touches                     INTEGER DEFAULT 0,
    expected_assists            NUMERIC,
    goal_assist                 INTEGER DEFAULT 0,
    possession_lost_ctrl        INTEGER DEFAULT 0,

    league_id                   TEXT    NOT NULL DEFAULT 'EPL',
    season                      TEXT,
    loaded_at                   TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_mps_event  ON match_passing_stats (event_id, league_id);
CREATE INDEX IF NOT EXISTS idx_mps_player ON match_passing_stats (player_id, league_id);
CREATE INDEX IF NOT EXISTS idx_mps_date   ON match_passing_stats (match_date);


-- ──────────────────────────────────────────────────────────
-- 12c. MATCH PLAYER ADVANCED STATS (SofaScore — per-player per-match)
--      Source: SofaScore lineups API (/event/{id}/lineups)
--      PK: (event_id, player_id, league_id)
--      Metrics: xGOT, duels, aerials, tackles, recoveries, clearances, GK saves
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_player_advanced_stats (
    event_id                        BIGINT  NOT NULL,
    match_date                      TEXT,
    home_team                       TEXT,
    away_team                       TEXT,

    player_id                       BIGINT  NOT NULL,
    player_name                     TEXT    NOT NULL,
    team_name                       TEXT,
    position                        TEXT,
    minutes_played                  INTEGER DEFAULT 0,

    -- Shooting
    expected_goals_on_target        NUMERIC,

    -- Chances
    big_chance_created              INTEGER DEFAULT 0,
    big_chance_missed               INTEGER DEFAULT 0,

    -- Duels
    duel_won                        INTEGER DEFAULT 0,
    duel_lost                       INTEGER DEFAULT 0,
    aerial_won                      INTEGER DEFAULT 0,
    aerial_lost                     INTEGER DEFAULT 0,

    -- Defensive
    interception_won                INTEGER DEFAULT 0,
    total_tackle                    INTEGER DEFAULT 0,
    ball_recovery                   INTEGER DEFAULT 0,
    total_clearance                 INTEGER DEFAULT 0,

    -- Goalkeeping
    goals_prevented                 NUMERIC,
    saves                           INTEGER DEFAULT 0,
    saved_shots_from_inside_the_box INTEGER DEFAULT 0,

    league_id                       TEXT    NOT NULL DEFAULT 'EPL',
    season                          TEXT,
    loaded_at                       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (event_id, player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_mpas_event  ON match_player_advanced_stats (event_id, league_id);
CREATE INDEX IF NOT EXISTS idx_mpas_player ON match_player_advanced_stats (player_id, league_id);
CREATE INDEX IF NOT EXISTS idx_mpas_date   ON match_player_advanced_stats (match_date);


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

-- JSONB validation: ensure heatmap_points is always a JSON array
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_heatmap_points_array'
    ) THEN
        ALTER TABLE heatmaps
            ADD CONSTRAINT chk_heatmap_points_array
            CHECK (heatmap_points IS NULL OR jsonb_typeof(heatmap_points) = 'array');
    END IF;
END $$;

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
-- FK references (team_id, league_id, season) which is now part of squad_rosters PK
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
    ON live_incidents (
        event_id, incident_type, minute,
        COALESCE(added_time, -1),
        COALESCE(player_name, ''), is_home
    );

-- ──────────────────────────────────────────────────────────
-- 29. LIVE MATCH STATE — lightweight live state (1 row/event)
--     Dual-write target: scheduler writes here + live_snapshots
--     FK → ss_events (event_id, league_id)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS live_match_state (
    event_id            BIGINT   NOT NULL,
    league_id           TEXT     NOT NULL,
    home_team           TEXT,
    away_team           TEXT,
    home_score          INTEGER  DEFAULT 0,
    away_score          INTEGER  DEFAULT 0,
    status              TEXT,
    minute              INTEGER  DEFAULT 0,
    poll_count          INTEGER  DEFAULT 0,
    insight_text        TEXT,
    stats_core_json     JSONB,
    last_processed_seq  BIGINT,
    flush_incomplete    BOOLEAN  NOT NULL DEFAULT FALSE,
    loaded_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ,
    PRIMARY KEY (event_id)
);

-- CHECK: stats_core_json must be a JSON object if not NULL
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_lms_stats_core_json_object'
    ) THEN
        ALTER TABLE live_match_state
            ADD CONSTRAINT chk_lms_stats_core_json_object
            CHECK (stats_core_json IS NULL OR jsonb_typeof(stats_core_json) = 'object');
    END IF;
END $$;

-- FK: live_match_state → ss_events
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_lms_event'
    ) THEN
        ALTER TABLE live_match_state
            ADD CONSTRAINT fk_lms_event
            FOREIGN KEY (event_id, league_id)
            REFERENCES ss_events (event_id, league_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_lms_status ON live_match_state(status);
CREATE INDEX IF NOT EXISTS idx_lms_league_status ON live_match_state(league_id, status);

-- Add seq cursor column to live_incidents for incremental streaming
ALTER TABLE live_incidents ADD COLUMN IF NOT EXISTS seq BIGSERIAL;
CREATE INDEX IF NOT EXISTS idx_live_inc_event_seq ON live_incidents(event_id, seq);

CREATE OR REPLACE FUNCTION cleanup_live_data(keep_days INTEGER DEFAULT 7)
RETURNS TABLE (deleted_snapshots INT, deleted_incidents INT)
LANGUAGE plpgsql AS $$
DECLARE
    v_snap INT;
    v_inc  INT;
    v_active_events BIGINT[];
BEGIN
    -- Snapshot active events first to avoid race between 2 DELETEs
    SELECT ARRAY_AGG(event_id) INTO v_active_events
    FROM live_snapshots WHERE status = 'inprogress';

    DELETE FROM live_incidents
    WHERE loaded_at < NOW() - make_interval(days => keep_days)
      AND event_id != ALL(COALESCE(v_active_events, '{}'));
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
-- 24a. mv_tm_player_candidates: pre-normalized TM candidates for player matching
--      Mandatory prerequisite for mv_player_profiles.
--      Refresh order: team_canonical → mv_tm_player_candidates → mv_player_profiles
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tm_player_candidates AS
SELECT
    mv.player_id                                              AS tm_player_id,
    mv.league_id,
    mv.team_id                                                AS tm_team_id,
    lower(regexp_replace(mv.player_name, '\s+', '', 'g'))     AS player_name_norm,
    lower(regexp_replace(mv.team_name,   '\s+', '', 'g'))     AS team_name_norm,
    mv.market_value_numeric,
    mv.player_image_url,
    mv.loaded_at
FROM market_values mv;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_tm_cand_pk
    ON mv_tm_player_candidates (tm_player_id, tm_team_id, league_id);
CREATE INDEX IF NOT EXISTS idx_mv_tm_cand_name_league
    ON mv_tm_player_candidates (player_name_norm, league_id);

-- ──────────────────────────────────────────────────────────
-- 24. mv_player_profiles: 3-priority matching with confidence metadata
--     Priority 1: crossref exact (1.00)
--     Priority 2: canonical team-constrained name (0.90)
--     Priority 3: name-only unique in league (0.60, guarded)
--     Unmatched: 0.00
--     Known limitation: mid-season transfer lag may cause team inconsistency
--     for name_only_unique rows — review after each transfer window.
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_profiles AS
WITH roster AS (
    SELECT DISTINCT ON (r.player_id, r.league_id)
        r.player_id,
        r.player_name,
        r.team_id,
        r.league_id,
        r.position,
        r.nationality,
        lower(regexp_replace(r.player_name, '\s+', '', 'g')) AS player_name_norm
    FROM squad_rosters r
    ORDER BY r.player_id, r.league_id, r.season DESC
),
candidates AS (
    -- Priority 1: crossref exact
    SELECT ro.player_id, ro.league_id,
           c.player_image_url, c.market_value_numeric, c.tm_player_id,
           1 AS priority, 'crossref_tm_id'::text AS matched_by, 1.00::numeric AS match_confidence
    FROM roster ro
    JOIN player_crossref xr
        ON xr.fbref_player_id = ro.player_id AND xr.league_id = ro.league_id
        AND xr.tm_player_id IS NOT NULL
    JOIN mv_tm_player_candidates c
        ON c.tm_player_id = xr.tm_player_id AND c.league_id = ro.league_id

    UNION ALL

    -- Priority 2: canonical team-constrained name
    SELECT ro.player_id, ro.league_id,
           c.player_image_url, c.market_value_numeric, c.tm_player_id,
           2, 'canonical_team_name_exact'::text, 0.90::numeric
    FROM roster ro
    JOIN team_canonical tc
        ON tc.fbref_team_id = ro.team_id AND tc.league_id = ro.league_id
        AND tc.tm_team_id IS NOT NULL
    JOIN mv_tm_player_candidates c
        ON c.league_id = ro.league_id
        AND c.player_name_norm = ro.player_name_norm
        AND (c.tm_team_id = tc.tm_team_id
             OR c.team_name_norm = lower(regexp_replace(COALESCE(tc.tm_name, ''), '\s+', '', 'g')))

    UNION ALL

    -- Priority 3: name-only (all candidates; filtered for uniqueness below)
    SELECT ro.player_id, ro.league_id,
           c.player_image_url, c.market_value_numeric, c.tm_player_id,
           3, 'name_only_unique'::text, 0.60::numeric
    FROM roster ro
    JOIN mv_tm_player_candidates c
        ON c.league_id = ro.league_id
        AND c.player_name_norm = ro.player_name_norm
),
best_priority AS (
    SELECT player_id, league_id, MIN(priority) AS best_p
    FROM candidates
    GROUP BY player_id, league_id
),
filtered AS (
    SELECT c.*
    FROM candidates c
    JOIN best_priority bp
        ON bp.player_id = c.player_id AND bp.league_id = c.league_id
        AND c.priority = bp.best_p
),
p3_unique_check AS (
    SELECT player_id, league_id, COUNT(DISTINCT tm_player_id) AS distinct_players
    FROM filtered
    WHERE priority = 3
    GROUP BY player_id, league_id
),
final_filtered AS (
    SELECT f.*
    FROM filtered f
    LEFT JOIN p3_unique_check uc
        ON uc.player_id = f.player_id AND uc.league_id = f.league_id
    WHERE f.priority < 3
       OR (f.priority = 3 AND uc.distinct_players = 1)
),
best AS (
    SELECT DISTINCT ON (player_id, league_id)
        player_id, league_id, player_image_url, market_value_numeric,
        matched_by, match_confidence
    FROM final_filtered
    ORDER BY player_id, league_id,
             match_confidence DESC,
             market_value_numeric DESC NULLS LAST
)
SELECT
    ro.player_id AS fbref_player_id,
    ro.player_name,
    ro.team_id   AS fbref_team_id,
    ro.league_id,
    ro.position,
    ro.nationality,
    b.player_image_url,
    b.market_value_numeric,
    COALESCE(b.matched_by, 'unmatched')         AS matched_by,
    COALESCE(b.match_confidence, 0.00)::numeric  AS match_confidence
FROM roster ro
LEFT JOIN best b ON b.player_id = ro.player_id AND b.league_id = ro.league_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_profiles_id
    ON mv_player_profiles (fbref_player_id, league_id);

-- ──────────────────────────────────────────────────────────
-- 25. mv_team_profiles: canonical mapping preferred, name fallback secondary
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_team_profiles AS
SELECT DISTINCT ON (t.team_id, t.league_id)
    t.team_id   AS fbref_team_id,
    t.team_name,
    t.league_id,
    COALESCE(tm1.logo_url,       tm2.logo_url)       AS logo_url,
    COALESCE(tm1.stadium_name,   tm2.stadium_name)   AS stadium_name,
    COALESCE(tm1.manager_name,   tm2.manager_name)   AS manager_name,
    CASE
        WHEN tm1.team_id IS NOT NULL THEN 'canonical_tm_id'
        WHEN tm2.team_id IS NOT NULL THEN 'name_fallback'
        ELSE 'unmatched'
    END::text AS matched_by,
    CASE
        WHEN tm1.team_id IS NOT NULL THEN 1.00
        WHEN tm2.team_id IS NOT NULL THEN 0.70
        ELSE 0.00
    END::numeric AS match_confidence
FROM standings t
-- Priority 1: canonical mapping via team_canonical
LEFT JOIN team_canonical tc
    ON tc.fbref_team_id = t.team_id AND tc.league_id = t.league_id
LEFT JOIN team_metadata tm1
    ON tm1.team_id = tc.tm_team_id AND tm1.league_id = t.league_id
-- Priority 2: name fallback
LEFT JOIN team_metadata tm2
    ON tm2.league_id = t.league_id
    AND lower(regexp_replace(tm2.team_name, '\s+', '', 'g'))
      = lower(regexp_replace(t.team_name,  '\s+', '', 'g'))
ORDER BY t.team_id, t.league_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_team_profiles_id
    ON mv_team_profiles (fbref_team_id, league_id);


-- ──────────────────────────────────────────────────────────
-- 31b. mv_shot_agg: pre-aggregated Understat shots
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_shot_agg AS
SELECT
    s.player_id  AS understat_player_id,
    s.league_id,
    s.season     AS shot_season,
    SUM(s.xg)                                         AS total_xg,
    COUNT(*) FILTER (WHERE s.result = 'Goal')         AS goals_from_shots
FROM shots s
GROUP BY s.player_id, s.league_id, s.season;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_shot_agg_pk
    ON mv_shot_agg(understat_player_id, league_id, shot_season);

-- ──────────────────────────────────────────────────────────
-- 32. mv_player_complete_stats: single source of truth cho player comparison
--     Grain: 1 row per (player_id, league_id, season)
--     Sources: player_season_stats + crossref + shots(Understat) +
--              defensive + possession + gk_stats + mv_player_profiles
-- ──────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_complete_stats AS
WITH base AS (
    -- Anchor: pick primary team row (highest minutes) per player/league/season
    SELECT DISTINCT ON (pss.player_id, pss.league_id, pss.season)
        pss.player_id,
        pss.player_name,
        pss.team_id,
        pss.league_id,
        pss.season,
        pss.position,
        pss.nationality,
        pss.minutes_90s,
        pss.goals,
        pss.assists,
        pss.goals_per90,
        pss.shots         AS pss_shots,
        pss.shots_on_target_pct,
        pss.yellow_cards,
        pss.red_cards
    FROM player_season_stats pss
    ORDER BY pss.player_id, pss.league_id, pss.season,
             pss.minutes_90s DESC NULLS LAST
)
SELECT
    -- Identity / profile
    b.player_id,
    b.player_name,
    b.team_id,
    b.league_id,
    b.season,
    b.position,
    b.nationality,
    pp.player_image_url,
    pp.market_value_numeric,

    -- Attacking / core
    b.minutes_90s,
    b.goals,
    b.assists,
    b.goals_per90,
    b.pss_shots        AS shots,
    b.shots_on_target_pct,
    b.yellow_cards,
    b.red_cards,

    -- Understat shot context
    (cx.understat_player_id IS NOT NULL)  AS has_understat_crossref,
    CASE
        WHEN cx.understat_player_id IS NULL THEN NULL          -- no crossref → unknown
        ELSE COALESCE(sa.total_xg, 0)
    END                                   AS total_xg,
    CASE
        WHEN cx.understat_player_id IS NULL THEN NULL
        WHEN b.minutes_90s IS NULL OR b.minutes_90s = 0 THEN NULL
        ELSE COALESCE(sa.total_xg, 0) / b.minutes_90s
    END                                   AS xg_per90,
    CASE
        WHEN cx.understat_player_id IS NULL THEN NULL
        ELSE COALESCE(sa.goals_from_shots, 0)
    END                                   AS goals_from_shots,
    CASE
        WHEN cx.understat_player_id IS NULL THEN NULL
        ELSE COALESCE(sa.goals_from_shots, 0) - COALESCE(sa.total_xg, 0)
    END                                   AS xg_overperformance,

    -- Defensive
    d.tackles,
    d.interceptions,
    d.blocks,
    d.clearances,
    d.pressures,

    -- Possession / progression
    po.progressive_carries,
    po.progressive_passes_received,
    po.take_ons_won_pct,

    -- Goalkeeper subset (NULL for non-GK)
    gk.gk_save_pct,
    gk.gk_clean_sheets,

    -- Metadata
    NOW()  AS built_at

FROM base b

-- Crossref for Understat mapping
LEFT JOIN player_crossref cx
    ON cx.fbref_player_id = b.player_id
   AND cx.league_id       = b.league_id

-- Understat shot aggregates
LEFT JOIN mv_shot_agg sa
    ON sa.understat_player_id = cx.understat_player_id
   AND sa.league_id           = b.league_id
   AND sa.shot_season         = SPLIT_PART(b.season, '-', 1)::INTEGER

-- Defensive stats
LEFT JOIN player_defensive_stats d
    ON d.player_id  = b.player_id
   AND d.league_id  = b.league_id
   AND d.season     = b.season

-- Possession stats
LEFT JOIN player_possession_stats po
    ON po.player_id  = b.player_id
   AND po.league_id  = b.league_id
   AND po.season     = b.season

-- GK stats: LATERAL pick primary stint (most games) to prevent fanout
-- when a GK transfers mid-season and has 2 rows for same (player,league,season)
LEFT JOIN LATERAL (
    SELECT gk2.gk_save_pct, gk2.gk_clean_sheets
    FROM gk_stats gk2
    WHERE gk2.player_id = b.player_id
      AND gk2.league_id = b.league_id
      AND gk2.season    = b.season
    ORDER BY gk2.gk_games DESC NULLS LAST
    LIMIT 1
) gk ON TRUE

-- Profile (image + market value)
LEFT JOIN mv_player_profiles pp
    ON pp.fbref_player_id = b.player_id
   AND pp.league_id       = b.league_id;

-- Indexes for REFRESH CONCURRENTLY and common queries
CREATE UNIQUE INDEX IF NOT EXISTS idx_mvpcs_pk
    ON mv_player_complete_stats(player_id, league_id, season);
CREATE INDEX IF NOT EXISTS idx_mvpcs_league_season
    ON mv_player_complete_stats(league_id, season);
CREATE INDEX IF NOT EXISTS idx_mvpcs_team
    ON mv_player_complete_stats(team_id, league_id, season);


-- ============================================================
-- NHÓM K: TEAM CANONICAL + MATCH CROSSREF (Tầng 2 foundation)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 26. TEAM_REGISTRY — anchor table seeded from standings
--     PK: (league_id, fbref_team_id)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_registry (
    league_id       TEXT NOT NULL,
    fbref_team_id   TEXT NOT NULL,
    fbref_team_name TEXT,
    is_active       BOOLEAN DEFAULT TRUE,
    loaded_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ,
    PRIMARY KEY (league_id, fbref_team_id)
);

-- ──────────────────────────────────────────────────────────
-- 27. TEAM_CANONICAL — cross-source team name mapping
--     FK → team_registry
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS team_canonical (
    league_id          TEXT NOT NULL,
    fbref_team_id      TEXT NOT NULL,
    canonical_name     TEXT NOT NULL,

    fbref_name         TEXT,
    understat_name     TEXT,
    sofascore_name     TEXT,
    tm_name            TEXT,

    sofascore_team_id  INTEGER,
    tm_team_id         TEXT,

    is_active          BOOLEAN DEFAULT TRUE,
    matched_by         TEXT DEFAULT 'manual_seed',
    loaded_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at         TIMESTAMPTZ,

    PRIMARY KEY (league_id, fbref_team_id),
    CONSTRAINT fk_tc_team_registry
        FOREIGN KEY (league_id, fbref_team_id)
        REFERENCES team_registry (league_id, fbref_team_id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_team_canonical_league_name
    ON team_canonical (league_id, canonical_name);

CREATE UNIQUE INDEX IF NOT EXISTS uq_team_canonical_sofascore_id
    ON team_canonical (league_id, sofascore_team_id)
    WHERE sofascore_team_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_team_canonical_tm_id
    ON team_canonical (league_id, tm_team_id)
    WHERE tm_team_id IS NOT NULL;

-- ──────────────────────────────────────────────────────────
-- 28. MATCH_CROSSREF — cross-source match bridging
--     FK → team_registry (home + away)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS match_crossref (
    crossref_id           BIGSERIAL PRIMARY KEY,
    understat_match_id    BIGINT,
    fbref_match_id        TEXT,
    sofascore_event_id    BIGINT,

    home_fbref_team_id    TEXT NOT NULL,
    away_fbref_team_id    TEXT NOT NULL,
    match_date            DATE NOT NULL,
    original_date         DATE,
    is_rescheduled        BOOLEAN DEFAULT FALSE,

    league_id             TEXT NOT NULL DEFAULT 'EPL',
    season                TEXT,

    matched_by            TEXT DEFAULT 'auto_exact',
    confidence            REAL DEFAULT 1.0,
    notes                 TEXT,
    loaded_at             TIMESTAMPTZ DEFAULT NOW(),
    updated_at            TIMESTAMPTZ,

    CONSTRAINT fk_mcr_home_team
        FOREIGN KEY (league_id, home_fbref_team_id)
        REFERENCES team_registry (league_id, fbref_team_id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_mcr_away_team
        FOREIGN KEY (league_id, away_fbref_team_id)
        REFERENCES team_registry (league_id, fbref_team_id)
        ON DELETE RESTRICT,
    CONSTRAINT chk_rescheduled_has_original
        CHECK (is_rescheduled = FALSE OR original_date IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mcr_understat
    ON match_crossref (league_id, understat_match_id)
    WHERE understat_match_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mcr_fbref
    ON match_crossref (league_id, fbref_match_id)
    WHERE fbref_match_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_mcr_sofascore
    ON match_crossref (league_id, sofascore_event_id)
    WHERE sofascore_event_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_mcr_fixture_key
    ON match_crossref (league_id, season, home_fbref_team_id, away_fbref_team_id, match_date);

CREATE INDEX IF NOT EXISTS idx_mcr_understat ON match_crossref (understat_match_id);
CREATE INDEX IF NOT EXISTS idx_mcr_fbref     ON match_crossref (fbref_match_id);
CREATE INDEX IF NOT EXISTS idx_mcr_sofascore ON match_crossref (sofascore_event_id);


-- ============================================================
-- NHÓM L: AI INSIGHT PIPELINE
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 30. AI_INSIGHT_JOBS — queue/orchestration for AI insight generation
--     Managed by worker (pick → execute → gate → publish/drop).
--     Producer inserts with ON CONFLICT DO NOTHING on dedupe_key.
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_insight_jobs (
    id               BIGSERIAL PRIMARY KEY,
    job_type         TEXT NOT NULL,      -- live_badge | match_story | player_trend
    event_id         BIGINT,
    league_id        TEXT,
    team_focus       TEXT,
    status           TEXT NOT NULL DEFAULT 'queued',
    priority         INTEGER NOT NULL DEFAULT 100,
    dedupe_key       TEXT,               -- business-key for active job uniqueness
    fingerprint      TEXT,               -- SHA256 of canonical payload for analytics
    payload_json     JSONB NOT NULL,
    prompt_version   TEXT NOT NULL DEFAULT 'v1',
    provider_used    TEXT,
    model_used       TEXT,
    attempt_count    INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    next_retry_at    TIMESTAMPTZ,
    lease_until      TIMESTAMPTZ,
    worker_id        TEXT,
    result_text      TEXT,
    reason_code      TEXT,               -- near_duplicate | cooldown_block | low_signal | lease_expired
    is_published     BOOLEAN NOT NULL DEFAULT FALSE,
    published_at     TIMESTAMPTZ,
    error_code       TEXT,
    error_message    TEXT,
    latency_ms       INTEGER,
    input_tokens     INTEGER,
    output_tokens    INTEGER,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ,
    updated_at       TIMESTAMPTZ
);

-- CHECK: status enum
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ai_jobs_status'
    ) THEN
        ALTER TABLE ai_insight_jobs
            ADD CONSTRAINT chk_ai_jobs_status
            CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'dropped'));
    END IF;
END $$;

-- CHECK: job_type enum
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ai_jobs_type'
    ) THEN
        ALTER TABLE ai_insight_jobs
            ADD CONSTRAINT chk_ai_jobs_type
            CHECK (job_type IN ('live_badge', 'match_story', 'player_trend'));
    END IF;
END $$;

-- CHECK: is_published = TRUE implies published_at IS NOT NULL
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_published_consistency'
    ) THEN
        ALTER TABLE ai_insight_jobs
            ADD CONSTRAINT chk_published_consistency
            CHECK (is_published = FALSE OR published_at IS NOT NULL);
    END IF;
END $$;

-- Pick query index (league-aware worker)
CREATE INDEX IF NOT EXISTS idx_ai_jobs_pick
    ON ai_insight_jobs (league_id, status, priority, next_retry_at, created_at);

CREATE INDEX IF NOT EXISTS idx_ai_jobs_event
    ON ai_insight_jobs (event_id, created_at DESC);

-- Published lookup (novelty gate source)
CREATE INDEX IF NOT EXISTS idx_ai_jobs_published
    ON ai_insight_jobs (event_id, job_type, is_published, published_at DESC);

-- Partial unique for active dedupe (ON CONFLICT target)
CREATE UNIQUE INDEX IF NOT EXISTS uq_ai_jobs_dedupe_active
    ON ai_insight_jobs (dedupe_key)
    WHERE status IN ('queued', 'running');

-- ──────────────────────────────────────────────────────────
-- 31. AI_INSIGHT_FEEDBACK — consumer/manual quality feedback
--     FK → ai_insight_jobs ON DELETE RESTRICT (preserve feedback data)
-- ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ai_insight_feedback (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL,
    event_id        BIGINT,
    league_id       TEXT,
    channel         TEXT,           -- discord | api | manual_review
    feedback_type   TEXT NOT NULL,  -- upvote | downvote | duplicate | irrelevant | too_generic
    score           SMALLINT,       -- -2..+2
    tags            TEXT[],
    comment         TEXT,
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- FK: feedback → jobs (RESTRICT — never cascade-delete feedback)
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_ai_feedback_job'
    ) THEN
        ALTER TABLE ai_insight_feedback
            ADD CONSTRAINT fk_ai_feedback_job
            FOREIGN KEY (job_id)
            REFERENCES ai_insight_jobs(id)
            ON DELETE RESTRICT;
    END IF;
END $$;

-- CHECK: score range
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ai_feedback_score'
    ) THEN
        ALTER TABLE ai_insight_feedback
            ADD CONSTRAINT chk_ai_feedback_score
            CHECK (score IS NULL OR score BETWEEN -2 AND 2);
    END IF;
END $$;

-- CHECK: feedback_type enum
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_ai_feedback_type'
    ) THEN
        ALTER TABLE ai_insight_feedback
            ADD CONSTRAINT chk_ai_feedback_type
            CHECK (feedback_type IN ('upvote', 'downvote', 'duplicate', 'irrelevant', 'too_generic'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ai_feedback_job
    ON ai_insight_feedback (job_id);

CREATE INDEX IF NOT EXISTS idx_ai_feedback_event
    ON ai_insight_feedback (event_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_feedback_type_time
    ON ai_insight_feedback (feedback_type, created_at DESC);


-- ============================================================
-- NHÓM J: UPDATED_AT TRIGGER + COLUMNS
-- ============================================================
-- Tự động set updated_at = NOW() khi row bị UPDATE.
-- Giúp debug data cũ vs mới, biết row đến từ lần cào nào.

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- Thêm cột updated_at vào các bảng quan trọng (idempotent)
ALTER TABLE match_stats             ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE standings               ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE shots                   ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE player_match_stats      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE squad_stats             ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE player_season_stats     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE player_defensive_stats  ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE player_possession_stats ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE fixtures                ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE ss_events               ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE match_passing_stats     ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE match_player_advanced_stats ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE market_values           ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE player_crossref         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE team_registry           ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE team_canonical          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE match_crossref          ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE live_match_state        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
ALTER TABLE ai_insight_jobs         ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

-- Tạo trigger cho mỗi bảng (DROP IF EXISTS → idempotent)
DO $$ 
DECLARE
    tbl TEXT;
BEGIN
    FOR tbl IN SELECT unnest(ARRAY[
        'match_stats', 'standings', 'shots', 'player_match_stats',
        'squad_rosters', 'squad_stats', 'player_season_stats',
        'player_defensive_stats', 'player_possession_stats',
        'fixtures', 'gk_stats', 'ss_events',
        'match_passing_stats', 'match_player_advanced_stats',
        'market_values', 'player_crossref',
        'team_registry', 'team_canonical', 'match_crossref',
        'live_match_state', 'ai_insight_jobs'
    ])
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_%s_updated ON %I; '
            'CREATE TRIGGER trg_%s_updated '
            'BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION set_updated_at();',
            tbl, tbl, tbl, tbl
        );
    END LOOP;
END $$;
