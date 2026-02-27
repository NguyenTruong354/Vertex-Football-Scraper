-- ============================================================
-- schema.sql — Vertex Football Scraper · PostgreSQL DDL
-- ============================================================
-- Chạy một lần để tạo database schema.
-- Dùng IF NOT EXISTS → an toàn khi chạy lại.
--
-- Tables:
--   1. shots                — shot-level xG data (Understat)
--   2. player_match_stats   — player xG stats per match (Understat)
--   3. match_stats          — match-level aggregate (Understat)
--   4. standings            — league table standings (FBref)
--   5. squad_rosters        — player profiles per team (FBref)
--   6. squad_stats          — team summary stats (FBref)
--   7. player_season_stats  — player season totals (FBref)
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 1. SHOTS (xG data từ Understat)
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

CREATE INDEX IF NOT EXISTS idx_shots_player    ON shots (player_id);
CREATE INDEX IF NOT EXISTS idx_shots_match     ON shots (match_id);
CREATE INDEX IF NOT EXISTS idx_shots_season    ON shots (season);
CREATE INDEX IF NOT EXISTS idx_shots_result    ON shots (result);

-- ──────────────────────────────────────────────────────────
-- 2. PLAYER MATCH STATS (per-player xG per match)
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

CREATE INDEX IF NOT EXISTS idx_pms_player  ON player_match_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_pms_match   ON player_match_stats (match_id);
CREATE INDEX IF NOT EXISTS idx_pms_team    ON player_match_stats (team_id);

-- ──────────────────────────────────────────────────────────
-- 3. MATCH STATS (aggregate per match)
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

-- ──────────────────────────────────────────────────────────
-- 4. STANDINGS (bảng xếp hạng)
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
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, league_id)
);

-- ──────────────────────────────────────────────────────────
-- 5. SQUAD ROSTERS (danh sách cầu thủ)
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

CREATE INDEX IF NOT EXISTS idx_rosters_team  ON squad_rosters (team_id);

-- ──────────────────────────────────────────────────────────
-- 6. SQUAD STATS (thống kê đội bóng)
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
-- 7. PLAYER SEASON STATS (thống kê cầu thủ cả mùa)
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

CREATE INDEX IF NOT EXISTS idx_pss_team    ON player_season_stats (team_id);
CREATE INDEX IF NOT EXISTS idx_pss_season  ON player_season_stats (season);
