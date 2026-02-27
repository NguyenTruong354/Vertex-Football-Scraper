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
--   8. player_crossref      — ánh xạ Understat ↔ FBref player IDs
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
    loaded_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (team_id, league_id)
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
CREATE INDEX IF NOT EXISTS idx_shots_match   ON shots (match_id, league_id);
-- Index player_id: query "tất cả shot của cầu thủ Y"
CREATE INDEX IF NOT EXISTS idx_shots_player  ON shots (player_id);
CREATE INDEX IF NOT EXISTS idx_shots_season  ON shots (season);
CREATE INDEX IF NOT EXISTS idx_shots_result  ON shots (result);

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
CREATE INDEX IF NOT EXISTS idx_pms_match   ON player_match_stats (match_id, league_id);
-- Index player_id: query timeline của 1 cầu thủ qua nhiều trận
CREATE INDEX IF NOT EXISTS idx_pms_player  ON player_match_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_pms_team    ON player_match_stats (team_id);

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
CREATE INDEX IF NOT EXISTS idx_pss_season  ON player_season_stats (season);

-- ============================================================
-- NHÓM D: CROSS-SOURCE MAPPING
-- ============================================================

-- ──────────────────────────────────────────────────────────
-- 8. PLAYER CROSSREF — ánh xạ Understat player_id ↔ FBref player_id
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
    understat_player_id  BIGINT  NOT NULL,
    fbref_player_id      TEXT    NOT NULL,
    canonical_name       TEXT,
    league_id            TEXT    NOT NULL DEFAULT 'EPL',
    matched_by           TEXT    DEFAULT 'name_exact',
    loaded_at            TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (understat_player_id, fbref_player_id, league_id)
);

CREATE INDEX IF NOT EXISTS idx_crossref_us    ON player_crossref (understat_player_id, league_id);
CREATE INDEX IF NOT EXISTS idx_crossref_fb    ON player_crossref (fbref_player_id, league_id);

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
            FOREIGN KEY (team_id, league_id)
            REFERENCES standings (team_id, league_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

-- ── FBref: squad_stats → standings ──────────────────────────
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'fk_squad_stats_team'
    ) THEN
        ALTER TABLE squad_stats
            ADD CONSTRAINT fk_squad_stats_team
            FOREIGN KEY (team_id, league_id)
            REFERENCES standings (team_id, league_id)
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
            FOREIGN KEY (team_id, league_id)
            REFERENCES standings (team_id, league_id)
            ON DELETE CASCADE
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;
