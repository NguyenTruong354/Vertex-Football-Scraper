-- SQL Script tạo Partition cho mùa 2023
-- Chạy script này trong PostgreSQL console

-- 1. shots
CREATE TABLE IF NOT EXISTS shots_2023 PARTITION OF shots FOR VALUES FROM (2023) TO (2024);

-- 2. standings
CREATE TABLE IF NOT EXISTS standings_2023 PARTITION OF standings FOR VALUES FROM (2023) TO (2024);

-- 3. squad_rosters
CREATE TABLE IF NOT EXISTS squad_rosters_2023 PARTITION OF squad_rosters FOR VALUES FROM (2023) TO (2024);

-- 4. squad_stats
CREATE TABLE IF NOT EXISTS squad_stats_2023 PARTITION OF squad_stats FOR VALUES FROM (2023) TO (2024);

-- 5. player_season_stats
CREATE TABLE IF NOT EXISTS player_season_stats_2023 PARTITION OF player_season_stats FOR VALUES FROM (2023) TO (2024);

-- 6. player_defensive_stats
CREATE TABLE IF NOT EXISTS player_defensive_stats_2023 PARTITION OF player_defensive_stats FOR VALUES FROM (2023) TO (2024);

-- 7. player_possession_stats
CREATE TABLE IF NOT EXISTS player_possession_stats_2023 PARTITION OF player_possession_stats FOR VALUES FROM (2023) TO (2024);

-- 8. gk_stats
CREATE TABLE IF NOT EXISTS gk_stats_2023 PARTITION OF gk_stats FOR VALUES FROM (2023) TO (2024);


-- 9. (Optional) match_stats nếu có dùng partition season
-- CREATE TABLE IF NOT EXISTS match_stats_2023 PARTITION OF match_stats FOR VALUES IN (2023);
