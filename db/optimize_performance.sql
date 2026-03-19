-- ============================================================
-- Vertex Football Scraper - Database Optimization Log
-- Last Updated: 2026-03-20 
-- Purpose: Fix performance issues identified by Aiven Insights
-- ============================================================

-- 1. Remove redundant indexes to speed up INSERT performance
-- Covered by idx_live_inc_event_seq (event_id, seq)
DROP INDEX IF EXISTS idx_live_inc_event;

-- Covered by Primary Key (fbref_player_id, league_id)
DROP INDEX IF EXISTS idx_crossref_fb;


-- 2. Add performance index for heavy queries
-- Speeds up SELECT * FROM heatmaps ORDER BY event_id, player_id
CREATE INDEX IF NOT EXISTS idx_heatmaps_event_player ON heatmaps(event_id, player_id);


-- 3. Maintenance recommendations
-- VACUUM ANALYZE player_match_stats;
-- VACUUM ANALYZE live_incidents;
-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_player_complete_stats;
