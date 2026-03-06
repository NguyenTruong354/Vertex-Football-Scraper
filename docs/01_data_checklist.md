# Data Checklist — Vertex Football Scraper
> Cập nhật: 06/03/2026 | Trạng thái: 4 nguồn scraping + PostgreSQL + Live Tracker + Scheduler Master
> File output theo league: `output/{source}/{league}/dataset_{league}_*.csv`

---

## Tổng quan nguồn dữ liệu

| # | Nguồn | Pipeline | Tables trong DB | Trạng thái |
|---|-------|----------|-----------------|-----------|
| 1 | **Understat** | `understat/async_scraper.py` | `match_stats`, `shots`, `player_match_stats` | ✅ Production |
| 2 | **FBref** | `fbref/fbref_scraper.py` | `standings`, `squad_rosters`, `squad_stats`, `player_season_stats`, `fixtures`, `gk_stats`, `player_defensive_stats`, `player_possession_stats` | ✅ Production |
| 3 | **SofaScore** | `sofascore/sofascore_client.py` | `ss_events`, `player_avg_positions`, `heatmaps`, `match_lineups` | ✅ Production |
| 4 | **Transfermarkt** | `transfermarkt/tm_scraper.py` | `team_metadata`, `market_values` | ✅ Production |
| 5 | **Live Tracker** | `live_match.py` / `scheduler_master.py` | `live_snapshots`, `live_incidents` | ✅ Production |
| — | **Cross-source** | `db/loader.py` (auto-build) | `player_crossref` | ✅ Production |
| — | **Analytics** | SQL Materialized Views | `mv_player_profiles`, `mv_team_profiles` | ✅ Production |

---

## 📦 Nguồn 1: Understat
*(Giữ nguyên nội dung cũ)*

---

## 📦 Nguồn 2: FBref

*Script: `fbref/fbref_scraper.py` | Kỹ thuật: nodriver (Chrome headed) — bypass Cloudflare*

### 4. League Standings — bảng `standings`
(Giữ nguyên)

### 5. Squad Stats — bảng `squad_stats`
(Giữ nguyên)

### 6. Squad Rosters — bảng `squad_rosters`
(Giữ nguyên)

### 7. Player Season Stats — bảng `player_season_stats`
(Giữ nguyên)

### 8. Fixtures — bảng `fixtures`
(Giữ nguyên)

### 9. Goalkeeper Stats — bảng `gk_stats`
(Giữ nguyên)

### 9a. Player Defensive Stats — bảng `player_defensive_stats`
*Scraped from League-wide stats page*

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` | TEXT | FBref player ID (Slug) |
| `player_name` | TEXT | Tên cầu thủ |
| `team_id` / `team_name` | TEXT | ID & Tên đội |
| `nationality` / `position` / `age` | TEXT | Metadata |
| `minutes_90s` | NUMERIC | Số trận 90p |
| `tackles` / `tackles_won` | INTEGER | Tắc bóng / Thắng tắc bóng |
| `interceptions` | INTEGER | Cắt bóng |
| `blocks` / `clearances` / `errors` | INTEGER | Chặn / Phá bóng / Lỗi dẫn đến cú sút |
| `pressures` / `pressure_regains` | INTEGER | Áp lực / Giành lại bóng sau áp lực |

### 9b. Player Possession Stats — bảng `player_possession_stats`
*Scraped from League-wide stats page*

| Field | Kiểu | Mô tả |
|---|---|---|
| `player_id` | TEXT | FBref player ID |
| `player_name` | TEXT | Tên cầu thủ |
| `team_id` / `team_name` | TEXT | ID & Tên đội |
| `minutes_90s` | NUMERIC | |
| `touches` / `touches_att_pen_area` | INTEGER | Chạm bóng / Chạm bóng trong vòng cấm đối phương |
| `take_ons` / `take_ons_won` | INTEGER | Qua người / Qua người thành công |
| `carries` / `progressive_carries` | INTEGER | Dẫn bóng / Dẫn bóng tịnh tiến |
| `dispossessed` / `miscontrols` | INTEGER | Bị mất bóng / Chống bóng hỏng |

---

## 📦 Nguồn 3: SofaScore

*Script: `sofascore/sofascore_client.py` | `scheduler_master.py`*

### 10. Match Events — bảng `ss_events`
(Giữ nguyên)

### 11. Player Average Positions — bảng `player_avg_positions`
(Giữ nguyên)

### 12. Heatmaps — bảng `heatmaps`
(Giữ nguyên)

### 12a. Match Lineups — bảng `match_lineups`
*Fetched in 3 phases: 60m before, 15m before, and post-match stats*

| Field | Kiểu | Mô tả |
|---|---|---|
| `event_id` | BIGINT | Match ID |
| `player_id` | BIGINT | Player ID |
| `player_name` | TEXT | Tên cầu thủ |
| `team_side` | TEXT | `home` hoặc `away` |
| `formation` | TEXT | Sơ đồ (VD: `4-3-3`) |
| `is_substitute` | BOOLEAN | Dự bị hay đá chính |
| `rating` | REAL | Điểm số (cập nhật sau trận) |

---

## 📦 Nguồn 4: Transfermarkt
(Giữ nguyên)

---

## 📦 Nguồn 5: Live Match Tracker
(Giữ nguyên)

---

## 🔗 Cross-Source Mapping

### 17. Player Crossref — bảng `player_crossref`

| Field | Kiểu | Mô tả |
|---|---|---|
| `understat_player_id` | BIGINT | |
| `fbref_player_id` | TEXT | |
| `tm_player_id` | TEXT | **Transfermarkt Player ID (Mới)** |
| `canonical_name` | TEXT | |
| `matched_by` | TEXT | |

---

## 📊 Analytics & Views

### 18. Materialized Profiles
*Tối ưu cho Frontend query ảnh và thông tin cầu thủ*

- `mv_player_profiles`: Kết hợp FBref + Transfermarkt (để lấy `player_image_url`).
- `mv_team_profiles`: Kết hợp FBref + Transfermarkt (để lấy `logo_url`).

---

## 📊 Scale ước tính (1 mùa EPL)

| Bảng | Rows | Kích thước |
|------|------|-----------|
| `shots` | ~9,120 | ~3.2 MB |
| `player_match_stats` | ~10,640 | ~2.7 MB |
| `heatmaps` | ~8,500 | ~47 MB |
| `match_lineups` | ~15,000 | ~2.5 MB |
| **Tổng 1 mùa EPL** | | **~65 MB** |
