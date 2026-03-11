# VERTEX FOOTBALL — PROJECT COMPREHENSIVE DASHBOARD
> **Mục tiêu:** Bản đồ toàn diện về hệ thống Vertex Football Scraper. 
> **Tài liệu tham khảo:** Dùng file này để nắm bắt nhanh "cái gì làm gì" trong toàn bộ codebase.

---

## 🚀 1. CORE RUNNERS & DAEMONS (TRÌNH ĐIỀU KHIỂN CHÍNH)

| File | Chức năng chính |
|---|---|
| `scheduler_master.py` | **Đầu não điều khiển 24/7.** Quản lý 1 browser duy nhất, polling tất cả các giải đấu đang trực tiếp, điều phối Post-match worker và Daily maintenance. Hỗ trợ cơ chế **Daily Data Compensation** (bù dữ liệu thiếu nếu daemon gián đoạn). |
| `run_pipeline.py` | CLI tool để chạy pipeline cào dữ liệu cho một giải đấu cụ thể (bao gồm cào, load DB, tạo MV). |
| `run_daemon.py` | Phiên bản cũ của trình điều hành (dần được thay thế bởi `scheduler_master.py`). |
| `run_simulate_epl.py` | Script giả lập một ngày thi đấu Ngoại hạng Anh để test logic polling/post-match. |
| `live_match.py` | Định nghĩa các lớp trạng thái và logic cốt lõi cho việc theo dõi trận đấu trực tiếp. |

---

## 🕷️ 2. SCRAPER MODULES (CÁC BỘ CÀO DỮ LIỆU)

### ⚽ SofaScore (`/sofascore`)
- **`sofascore_client.py`**: Cào dữ liệu chi tiết trận đấu, đội hình, chỉ số cầu thủ sau trận.
- **`config_sofascore.py`**: Quản lý Season IDs, Tournament IDs và logic tra cứu mùa giải động.
- **`heatmaps_scraper.py`**: Chuyên biệt cào dữ liệu heatmap của từng cầu thủ.

### 📊 FBRef (`/fbref`)
- **`fbref_scraper.py`**: Cào dữ liệu chuyên sâu (Match Reports, Shooting, Passing, GCA) bằng cách parse HTML.
- **`config_fbref.py` / `schemas_fbref.py`**: Cấu hình URL và định nghĩa schema dữ liệu.

### 📈 Understat (`/understat`)
- **`async_scraper.py`**: Cào dữ liệu xG thời gian thực và lịch sử của Understat bằng cơ chế async.
- **`extractor.py`**: Parse dữ liệu JSON từ các biến JavaScript nhúng trong web Understat.

### 💰 Transfermarkt (`/transfermarkt`)
- **`tm_scraper.py`**: Cào dữ liệu chuyển nhượng, giá trị cầu thủ và hồ sơ chấn thương.
- **`config_tm.py`**: Quản lý ID của CLB và giải đấu trên Transfermarkt.

---

## 🤖 3. AI INSIGHT & SERVICES (`/services`)

| Component | Chức năng |
|---|---|
| `llm_client.py` | Interface chung kết nối Groq/LLM, có cơ chế Circuit Breaker chống treo và chuyển đổi API Key linh hoạt. |
| `insight_producer.py` | "Bộ lọc" quyết định khi nào nên tạo Job AI dựa trên diễn biến trận đấu (Goal, Red Card, Cooldown). |
| `insight_worker.py` | Tiến trình chạy ngầm thực hiện gọi LLM và lưu kết quả vào DB. |
| `match_story.py` | Tạo bài viết tóm tắt trận đấu (Narrative story). |
| `player_trend.py` | Phân tích phong độ 5 trận gần nhất để đưa ra nhận xét cầu thủ "Thăng hoa" hay "Sa sút". |
| `live_insight.py` | Tạo các câu Badge ngắn (mô tả nhanh) cho các trận đang trực tiếp. |
| `news_radar.py` | RSS Crawler cào tin tức từ BBC/Sky Sports và tự động tag nhãn giải đấu. |
| `league_registry.py` | "Danh bạ" tập trung quản lý ID của tất cả các giải đấu trên mọi nguồn dữ liệu. |

---

## 💾 4. DATABASE LAYER (`/db`)

- **`schema.sql`**: Định nghĩa toàn bộ cấu trúc DB (Bảng, Materialized Views, Triggers).
- **`loader.py`**: Module chịu trách nhiệm insert/update dữ liệu từ các Scraper vào DB.
- **`config_db.py`**: Quản lý Connection Pool kết nối PostgreSQL.
- **`queries.py`**: Tập hợp các câu truy vấn phức tạp (SQL) dùng chung cho hệ thống.

---

## 🛠️ 5. TOOLS & MAINTENANCE (`/tools`)

- **`/maintenance/build_match_crossref.py`**: Quan trọng nhất — Mapping ID trận đấu giữa 4 nguồn khác nhau.
- **`/maintenance/refresh_mv.py`**: Script điều khiển việc làm mới các Materialized Views.
- **`verify_missing_stats.py`**: Kiểm tra các trận đấu bị thiếu dữ liệu để cào bù.

---

## ✅ TRẠNG THÁI & CÁC CẢI TIẾN GẦN ĐÂY

### Đã hoàn thành (Recently Done):
- **Optimizer Daily Maintenance (2026-03-11)**: Giảm thời gian chạy từ vài giờ xuống 10-20 phút.
  - **FBref**: Áp dụng `--since-date` với window 3 ngày (lookback safety margin).
  - **Transfermarkt**: Dùng `--metadata-only` cho daily (bỏ qua kader/market value pages nặng).
  - **Data Compensation**: Tự động quét bù 10 trận gần nhất cho Understat và SofaScore mỗi đêm để đảm bảo không miss dữ liệu nếu daemon bị tắt/crash during match.
- **Fix Scheduler Bottleneck**: 
  - Thu hẹp Lock scope trong `get_json` + Jitter/Backoff logic tinh vi.
  - Thêm ThreadPool cho PostMatch để tránh block Event Loop.
  - Sửa lỗi Tier C trigger (Lineup refresh sau bàn thắng) bằng cách so sánh snapshot incidents.
- **DB Connection Integrity**: Fix lỗi rò rỉ (leak) connections trong `_check_drift` bằng mô hình `try-finally`.
- **Standings Error Boundary**: Wrap cập nhật BXH bằng safe handler, log lỗi và gửi Discord alert kèm lệnh manual fix thay vì im lặng.

### Vấn đề đang xử lý (Ongoing Issues):
- **AI Pipeline Improvement**: Đang tiến hành refactor các dịch vụ AI sang mô hình Prompts Registry và Context Awareness.
- **News Radar League Tagging**: Cải thiện việc gán nhãn League cho tin tức (tránh tag sai khi cầu thủ chuyển nhượng CLB khác giải).

---

## 📂 6. TỔ CHỨC THƯ MỤC CÒN LẠI
- **`/docs/plans`**: Chứa tất cả các bản kế hoạch chi tiết (AI, Live, DB Improvements).
- **`/systemd`**: Chứa các file cấu hình dịch vụ để treo Project trên Linux VM.
- **`/output`**: Nơi lưu tạm các file CSV/JSON trong quá trình cào dữ liệu thô.
- **`/logs`**: Nhật ký vận hành hệ thống.

---
*Cập nhật lần cuối: 2026-03-11*
