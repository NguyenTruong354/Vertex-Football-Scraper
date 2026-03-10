# VERTEX FOOTBALL — PROJECT DASHBOARD
> **Mục tiêu:** Cung cấp context nhanh cho AI về kiến trúc, trạng thái và lộ trình phát triển của dự án.
> **Lưu ý:** Đọc file này trước khi bắt đầu bất kỳ hội thoại mới nào để tiết kiệm token và đảm bảo tính nhất quán.

---

## 🏗 1. KIẾN TRÚC HỆ THỐNG (SYSTEM ARCHITECTURE)

Hệ thống hoạt động theo mô hình **Single-Browser Multi-League Daemon**.

### 核心 (Core Components):
- **`scheduler_master.py`**: "Đầu não" điều khiển chính. Quản lý 1 instance `curl_cffi` duy nhất cho tất cả các giải đấu.
  - **LiveTrackingPool**: Quản lý các trận đang diễn ra bằng cơ chế round-robin polling.
  - **PostMatchWorker**: Chạy hậu xử lý (scrapers FBRef/Understat/TM) trong ThreadPool sau khi trận kết thúc.
  - **DailyMaintenance**: Chạy bảo trì hàng đêm (cập nhật BXH, chuyển nhượng, dọn dẹp DB).
- **`CurlCffiClient`**: Client HTTP chính, sử dụng TLS Impersonation (Chrome) để vượt qua Cloudflare.
  - **Anti-Ban logic**: Sử dụng `asyncio.Lock` thu hẹp scope, Jitter trước khi request và Backoff khi bị rate-limit.

### 🤖 AI Insight Pipeline (Producer-Worker Pattern):
- **`insight_producer.py`**: Lọc dữ liệu, kiểm tra cooldown và "đặt hàng" Job AI vào bảng `ai_insight_jobs`.
- **`insight_worker.py`**: Chạy ngầm trong `scheduler_master`, lấy Job, gọi LLM (Groq) và lưu kết quả.
- **Dịch vụ AI**: `match_story.py` (tóm tắt trận), `player_trend.py` (phân tích cầu thủ), `live_insight.py` (badge tức thời).

---

## 📊 2. CẤU TRÚC DỮ LIỆU (DATABASE SNAPSHOT)

- **Database**: PostgreSQL.
- **Bảng quan trọng**:
  - `matches` / `player_match_stats`: Dữ liệu lịch sử.
  - `live_snapshots` / `live_match_state`: Dữ liệu realtime cho Live App.
  - `ai_insight_jobs`: Quản lý lifecycle của các nhận định AI.
  - `ai_insight_feedback`: Lưu đánh giá (upvote/downvote) để tối ưu Prompt.
  - `team_canonical`: Map tên CLB giữa các nguồn (SofaScore, FBRef...).

---

## 🛠 3. CÔNG NGHỆ CHÍNH (TECH STACK)

- **Language**: Python 3.10+ (Asyncio-heavy).
- **Scraping**: `curl_cffi` (impersonate Chrome), `nodriver` (chỉ dùng khi cần render JS nặng).
- **LLM**: Groq API (Llama-3 models).
- **Notification**: Discord Webhooks.
- **Database**: `psycopg2` / `SQLAlchemy` (dần chuyển sang async).

---

## 📝 4. TRẠNG THÁI HIỆN TẠI & NHỮNG GÌ ĐÃ FIX

### ✅ Đã hoàn thành (Recently Done):
- **SofaScore Dynamic Season**: Tự động tra cứu `season_id` thay vì hardcode.
- **Fix Scheduler Bottleneck**: 
  - Thu hẹp Lock scope trong `get_json`.
  - Thêm ThreadPool cho PostMatch để tránh block Event Loop.
  - Sửa lỗi Tier C trigger (Lineup refresh sau bàn thắng).
- **AI Quick Test**: Thêm block `if __name__ == "__main__"` cho các dịch vụ AI để test nhanh với mock data.

### 🔴 Vấn đề đang xử lý (Ongoing Issues):
- **Issue #5 (Plan Fix Scheduler)**: Rò rỉ kết nối DB (Connection leak) trong `_check_drift`.
- **AI Pipeline Improvement**: Đang tiến hành refactor các dịch vụ AI sang mô hình Prompts Registry và Context Awareness.
- **News Radar**: Cần cải thiện việc gán nhãn League cho tin tức (tránh gán sai khi cầu thủ chuyển nhượng).

---

## 🚀 5. LỘ TRÌNH (ROADMAP)

1. **Phase A (Fix Production)**: Hoàn thành nốt bản kế hoạch `Fix_plan_scheduler_master.md`.
2. **Phase B (AI Refactor)**: Triển khai `prompt_registry.py` và đưa `player_trend` hoàn toàn vào queue.
3. **Phase C (Frontend Sync)**: Đồng bộ dữ liệu AI Insight lên giao diện người dùng.

---
*Cập nhật lần cuối: 2026-03-10*
