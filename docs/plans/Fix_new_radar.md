Fix Plan — 

services/news_radar.py
Mục tiêu: Khắc phục các lỗi về độ tin cậy (reliability), rò rỉ tài nguyên (leak) và tính toàn vẹn dữ liệu (data integrity) trong module cào tin tức RSS. Phân loại: 🔴 Critical · 🟠 High · 🟡 Medium · 🔵 Low

Tóm tắt
| # | Mức độ | Vấn đề | File / Vị trí | Giải pháp | Trạng thái |
|---|--------|---------|---------------|-----------|------------|
| 1 | 🔴 Critical | DB Connection Leak | `run_and_save()` | Áp dụng `try...finally` cho connection và cursor. | ✅ **DONE** |
| 2 | 🔴 Critical | Missing Timeout | `fetch_news()` | Thiết lập `socket.timeout` (15s) tránh treo luồng. | ✅ **DONE** |
| 3 | 🟠 High | Hardcoded League ID | `run_and_save()` | Tagger ưu tiên: UCL > UEL > Leagues > NULL. | ✅ **DONE** |
| 4 | 🟡 Medium | Silent Feed Failures | `fetch_news()` | Kiểm tra `feed.bozo`, log lỗi chi tiết, báo Discord. | ✅ **DONE** |
Chi tiết từng Issue
1. 🔴 DB Connection Leak (Critical)
Nguyên nhân: Lệnh conn.close() nằm trong block 

try
. Khi quá trình INSERT gặp lỗi, code nhảy vào except và kết thúc hàm mà không đóng kết nối. Hệ quả: Mỗi lần lỗi sẽ treo 1 connection. Sau vài ngày chạy định kỳ (30p/lần), PostgreSQL sẽ hết slot kết nối, gây crash toàn bộ bot. Hướng sửa:

python
conn = None
cur = None
try:
    conn = get_connection()
    cur = conn.cursor()
    # ... logic ...
finally:
    if cur: cur.close()
    if conn: conn.close()
2. 🔴 Missing Timeout (Critical)
Nguyên nhân: feedparser.parse(url) không có timeout mặc định. Nếu server BBC/Sky bị treo kết nối ở tầng socket, luồng thực thi (sync) sẽ bị block vô thời hạn. Hệ quả: Vì News Radar chạy đồng bộ trong nhịp của 

MasterScheduler
, việc treo luồng cào tin sẽ kéo theo toàn bộ hệ thống polling trận đấu trực tiếp bị dừng lại (Blocking event loop là hệ quả của việc thiếu timeout). Hướng sửa: Sử dụng socket.setdefaulttimeout(15) ngay trong 

fetch_news()
 trước khi gọi parse và reset lại sau khi xong để đảm bảo an toàn cho daemon.

3. 🟠 Hardcoded League ID (High)
Nguyên nhân: Mọi tin tức từ mọi nguồn đều bị gán league_id = "EPL". Hệ quả: Tin tức về Champions League, La Liga... hiển thị sai chuyên mục trên frontend. Hướng sửa: Xây dựng hàm detect_league(title, summary) với cơ chế phân cấp ưu tiên (Priority Order):

UCL/UEL Rules: "Champions League", "Europa League", "UCL", "UEL" -> Gán ngay (UCL > UEL).
League Rules: "Premier League", "Madrid", "Bundesliga"... -> Gán giải tương ứng.
Fallback: Nếu không khớp bất kỳ từ khóa nào -> để NULL (None). Logic này giải quyết edge case tin tức match nhiều league (VD: Real Madrid vs Man City tại UCL).
4. 🟡 Silent Feed Failures (Medium)
Nguyên nhân: feedparser không raise Exception khi parse lỗi mà chỉ trả về entries rỗng và set bozo = True. Hệ quả: Bot chạy qua "im lặng", người vận hành không biết nguồn tin đang bị hỏng hay chỉ đơn giản là không có tin mới. Hướng sửa:

python
feed = feedparser.parse(url)
if feed.bozo:
    log.warning(f"Feed error {source}: {feed.bozo_exception}")
if not feed.entries:
    continue
Lộ trình triển khai (Verification steps)
Giai đoạn 1 (Stability): Fix triệt để Connection Leak và Timeout để đảm bảo an toàn cho 

scheduler_master.py
.
Giai đoạn 2 (Logic): Triển khai Keyword Tagger có phân cấp ưu tiên cho UCL/UEL.
Giai đoạn 3 (Alerting): Gửi Discord alert nếu phát hiện feed.bozo = True liên tiếp 3 lần trở lên cho cùng một nguồn tin (tránh spam khi mạng chập chờn nhất thời).
Tạo ngày: 2026-03-11 · Dựa trên phân tích mã nguồn 

services/news_radar.py