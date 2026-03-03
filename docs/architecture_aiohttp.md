# Architecture: Reverse-Engineering API (HTTP/2 + aiohttp)

## 1. Tổng quan vấn đề
**Mục tiêu:** Trao đổi dữ liệu trực tiếp với SofaScore API qua HTTP requests thông thường (GET/POST) thay vì dùng trình duyệt thực (Chrome/nodriver). 
**Lợi ích:** Tiết kiệm >90% RAM và CPU. Có thể theo dõi hàng trăm trận đấu cùng lúc trên cấu hình e2-micro.
**Khó khăn cốt lõi:** SofaScore sử dụng Cloudflare Bot Management để chặn mọi request tự động (non-browser). Mọi HTTP client thông thường (`requests`, `aiohttp`, `httpx`) sẽ bị chặn với mã lỗi `HTTP 403 Forbidden` trừ khi mô phỏng được hành vi của con người và trình duyệt.

## 2. Bản chất của lớp bảo vệ Cloudflare
Cloudflare kiểm tra các yếu tố sau để xác định bạn là người hay bot:
1. **Cookies:** `cf_clearance` (được cấp sau khi giải một bài toán JS/CAPTCHA ẩn).
2. **TLS Fingerprinting (JA3/JA4):** Đoán loại ứng dụng đang dùng dựa trên cách bắt tay SSL/TLS (VD: Python `aiohttp` bắt tay TLS rất khác với Google Chrome phiên bản 120).
3. **HTTP/2 Fingerprinting (AKAMAI):** Thứ tự các Frame window, SETTINGS, ưu tiên frame... trong giao thức HTTP/2.
4. **Header Order:** Trình duyệt thực luôn gửi User-Agent, Accept, Accept-Encoding theo một thứ tự cố định. Bot thường gửi lộn xộn.
5. **Browser Integrity (JS Challenge):** Trang yêu cầu chạy đoạn code JS phức tạp để chứng minh đây là trình duyệt thật.

## 3. Các bước triển khai (Phase 1 → 3)

### Phase 1: TLS Impersonation (Giả mạo Trình duyệt)
Bạn không thể dùng `aiohttp` hay `requests` thông thường vì TLS Fingerprint của chúng nằm trong danh sách đen của Cloudflare.

**Giải pháp:** Sử dụng thư viện Python có khả năng giả mạo TLS Fingerprint của Chrome/Edge/Firefox.
*   `curl_cffi` (Cực kỳ mạnh mẽ, port từ thư viện `curl-impersonate` của C++).
*   `nodriver` (Nếu bắt buộc phải lấy Cookie lần đầu tiên).

**Code Sandbox nghiệm thu:**
```python
from curl_cffi import requests

# curl_cffi sẽ tự động giả mạo SSL TLS bắt tay giống hệt Chrome 120
response = requests.get(
    "https://api.sofascore.com/api/v1/sport/football/scheduled-events/2026-03-03",
    impersonate="chrome120"
)
print(response.status_code) # Mong muốn: 200 (Nếu Cloudflare chỉ chặn theo TLS)
```

### Phase 2: Cookie Injection (Bypass JS Challenge)
Nếu `curl_cffi` vẫn trả về `403`, nghĩa là Cloudflare SofaScore đang bật mức bảo vệ cao (Bot Fight Mode) bắt buộc phải có Cookie `cf_clearance` hợp lệ.

**Quy trình kết hợp (Hybrid):**
1. **Buổi sáng (Khởi tạo):** Mở `nodriver` (Chrome ẩn) chui vào trang chủ `https://www.sofascore.com/`. Đợi 5 giây cho Cloudflare verify và cấp Cookie `cf_clearance`.
2. **Trích xuất:** Lấy Cookie `cf_clearance` và `User-Agent` chính xác từ `nodriver`.
3. **Tiêm vào HTTP Client:** Tắt Chrome. Pass Cookie và User-Agent đó vào `curl_cffi` hoặc `aiohttp` để xài cho phiên làm việc tiếp theo.

**Lưu ý sinh tử:**
*   User-Agent của Chrome lúc lấy Cookie **phải khớp 100%** với User-Agent bạn bơm vào thư viện giả mạo HTTP. Cùng một TLS Fingerprint, nhưng khác User-Agent là Cloudflare ban ngay lập tức.
*   `cf_clearance` thường có expiry từ 30 phút đến 1 tiếng. Bạn sẽ cần cơ chế "Refresh Cookie" (gọi lại `nodriver` chớp hoáng) nếu API đột nhiên trả về 403 giữa chừng.

### Phase 3: Live Tracking Logic (API Endpoints)
Khi đã bypass thành công Cloudflare và gọi được HTTP 200, bạn bắt đầu parse các endpoint của SofaScore. Đối với Live Tracking, hãy bắt Network request (F12) trên SofaScore khi một trận đấu đang đá, bạn sẽ thấy chúng gọi khoảng 3 tới 5 endpoints chính theo chu kỳ 15 đến 60 giây.

**Các Endpoints quan trọng:**
*   Trạng thái tỉ số & phút: `GET /event/{id}`
*   Sự kiện (Bàn thắng, thẻ phạt, thay người): `GET /event/{id}/incidents`
*   Thống kê trận đấu (Possession, Shots, vv): `GET /event/{id}/statistics`
*   Đội hình (Ra sân, sơ đồ): `GET /event/{id}/lineups`

**Luồng Worker (Pseudo-code):**
```python
async def poll_match(event_id, session: requests.AsyncSession):
    while match_is_live:
        try:
            # Chỉ tốn vài chục KB RAM cho request này
            resp = await session.get(f"https://api.sofascore.com/api/v1/event/{event_id}/incidents")
            if resp.status_code == 403:
                # Cookie hết hạn, gọi hàm refresh bằng nodriver
                await refresh_clearance_cookie(session)
                continue
                
            data = resp.json()
            process_data(data)
            
            # Ngủ 60s
            await asyncio.sleep(60)
            
        except Exception as e:
            await asyncio.sleep(10) # Lỗi mạng cục bộ The loop continues
```

## 4. Tóm tắt ưu nhược điểm

| Tiêu chí | Sử dụng kiến trúc hiện hành (Nodriver) | Áp dụng Reverse-Engineering API (Giải pháp 1) |
| :--- | :--- | :--- |
| **RAM Cost / Match** | Cao (~250MB) | Vô cùng thấp (< 10MB) |
| **CPU Cost** | Trung bình - Cao (JS Rendering) | Siêu thấp |
| **Độ ổn định** | Cao (Vì là DOM thật) | Thấp / Trung bình (Cookie chết, Cloudflare quét gắt) |
| **Khả năng triển khai** | Dễ | Rất Khó, yêu cầu kiên nhẫn bypass WAF. |

> **Khuyên dùng:** Bạn có thể thử nghiệm nghiệm thu giải pháp này bằng thư viện `curl_cffi`. Viết một script độc lập nhỏ (vd `benchmark_bypass.py`) chỉ chứa 1 file duy nhất chọc vào `api.sofascore.com` để xem 1 IP server bình thường có thể vượt qua được vòng bảo mật đầu tiên (TLS Fingerprint) hay không trước khi tốn thời gian xây dựng toàn hệ thống lớn.
