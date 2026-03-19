# 🚀 Lộ trình Mở rộng Vertex Football Scraper (Future Roadmap)

Tài liệu này ghi lại các ý tưởng cải tiến hệ thống theo 4 trục chính: AI, Phân phối, Dữ liệu chuyên sâu và Hạ tầng. Chúng ta sẽ bắt đầu thực hiện các phần này sau giai đoạn test độ ổn định của hệ thống lõi.

---

## 1. 🧠 Trục Trí tuệ Nhân tạo (AI Expansion)
*   **Predictive Analysis (Dự đoán trực tiếp)**: Tích hợp model dự đoán khả năng có bàn thắng dựa trên dữ liệu 15 phút gần nhất (Momentum Score).
*   **AI Commentary Generation**: Tự động tạo nội dung bình luận trận đấu theo thời gian thực (real-time commentary) theo phong cách chuyên nghiệp.
*   **Player Scouting Alerts**: AI tự động phát hiện các cầu thủ có chỉ số (rating) tăng đột biến qua nhiều trận để đưa ra gợi ý chuyển nhượng hoặc đầu tư.

## 2. 📡 Trục Kết nối & Phân phối (Distribution)
*   **Real-time Push Notifications**: Tích hợp Firebase/OneSignal để gửi thông báo bàn thắng, thẻ đỏ tức thì lên Web/App người dùng.
*   **Interactive Chatbot (Telegram/Discord)**: Xây dựng bot cho phép người dùng truy vấn tỉ số hoặc "đăng ký" theo dõi (follow) các trận đấu cụ thể qua lệnh chat.
*   **Streaming Webhook API**: Cung cấp cổng Webhook để các hệ thống bên ngoài có thể nhận dữ liệu trận đấu ngay khi vừa được scrape xong.

## 3. 📊 Trục Dữ liệu Chuyên sâu (Advanced Data)
*   **Live xG Mapping**: Visualize biểu đồ Expected Goals (xG) theo dòng thời gian của trận đấu.
*   **Historical Context Engine**: Tự động so sánh dữ liệu live với lịch sử (ví dụ: "Đây là lần thứ n đội A ghi bàn ở phút bù giờ mùa này").
*   **Heatmap Integration**: Nếu dữ liệu nguồn cung cấp tọa độ, hệ thống sẽ parse và lưu trữ Heatmap của các cầu thủ chủ chốt.

## 4. ⚙️ Trục Kỹ thuật & Hạ tầng (Infrastructure)
*   **Distributed Cluster Scrapers**: Chia nhỏ các giải đấu ra các Worker độc lập (Docker) để tăng khả năng chịu lỗi và tránh bị Ban IP diện rộng.
*   **Raw Data Lake**: Lưu trữ dữ liệu JSON thô vào Cloud Storage (S3/GCS) để phục vụ việc huấn luyện model AI tùy chỉnh trong tương lai.
*   **Admin Monitoring Dashboard**: Xây dựng giao diện web (Next.js/React) để theo dõi trạng thái Proxy, Anti-ban state và hiệu năng của Database Pool.

---

*Ghi chú: Lộ trình này sẽ được cập nhật dựa trên nhu cầu thực tế và phản hồi từ người dùng sau giai đoạn Beta.*
