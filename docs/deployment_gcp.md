# Deploying Vertex Football Scraper to GCP (e2-micro)

Hướng dẫn từng bước cấu hình máy chủ Linux ảo (GCP e2-micro Ubuntu/Debian) từ con số 0 để chạy hệ thống `scheduler_master.py` 24/7.

---

## Bước 1: Cài đặt môi trường cơ bản (Python & Git)
Truy cập SSH vào máy chủ e2-micro và chạy các lệnh sau để cập nhật hệ thống và cài đặt Python 3.11+ cùng với Git.

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev git -y
```

---

## Bước 2: Cài đặt PostgreSQL
Hệ thống sử dụng cơ sở dữ liệu PostgreSQL để lưu trữ.

```bash
# 1. Cài đặt PostgreSQL
sudo apt install postgresql postgresql-contrib -y

# 2. Đổi password cho user postgres mặc định
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'Chon1MatKhauSieuKho@123';"

# 3. Tạo database cho dự án
sudo -u postgres createdb vertex_football
```

---

## Bước 3: Phân quyền bảo mật (Tạo user riêng)
Không bao giờ nên chạy Bot bằng user `root`. Hãy tạo riêng một user tên là `vertex` để quản lý source code. (Đây cũng là user được định nghĩa trong file `systemd/scheduler-master.service`).

```bash
# Tạo user vertex
sudo useradd -m -s /bin/bash vertex

# Thêm vertex vào group sudo (tùy chọn)
sudo usermod -aG sudo vertex
```

---

## Bước 4: Clone Source Code & Cài đặt Thư viện
Chuyển quyền điều khiển sang user `vertex` và clone code vào thư mục `/opt/vertex-football-scraper` theo đúng cấu hình của systemd.

```bash
# Cấp quyền thư mục /opt cho user vertex
sudo mkdir -p /opt/vertex-football-scraper
sudo chown -R vertex:vertex /opt/vertex-football-scraper

# Đổi sang user vertex
sudo su - vertex

# Di chuyển vào thư mục clone code
cd /opt/vertex-football-scraper

# Trình bày Git Clone (thay bằng URL repo CỦA BẠN nếu để private, cần xài Personal Access Token)
git clone https://github.com/NguyenTruong354/Vertex-Football-Scraper.git .

# Tạo môi trường ảo (Virtual Environment)
python3.11 -m venv .venv
source .venv/bin/activate

# Cài đặt thư viện (Quan trọng nhất là curl_cffi)
pip install -r requirements.txt
```

---

## Bước 5: Cấu hình Biến môi trường (.env)
Tạo file `.env` từ file mẫu và điền thông tin Database cũng như Webhook Discord.

```bash
cp .env.example .env
nano .env
```
Nội dung file `.env` tham khảo:
```env
# Database (Dùng pass bạn đã set ở Bước 2)
DB_HOST=127.0.0.1
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=Chon1MatKhauSieuKho@123
DB_NAME=vertex_football

# Discord (Tạo webhook trong Server > Integrations)
DISCORD_WEBHOOK_LIVE=https://discord.com/api/webhooks/123/abc
DISCORD_WEBHOOK_ERROR=https://discord.com/api/webhooks/123/def
DISCORD_WEBHOOK_INFO=https://discord.com/api/webhooks/123/ghi
```

---

## Bước 6: Khởi tạo Cấu trúc Bảng (Schema)
Chạy script tạo bảng cơ sở dữ liệu ban đầu.

```bash
# (Đảm bảo đang bật .venv)
python db/setup_db.py
```

---

## Bước 7: Kích hoạt Systemd Daemon 24/7
Systemd là trình quản lý tiến trình của Linux, giúp bot tự động bật khi máy chủ khởi động lại và tự động restart nếu bị crash.

```bash
# Thoát khỏi user vertex để về lại user có quyền sudo
exit

# Copy file service systemd vào thư mục hệ thống
sudo cp /opt/vertex-football-scraper/systemd/scheduler-master.service /etc/systemd/system/

# Nhắc systemd đọc lại cấu hình mới
sudo systemctl daemon-reload

# Bật chạy cùng lúc boot máy
sudo systemctl enable scheduler-master.service

# KHỞI CHẠY BOT
sudo systemctl start scheduler-master.service
```

---

## Bước 8: Kiểm tra Log Trực tiếp
Để theo dõi xem Bot có đang đọc lịch, bypass Cloudflare SofaScore thành công hay không:

```bash
# Xem log hệ thống của riêng daemon này (nhấn Shift+F để cuộn theo real-time)
sudo journalctl -u scheduler-master.service -f

# Hoặc xem file log do python tự sinh ra
tail -f /opt/vertex-football-scraper/logs/scheduler_master.log
```

🎉 **XONG!** Bây giờ máy ảo e2-micro của bạn (chỉ tốn cỡ 60MB RAM) sẽ tự động hoạt động ngày ngày đêm đêm canh me bóng đá!
