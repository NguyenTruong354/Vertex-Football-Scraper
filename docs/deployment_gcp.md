# Deploying Vertex Football Scraper to GCP (e2-micro)

Hướng dẫn từng bước cấu hình máy chủ Linux ảo (GCP e2-micro Ubuntu/Debian) từ con số 0 để chạy hệ thống `scheduler_master.py` 24/7.

---

## Bước 1: Cài đặt Python, Git và Google Chrome
Truy cập SSH vào máy chủ e2-micro và chạy các lệnh sau để cập nhật hệ thống, cài đặt Python 3.11+ cùng với **Google Chrome** (bắt buộc để chạy Browser Scraper).

```bash
# 1. Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y
sudo apt install software-properties-common -y

# 2. Cài đặt Python 3.11
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev git -y

# 3. Cài đặt Google Chrome (Cần thiết cho nodriver/scraper)
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install ./google-chrome-stable_current_amd64.deb -y
# Xóa file deb sau khi cài xong cho đỡ tốn chỗ
rm google-chrome-stable_current_amd64.deb
```

---

## Bước 2: Phân quyền bảo mật (Tạo user riêng)
Không bao giờ nên chạy Bot bằng user `root`. Hãy tạo riêng một user tên là `vertex` để quản lý source code. (Đây cũng là user được định nghĩa trong file `systemd/scheduler-master.service`).

```bash
# Tạo user vertex
sudo useradd -m -s /bin/bash vertex

#Đặt lại pw
sudo passwd vertex

# Thêm vertex vào group sudo (tùy chọn)
sudo usermod -aG sudo vertex
```

---

## Bước 3: Clone Source Code & Cài đặt Thư viện
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

## Bước 4: Tải Chứng chỉ Aiven (ca.pem) & Cấu hình Biến môi trường (.env)
Dự án sử dụng cơ sở dữ liệu trên mây của Aiven. Bạn cần tải chứng chỉ bảo mật và khai báo chuỗi kết nối.

```bash
# 1. Tải file ca.pem từ Aiven (Nên tải trực tiếp trên máy chủ ảo)
mkdir -p /opt/vertex-football-scraper/certs
# Sử dụng nano copy/paste nội dung ca.pem của bạn vào đây:
nano /opt/vertex-football-scraper/certs/ca.pem

# 2. Tạo file .env
cp .env.example .env
nano .env
```
Nội dung file `.env` tham khảo:
```env
# Aiven PostgreSQL — lấy từ console.aiven.io (Connection → Service URI)
DATABASE_URL=postgres://avnadmin:MAT_KHAU_CUA_BAN@pg-xxx.aivencloud.com:PORT/defaultdb?sslmode=require
PGSSLROOTCERT=/opt/vertex-football-scraper/certs/ca.pem

# Discord (Tạo webhook trong Server > Integrations)
DISCORD_WEBHOOK_LIVE=https://discord.com/api/webhooks/123/abc
DISCORD_WEBHOOK_ERROR=https://discord.com/api/webhooks/123/def
DISCORD_WEBHOOK_INFO=https://discord.com/api/webhooks/123/ghi
```

---

## Bước 5: Khởi tạo Cấu trúc Bảng (Schema)
Tuy database nằm trên Aiven, nhưng bạn cần chạy script lần đầu từ server e2-micro để nó thiết lập các bảng nếu chưa có.

```bash
# (Đảm bảo đang bật .venv)
python db/setup_db.py
```

---

## Bước 6: Kích hoạt Systemd Daemon 24/7
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

## Bước 7: Kiểm tra Log Trực tiếp
Để theo dõi xem Bot có đang đọc lịch, bypass Cloudflare SofaScore thành công hay không:

```bash
# Xem log hệ thống của riêng daemon này (nhấn Shift+F để cuộn theo real-time)
sudo journalctl -u scheduler-master.service -f

# Hoặc xem file log do python tự sinh ra
tail -f /opt/vertex-football-scraper/logs/scheduler_master.log
```

🎉 **XONG!** Bây giờ máy ảo e2-micro của bạn (với 1GB-1.5GB RAM) sẽ hoạt động ổn định. Hệ thống tiêu thụ khoảng **700MB - 1GB RAM** nhờ cấu hình Ultra-Lite Chrome (tắt ảnh) và phân luồng LLM 8B thông minh mà chúng ta đã thiết lập!
