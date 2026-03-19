# DB Schema Refinement & Standardization

## Goal
Triển khai bảng Master cho `leagues` (Giải đấu) và `seasons` (Mùa giải) để chuẩn hóa dữ liệu đa nguồn, loại bỏ "magic strings" và cung cấp nền tảng vững chắc cho Backend API.

## Tasks
- [x] Task 1: Cập nhật DDL Header → Thêm định nghĩa bảng `leagues` và `seasons` vào đầu file `db/schema.sql`.
- [x] Task 2: Cơ chế Seeding Dữ liệu → Bổ sung lệnh `INSERT ... ON CONFLICT` để đổ dữ liệu từ `league_registry.py` vào DB trực tiếp trong `schema.sql` hoặc qua file SQL riêng.
- [x] Task 3: Thiết lập Foreign Keys (FK) → Cập nhật các bảng stats để tham chiếu đến bảng Master `leagues`.
- [x] Task 4: Chuẩn hóa logic Mùa giải → Thêm bảng `seasons` và các FK liên quan.
- [x] Task 5: Tối ưu Search Performance → Kích hoạt extension `pg_trgm` và tạo GIN index cho các cột tìm kiếm tên cầu thủ.

## Done When
- [x] Bảng `leagues` và `seasons` được tạo thành công.
- [x] Dữ liệu các giải đấu được seed thành công vào DB.
- [x] Các FK constraints được thêm vào `schema.sql` cho `league_id`.
- [x] Tính năng tìm kiếm dùng `pg_trgm` được thiết lập trên `squad_rosters` hoặc bảng `mv_player_complete_stats`.

## Notes
Sử dụng tool để thay thế nội dung file `db/schema.sql` và sau đó dùng `psql` (thông qua `run_command`) để apply các thay đổi nếu cần kiểm tra.
