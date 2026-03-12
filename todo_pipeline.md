📅 Kế Hoạch Bơm Dữ Liệu Lịch Sử (Data Backfill Plan)
Dưới đây là lịch trình cào dữ liệu được thiết kế cực kỳ an toàn cho máy Local của bạn:

Chia nhỏ: Mỗi lần chạy chỉ cào 1 giải trong 1 mùa.
Tiết kiệm: Chỉ bỏ qua cào Heatmap của SofaScore để tiết kiệm API, nhưng vẫn cào Điểm số, Đội hình và Chỉ số chuyền bóng.
Cập nhật tự động: Sau khi chạy từng lệnh, Tools sẽ tự map tên cầu thủ lại giúp AI hiểu được ngay!
🎯 Ưu Tiên 1: Mùa giải "Sát" Hiện Tại (2024/2025)
Mùa giải này là chìa khóa để AI đánh giá được phong độ 12 tháng qua.

 Lệnh: python run_pipeline.py --league EPL --season 2024 --skip-sofascore-heatmaps. Done
 Lệnh: python run_pipeline.py --league UCL --season 2024 --skip-sofascore-heatmaps. Done
 Lệnh: python run_pipeline.py --league LALIGA --season 2024 --skip-sofascore-heatmaps. Done
 Lệnh: python run_pipeline.py --league BUNDESLIGA --season 2024 --skip-sofascore-heatmaps. Done
 Lệnh: python run_pipeline.py --league SERIEA --season 2024 --skip-sofascore-heatmaps. Done
 Lệnh: python run_pipeline.py --league LIGUE1 --season 2024 --skip-sofascore-heatmaps. Done
🎯 Ưu Tiên 2: Mùa giải "Xưa" (2023/2024)
Dữ liệu này giúp AI viết được những bài "So sánh với năm ngoái" hoặc "Lịch sử đối đầu".

 Lệnh: python run_pipeline.py --league EPL --season 2023 --skip-sofascore-heatmaps
 Lệnh: python run_pipeline.py --league UCL --season 2023 --skip-sofascore-heatmaps
 Lệnh: python run_pipeline.py --league LALIGA --season 2023 --skip-sofascore-heatmaps
 Lệnh: python run_pipeline.py --league BUNDESLIGA --season 2023 --skip-sofascore-heatmaps
 Lệnh: python run_pipeline.py --league SERIEA --season 2023 --skip-sofascore-heatmaps
 Lệnh: python run_pipeline.py --league LIGUE1 --season 2023 --skip-sofascore-heatmaps
🎯 Chốt sổ: Mùa giải "Đương Đại" (2025/2026)
Sau khi nạp lịch sử xong, ta chạy 1 cú FULL bao gồm cả SofaScore cho mùa 2025 để lấy Heatmap và Update bảng xếp hạng Live nhất. Tôi set lệnh dưới đây chỉ quét Heatmap 50 trận mới nhất của mỗi giải, rất xịn và nhanh!

 Lệnh: python run_pipeline.py --league EPL LALIGA BUNDESLIGA SERIEA LIGUE1 UCL --ss-match-limit 50
Cách dùng: Bạn cứ copy lệnh dán vào Terminal của bạn. Chạy xong cái nào (báo "Tất cả thành công!") thì bạn dùng bút nhớ hoặc copy list này ra file text để đánh dấu (x) vào là được.

