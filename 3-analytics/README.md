# Slice 3 — Analytics & ML (Người 3)

Chưa có nội dung. Chủ thư mục tạo nhánh `<ten>/analytics-start` rồi đặt code vào đây.

Đầu vào: cùng warehouse ở trên. Ba điều phải đọc trước khi train — ở [../2-processing/docs/ban_giao_nhom.md](../2-processing/docs/ban_giao_nhom.md):

1. `fare_amount < 0` (5,9–9,4% dòng 2025) **đã bị loại**, warehouse không còn giá trị âm.
2. `passenger_count` không dùng làm feature.
3. 2009–2010 không có trong warehouse.
