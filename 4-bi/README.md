# Slice 4 — BI & Integration (Người 4)

Chưa có nội dung. Chủ thư mục tạo nhánh `<ten>/bi-start` rồi đặt code vào đây.

Đầu vào: cùng warehouse ở trên. Hai điều ảnh hưởng trực tiếp axis/dashboard — ở [../2-processing/docs/ban_giao_nhom.md](../2-processing/docs/ban_giao_nhom.md):

1. `day_of_week` đánh số **1 = Chủ nhật** (chuẩn Spark), không phải 1 = Thứ hai.
2. `pickup_datetime`/`dropoff_datetime` là giờ địa phương NYC, **không kèm offset** trong file.
