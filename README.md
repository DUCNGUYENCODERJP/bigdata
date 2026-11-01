# Big Data — NYC TLC Yellow Taxi

Bài tập lớn 4 người: dữ liệu Yellow Taxi của NYC TLC đi từ raw → làm sạch bằng PySpark → warehouse phân vùng → Spark SQL/ML → dashboard.

## Phân công và trạng thái

| Slice | Người | Nội dung | Trạng thái |
|---|---|---|---|
| [`1-warehouse/`](1-warehouse/) | Người 1 | Kiến trúc dữ liệu, Hive/HDFS, truy vấn | chờ |
| [`2-processing/`](2-processing/) | Người 2 | Pipeline tải + làm sạch + ghi warehouse | **xong, đã nghiệm thu** |
| [`3-analytics/`](3-analytics/) | Người 3 | Spark SQL, EDA, GBTRegressor | chờ |
| [`4-bi/`](4-bi/) | Người 4 | Power BI, tích hợp, báo cáo cuối kỳ | chờ |

## Điểm giao giữa các slice

- **Dữ liệu sạch**: Parquet Snappy phân vùng `year=/month=`, **25 cột** (23 vật lý + `year`/`month`), 188 tháng `2011-01 → 2026-08`, 1.506.791.871 dòng. Schema chuẩn: [2-processing/docs/schema.md](2-processing/docs/schema.md).
- **Bàn giao của Người 2 cho cả nhóm**: [2-processing/docs/ban_giao_nhom.md](2-processing/docs/ban_giao_nhom.md) (bản ngắn) và [2-processing/docs/handover.md](2-processing/docs/handover.md) (bản kỹ thuật).
- **Dữ liệu không nằm trong repo** (33 GB). Cách lấy: mục 6 của `docs/handover.md`.

## Làm việc chung

Quy tắc để không giẫm chân nhau (thư mục ai người nấy sửa, nhánh theo tên, bắt buộc Pull Request): [CONTRIBUTING.md](CONTRIBUTING.md).
