# Báo cáo phân hệ Analytics & Machine Learning

## Kết luận theo bằng chứng hiện có

Mã nguồn đã triển khai các truy vấn Spark SQL, pipeline dự báo Spark MLlib bằng
`GBTRegressor`, mô hình baseline và bộ tính RMSE/MAE/MAPE. Hồ sơ vận hành ghi
nhận lần đánh giá toàn kỳ 2011-01–2026-08 trên 188 tháng, với split theo thời
gian và test set 3.772.274 dòng tổng hợp. Kết quả chi tiết, cấu hình và các CSV
nằm trong `artifacts/full_history_20261106/`.

## 1. Phân tích Spark SQL

Các hàm trong `modules/queries_analytics.py` tạo các kết quả:

| Bảng kết quả | Nhóm phân tích | Trường chính |
|---|---|---|
| `analytics_summary` | Khu vực đón × giờ | `PULocationID`, `pickup_hour`, `trip_count`, `avg_fare`, `avg_duration_min` |
| `fare_travel_by_hour` | Toàn thành phố × giờ | `pickup_hour`, `trip_count`, `avg_fare`, `avg_duration_min` |
| `demand_heatmap` | Thứ trong tuần × giờ | `day_of_week` (Spark: 1 = Chủ Nhật), `pickup_hour`, `trip_count` |
| `cancelled_trips` | Bản ghi raw có cước âm | Năm, khu vực, giờ, số dòng cước âm |

Các tổng hợp fare và duration loại thời lượng âm, giữ số chuyến làm trọng số để
Power BI có thể tính trung bình tổng thể có trọng số. Phân tích cước âm đọc bảng
raw vì ETL loại các dòng này khỏi bảng clean.

## 2. Mô hình dự báo

`modules/features.py` tổng hợp nhãn theo ngày × khu vực × giờ. Dữ liệu chia theo
thời gian, không xáo trộn ngẫu nhiên. Baseline lấy trung bình lịch sử theo
`PULocationID × pickup_hour`; GBT dùng đặc trưng khu vực, giờ, thứ và chỉ số
ngày. `passenger_count` không được đưa vào feature. Dự báo âm được chặn về 0.

`modules/metrics_eval.py` tính RMSE và MAE trên cùng test set; MAPE chỉ tính các
dòng có nhãn thực tế lớn hơn 0 để tránh chia cho 0. Pipeline ghi metrics và dự
báo test ra các bảng đầu ra được mô tả trong `README.md`.

## 3. Kết quả chạy đã được lưu hồ sơ

Một lần smoke run đã chạy 6/6 nhóm bước trên dữ liệu tháng `2024-01`, trong
521 giây: nạp clean/raw, tạo các tổng hợp, chia train/test,
fit GBT, tính metrics và ghi đầu ra thử lên HDFS. Bộ chạy này dùng `maxIter=10`
để kiểm tra đường dữ liệu. Hồ sơ ghi RMSE của baseline là **29,48**, GBT là
**48,00**; GBT kém hơn baseline trong cấu hình smoke một tháng đó. MAPE cũng
được in trong phiên chạy, nhưng hồ sơ bàn giao không giữ đủ các giá trị chi tiết
để lập bảng đầy đủ; không tự điền MAE/MAPE còn thiếu.

Một cache giao diện khác lưu kết quả cho `2026-08` với RMSE 42,1540 / 24,8012,
MAE 31,4025 / 16,5540 và MAPE 45,20% / 18,60% (baseline / GBT). Cache này không
kèm log chạy, tham số, phiên bản mã hoặc file metrics gốc để tái lập và đối chiếu
với smoke test tháng. Các số đó là snapshot cũ, không thay cho kết quả toàn kỳ
đã lưu trong artifact mới.

Lượt đánh giá toàn kỳ dùng aggregate checkpoint đã xác minh thay vì đọc lại bảng
clean: 1.506.791.871 chuyến tạo 17.452.027 dòng ngày × zone × giờ. Với
`maxIter=50`, train có 13.679.753 dòng và test có 3.772.274 dòng; cutoff là
2023-07-13. Baseline đạt RMSE 112,3053, MAE 57,6226, MAPE 306,0804%; GBT đạt
RMSE 42,2410, MAE 17,7288, MAPE 103,7799%. GBT giảm lỗi lần lượt 62,39%,
69,23% và 66,09% trên cùng tập test. MAPE chỉ tính các dòng có nhãn thực tế > 0.

Mô hình đã fit và dự báo xong; Spark gặp timeout khởi tạo Python worker khi ghi
CSV metrics sau đó. Metrics được phục hồi từ log Spark và predictions đã được
ghi ra. File mô hình chưa được lưu; runner đã được sửa để lưu model sớm hơn và
ghi bảng metric nhỏ trực tiếp bằng CSV khi chạy lại.

## 4. Trạng thái đáp ứng

| Yêu cầu | Trạng thái | Căn cứ / phần còn lại |
|---|---|---|
| Spark SQL phân tích nhu cầu theo khu vực và giờ | Đã triển khai; đã chạy smoke | `queries_analytics.py`; 5.296 nhóm zone×hour trong hồ sơ tháng 2024-01 |
| Giá cước và thời lượng di chuyển | Đã triển khai; đã chạy smoke | `fare_travel_by_hour`; hồ sơ smoke tạo 24 nhóm giờ |
| Heatmap 7 thứ × 24 giờ | Đã triển khai; đã chạy smoke | `demand_heatmap`; hồ sơ smoke tạo 168 ô |
| GBTRegressor và baseline | Đã fit trên train toàn kỳ | `maxIter=50`; chia theo thời gian, 13.679.753 train / 3.772.274 test |
| RMSE, MAE, MAPE | Đã tính và lưu artifact | Baseline: 112,3053 / 57,6226 / 306,0804%; GBT: 42,2410 / 17,7288 / 103,7799% |
| Đánh giá đại diện toàn giai đoạn | Hoàn tất | 188 tháng, 2011-01–2026-08; `evaluation_metrics.csv`, `predictions_zone_hour.csv` và manifest đã lưu |

## 5. Tái chạy và lưu bằng chứng

Lượt toàn kỳ đã dùng checkpoint `D:\CaptoneProject\full_history_resumed\daily_zone_hour.parquet`;
không đọc lại bảng clean. Để chạy lại với dữ liệu tháng mới, dùng
`3-analytics/run_full_history_evaluation.py` trên máy có JDK 17, PySpark và kết
nối Hive/HDFS; lưu log, tham số và manifest của lần chạy mới. Bảng kết quả hiện
tại ở `artifacts/full_history_20261106/`. Xem ma trận tổng thể tại
[`docs/PROJECT_REQUIREMENTS_STATUS.md`](../docs/PROJECT_REQUIREMENTS_STATUS.md).
