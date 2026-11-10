# Ma trận đáp ứng yêu cầu đồ án

Đối chiếu với yêu cầu: kho NYC TLC bằng HDFS/Hive/Parquet, phân tích bằng Spark
SQL hoặc Impala, dự báo zone × giờ bằng GBTRegressor đánh giá với baseline, và
trực quan Power BI gồm bản đồ khu vực cùng heatmap thứ × giờ.

| Yêu cầu | Mã/tài liệu | Bằng chứng và trạng thái |
|---|---|---|
| HDFS + Hive, Parquet, partition năm/tháng | `1-warehouse/`, `2-processing/src/writer.py` | Metadata Hive live trả về 188 partition giao nhau từ 2011-01 đến 2026-08; HDFS báo 35.788.700.805 byte và 1.447 file clean. Kết quả lấy ngày 2026-11-06, chưa quét lại bản ghi. |
| Phạm vi từ tháng cũ nhất đến tháng mới nhất, tự mở rộng theo tháng | `4-bi/dashboard_backend/warehouse.py`, `4-bi/dashboard_backend/sync.py`, `3-analytics/modules/loader.py` | Dashboard phát hiện giao partition clean/raw và cache chỉ tháng mới; analytics bỏ cận năm cố định và dùng max partition year. Có hiệu lực sau khi restart dashboard/chạy analytics với code mới. |
| Phân tích nhu cầu theo zone × giờ | `3-analytics/modules/queries_analytics.py` | Đã xuất dữ liệu toàn kỳ 2011-01–2026-08: 6.357 nhóm zone × giờ trong `analytics_summary.csv`. |
| Phân tích cước và thời lượng | `queries_analytics.py` | `fare_travel_by_hour.csv` có 24 nhóm giờ, gồm số chuyến, cước trung bình và thời lượng trung bình. |
| Heatmap thứ × giờ | `queries_analytics.py` | Đã xuất đủ 168 ô (7 × 24) trong `demand_heatmap.csv`. |
| GBTRegressor dự báo zone × giờ | `3-analytics/modules/features.py`, `gbt_model.py` | Đã fit 50 vòng trên tập train theo thời gian: 13.679.753 dòng aggregate; test có 3.772.274 dòng. |
| RMSE, MAE, MAPE so với baseline | `modules/metrics_eval.py`, `3-analytics/artifacts/full_history_20261106/` | Baseline 112,3053 / 57,6226 / 306,0804%; GBT 42,2410 / 17,7288 / 103,7799%. GBT giảm lỗi 62,39% / 69,23% / 66,09%. File model chưa lưu do Python worker timeout sau khi metrics đã được tính. |
| Bản đồ khu vực trong Power BI | `4-bi/power_query/01_taxi_zone_lookup.m`, `4-bi/POWER_BI_BUILD_GUIDE.md` | Workbook đã có `MapLocation` và bảng zone; chưa tạo `.pbix` hay xác thực geocoding trong Desktop. Power Query đã tới màn hình chọn bảng; thao tác nạp bị Windows chặn ở WebView2 con. |
| Heatmap Power BI thứ × giờ | `4-bi/power_query/03_demand_heatmap.m`, `4-bi/dax/01_calculated_columns.dax` | Workbook có 168 ô, nhãn và sort order; chưa nạp/kiểm chứng trực quan trong Desktop. |
| Đối soát raw theo manifest | `3-analytics/artifacts/warehouse_reconciliation_20261106.md`, `4-bi/cache/dashboard_hive_cache.json` | Đã đọc footer Parquet của đủ 188 file tháng: HDFS raw = manifest = 1.516.766.780, chênh lệch 0. Cache cũ cao hơn 51.342; cache đã được sửa theo số đã đối soát. Không phải quét 1,5 tỷ dòng. |

## Kết luận nghiệm thu

Phần kho dữ liệu, tổng hợp Spark SQL, đánh giá ML toàn kỳ và đối soát raw đã có
bằng chứng. Workbook Power BI đã được tạo và kiểm tra số bảng/số dòng. File
`.pbix`, refresh trong Desktop và xác nhận trực quan map/heatmap chưa hoàn tất.
Power Query đã tới màn hình chọn bảng và preview được dữ liệu; thao tác nạp bị
Windows chặn vì control nằm trong tiến trình WebView2 con.

Tại lần kiểm tra 2026-11-06, HDFS 8020, WebHDFS 9870, Hive Metastore 9083 và
HiveServer2 10000 mở TCP. Beeline bị peer đóng kết nối khi mở phiên; Spark đọc
được catalog metadata và xác nhận partition trên hai bảng vật lý mà không quét
chuyến đi. Dashboard 8080 hiện không chạy.
Khi khởi động lại `run_dashboard.ps1`, watcher giữ nguyên các tháng đã cache và
chỉ nạp partition tháng còn thiếu.

## Kiểm tra bàn giao lần cuối — 2026-11-06

- Kiểm tra cú pháp đạt: 49 file Python, 4 JavaScript, 5 PowerShell và YAML cấu
  hình. `git diff --check` không phát hiện whitespace error.
- `run_pipeline.ps1 -CheckOnly` đạt với JDK 17, Spark 4.2.0 và Hadoop đã cấu
  hình. Hive catalog xác nhận bảng vật lý `clean_nyc_taxi` và `raw_nyc_taxi`
  đều có đủ 188 partition. Bảng `clean_nyc_taxi_tlc` là view tương thích nên
  không dùng trực tiếp cho `SHOW PARTITIONS`.
- Dry-run batch 2011-01–2026-08 không ghi dữ liệu; trên máy này thư mục staging
  raw/clean cục bộ rỗng nên nếu chạy lại từ đầu sẽ tải khoảng 19,5 GiB (ước tính
  khoảng 7,2 giờ). Không chạy lại ETL toàn kỳ vì HDFS/Hive đã có đủ partition.
- Đã đọc và đối chiếu workbook: đủ 10 bảng/trang tính, gồm map lookup,
  predictions, metrics, reconciliation và heatmap 168 ô. Workbook chưa được
  nạp vào model Power BI Desktop; `.pbix` chưa tồn tại. Hộp thoại Power Query
  đã hiện preview, nhưng Windows từ chối click/keyboard vì control nằm trong
  tiến trình WebView2 khác cửa sổ Power BI được điều khiển. Vì vậy map và
  heatmap chưa được xác nhận trực quan trong Power BI.
- Dashboard local cổng 8080 chưa chạy. Báo cáo ML và predictions đã có; model
  GBT chưa lưu được do Spark Python worker timeout sau khi hoàn tất đánh giá.

**Trạng thái bàn giao:** dữ liệu kho, tổng hợp Spark SQL, đánh giá ML và đối soát
raw đã có bằng chứng; yêu cầu Power BI chưa hoàn tất cho đến khi workbook được
nạp, map/heatmap được kiểm tra trực quan và file `.pbix` được lưu.
