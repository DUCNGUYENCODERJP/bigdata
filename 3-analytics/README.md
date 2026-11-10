# NYC Taxi Analytics & Machine Learning Pipeline

Pipeline Spark SQL và machine learning cho NYC TLC Yellow Taxi. Pipeline đọc
Hive, tạo các bảng phân tích, huấn luyện GBTRegressor so với baseline, tính
RMSE/MAE/MAPE và ghi kết quả ra Parquet/CSV.

Nguồn đầu vào duy nhất là các bảng Hive SQL. Nếu không truy cập được bảng hoặc
thiếu cột bắt buộc, pipeline dừng với lỗi; không đọc fallback từ đường dẫn
Parquet.

## Nguồn dữ liệu

Trên warehouse của nhóm, dùng view **`default.clean_nyc_taxi_tlc`** cho dữ liệu
sạch và bảng **`default.raw_nyc_taxi`** cho phân tích cuốc hủy. Không dùng
`clean_nyc_taxi` trực tiếp: view TLC khôi phục tên cột `PULocationID` và kiểu
timestamp mà code yêu cầu. Pipeline giữ pickup year từ **2011 đến năm partition
mới nhất đang có trong Hive**; không có mốc năm kết thúc cố định nên dữ liệu của
các năm mới tự được nhận. Cận trên theo partition cũng loại timestamp sai tới
năm 2098 trong bảng raw.

Các lưu ý dữ liệu đã áp dụng:

- Cuốc có `fare_amount < 0` đã bị loại khỏi bảng clean; phân tích cuốc hủy phải
  đọc raw.
- `passenger_count` không được dùng làm feature.
- Snapshot warehouse hiện ghi nhận dữ liệu từ 2011-01 đến 2026-08; pipeline mặc
  định dùng từ partition sạch cũ nhất (tối thiểu 2011-01) tới partition mới nhất.
  Không đặt cận trên năm cố định, nên các tháng/năm mới được nhận ở lần chạy
  analytics kế tiếp. Mô hình train trên 80% ngày đầu theo thời gian và đánh giá
  trên 20% ngày cuối.

## Điều kiện chạy từ PC riêng

- JDK 17 (không dùng Java 21/25).
- Hadoop Windows binaries tương thích: `winutils.exe` và `hadoop.dll`; đặt
  `HADOOP_HOME` trỏ tới thư mục Hadoop chứa `bin`.
- PySpark 4.0.1 và NumPy trong cùng Python environment mà `spark-submit` sử
  dụng; xác nhận environment đó `import pyspark` được.
- Kết nối mạng tới máy chạy warehouse, Hive Metastore `9083`, HDFS NameNode
  `8020` và địa chỉ DataNode mà NameNode quảng bá.
- Location của bảng và từng partition Hive phải trỏ tới HDFS NameNode hiện tại;
  đổi `fs.defaultFS` ở client không sửa được location cũ đã lưu trong Metastore.

## Chạy pipeline từ PowerShell

Đổi IP nếu máy warehouse đổi mạng. Lệnh bên dưới dùng tên view bắt buộc và ghi
output lên HDFS:

```powershell
Set-Location <repo>\3-analytics
spark-submit --conf spark.sql.catalogImplementation=hive `
  --conf spark.hadoop.hive.metastore.uris=thrift://192.168.100.17:9083 `
  --conf spark.hadoop.fs.defaultFS=hdfs://192.168.100.17:8020 `
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
  --conf "spark.driver.extraJavaOptions=-Djdk.net.hosts.file=C:/hadoop/custom_hosts" `
  --conf "spark.executor.extraJavaOptions=-Djdk.net.hosts.file=C:/hadoop/custom_hosts" `
  main.py --clean-table default.clean_nyc_taxi_tlc --raw-table default.raw_nyc_taxi `
  --output-base hdfs:///user/hive/warehouse/nyc_taxi_analytics
```

`--max-iter` mặc định là **50**. Không nên đánh giá GBT chỉ từ smoke test một
tháng hoặc số vòng lặp thấp; dùng toàn bộ năm/dải dữ liệu trước khi kết luận về
chất lượng mô hình. Nếu chỉ muốn chạy analytics/ML khi bảng raw không truy cập
được, thêm `--skip-cancelled`; nếu không, lỗi đọc raw sẽ làm job thất bại để
không báo thành công khi thiếu một đầu ra.

Xem toàn bộ tham số bằng `spark-submit main.py --help`.

## Nội dung đầu ra

Mọi đầu ra nằm dưới `--output-base`:

| Thư mục | Nội dung |
|---|---|
| `analytics_summary/` | Số chuyến, fare trung bình, thời lượng trung bình theo pickup zone × giờ |
| `fare_travel_by_hour/` | Số chuyến, fare và thời lượng theo giờ |
| `demand_heatmap/` | Nhu cầu theo giờ × thứ trong tuần (1 = Chủ nhật) |
| `cancelled_trips/` | Số cuốc raw có fare âm theo năm × zone × giờ |
| `predictions/` | Actual, baseline và GBT prediction trên test set theo thời gian |
| `metrics/` | CSV RMSE, MAE, MAPE của baseline và GBT |

Pipeline chia train/test theo thứ tự thời gian (mặc định 80/20), fit baseline
chỉ trên train, rồi so sánh hai mô hình trên cùng test set. GBT dự báo nhu cầu
theo zone/giờ/thứ/ngày; giá trị dự báo âm được chặn về 0.

## Trạng thái kiểm chứng

Code có thể chạy với warehouse đã đăng ký và các điều kiện môi trường ở trên.
Để lưu một artifact đánh giá toàn kỳ mà không ghi đè các bảng analytics Hive,
chạy `run_full_history_evaluation.py`. Script tìm giao partition clean/raw,
đọc các cột chuẩn hóa cần thiết từ bảng clean vật lý trong một lượt, rồi lưu
metrics, mô hình và các CSV Power BI vào thư mục timestamp dưới `artifacts/`.
Mặc định dùng JDK 17, `local[6]`, chronological split 80/20 và `maxIter=50`;
có thể cấu hình bằng tham số CLI. Thời gian chạy phụ thuộc thông lượng HDFS.

Sau khi script hoàn tất, cài dependency tùy chọn và đóng gói dữ liệu Power BI:

```powershell
3-analytics/.venv/Scripts/python.exe -m pip install -r 3-analytics/requirements-powerbi.txt
3-analytics/.venv/Scripts/python.exe 3-analytics/package_power_bi_data.py `
  3-analytics/artifacts/full_history_<run_id>
```

Workbook đầu ra mặc định là `4-bi/power_bi_data/NYC_Taxi_Full_History.xlsx`.
Power BI nạp các bảng đã tổng hợp; không đưa 1,5 tỷ chuyến raw vào model.
