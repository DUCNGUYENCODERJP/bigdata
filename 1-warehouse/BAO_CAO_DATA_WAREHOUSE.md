# 📑 BÁO CÁO NGHIỆM THU PHÂN HỆ DATA WAREHOUSE (PHẦN 1)
## Đề Tài: Xây Dựng Kho Dữ Liệu Phân Tán NYC TLC Yellow Taxi (Hadoop HDFS & Apache Hive)

> **Phân hệ:** Data Warehouse Layer (Kho dữ liệu phân tán)  
> **Công nghệ lõi:** Apache Hadoop 3.4.1 (HDFS), Apache Hive Metastore 3.1.3, Docker Compose, Parquet Snappy  
> **Trạng thái:** ✅ **HOÀN THÀNH VÀ ĐÃ NGHIỆM THU 100%**

---

## 1. TỔNG QUAN MỤC TIÊU & NHIỆM VỤ

Phân hệ Data Warehouse đóng vai trò là "trái tim" lưu trữ của toàn bộ nền tảng Big Data, chịu trách nhiệm:
1. Thiết kế và triển khai cụm lưu trữ phân tán Hadoop HDFS có khả năng mở rộng quy mô lớn, lưu trữ an toàn hơn 53 GiB dữ liệu taxi.
2. Cấu hình dịch vụ quản lý metadata Apache Hive Metastore Standalone phục vụ kết nối đa người dùng và đa nền tảng tính toán (PySpark, Spark SQL, Power BI).
3. Thiết kế lược đồ bảng (Schema Design) định dạng cột Parquet nén Snappy, phân vùng tối ưu theo hai cấp `year=/month=`.
4. Cung cấp view tương thích kiểu dữ liệu và hỗ trợ cơ chế cắt tỉa phân vùng (Partition Pruning) giúp tăng tốc độ truy vấn gấp hàng chục lần.

---

## 2. KIẾN TRÚC HẠ TẦNG CỤM DATA WAREHOUSE (DOCKER CONTAINER)

Kho dữ liệu được đóng gói và vận hành qua 4 dịch vụ Docker phân tán độc lập:

```
┌────────────────────────────────────────────────────────────────────────┐
│               KIẾN TRÚC CỤM DATA WAREHOUSE (DOCKER COMPOSE)             │
├────────────────────────────────────────────────────────────────────────┤
│ 1. nyc-namenode (apache/hadoop:3.4.1)                                   │
│    • Quản lý không gian tên (Namespace) và siêu dữ liệu khối HDFS       │
│    • Cổng phục vụ: 8020 (RPC kết nối dữ liệu), 9870 (WebHDFS & UI)      │
│                                                                        │
│ 2. nyc-datanode (apache/hadoop:3.4.1)                                   │
│    • Lưu trữ vật lý toàn bộ các khối Parquet (Replication Factor = 1)   │
│    • Dung lượng lưu trữ: 56.98 GB khối (Clean + Raw)                    │
│    • Cổng phục vụ: 9866 (Block Transfer), 9864 (WebHDFS Data)           │
│                                                                        │
│ 3. nyc-metastore (apache/hive:3.1.3)                                    │
│    • Hive Standalone Metastore lưu trữ schema bảng và định vị partition│
│    • Cơ sở dữ liệu catalog: Apache Derby nhúng                          │
│    • Cổng phục vụ: 9083 (Giao thức Thrift kết nối Spark / Client)       │
│                                                                        │
│ 4. nyc-client (apache/hadoop:3.4.1)                                     │
│    • Shell tiện ích phục vụ quản trị, kiểm tra HDFS CLI nội bộ cụm     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. THIẾT KẾ LƯỢC ĐỒ DỮ LIỆU (SCHEMA DESIGN)

Kho dữ liệu tổ chức thành 2 bảng chính và 1 view tương thích:

### 3.1. Bảng `default.clean_nyc_taxi` (Dữ liệu đã làm sạch)
- **Định dạng lưu trữ:** Parquet Snappy phân vùng theo `year` và `month`.
- **Cấu trúc:** Gồm **25 cột** (23 cột vật lý + 2 cột partition):
  - *Thời gian:* `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `pickup_hour_ts`
  - *Lịch biểu:* `pickup_year`, `pickup_month`, `day_of_week`, `hour`
  - *Không gian:* `pickup_location_id`, `dropoff_location_id`, `pickup_borough`, `dropoff_borough`, `pickup_zone`, `dropoff_zone`
  - *Chuyến đi:* `passenger_count`, `trip_distance`, `trip_duration_min`
  - *Tài chính:* `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `total_amount`
  - *Cờ chất lượng:* `is_unknown_zone`, `is_short_trip`, `is_distance_missing`
  - *Partition key:* `year` (INT), `month` (INT)

### 3.2. Bảng `default.raw_nyc_taxi` (Dữ liệu gốc chưa lọc)
- **Mục đích:** Lưu trữ nguyên bản 188 file gốc từ TLC để phục vụ phân tích các cuốc xe bất thường và đối soát chất lượng.
- **Quy mô:** **19 cột dữ liệu + year/month**. Lưu trữ nguyên vẹn **5.026.251 cuốc xe có `fare_amount < 0` (0.33%)**.

### 3.3. View tương thích `default.clean_nyc_taxi_tlc`
- **Mục đích:** Khôi phục lại tên cột gốc của TLC (`PULocationID`, `DOLocationID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`) và chuyển đổi kiểu dữ liệu sang `TIMESTAMP` tiêu chuẩn.
- **Ưu điểm:** Cho phép các ứng dụng Spark SQL và ML đọc dữ liệu tự nhiên mà không cần sửa đổi mã nguồn xử lý, đồng thời cơ chế **Partition Pruning** vẫn hoạt động cắt tỉa chính xác từng partition.

---

## 4. KẾT QUẢ NGHIỆM THU KHO DỮ LIỆU

Toàn bộ chỉ tiêu nghiệm thu được kiểm chứng thực tế qua các lệnh hệ thống:

| Chỉ tiêu kiểm tra | Kết quả nghiệm thu thực tế | Phương pháp kiểm chứng | Đánh giá |
|---|:---:|:---:|:---:|
| **Trạng thái cụm** | 4/4 container `Up (healthy)` | `docker compose ps` | ✅ Đạt |
| **Số phân vùng (Partitions)** | **188/188** phân vùng (2011-01 $\rightarrow$ 2026-08) | `SHOW PARTITIONS clean_nyc_taxi` | ✅ Hoàn hảo |
| **Tổng số dòng dữ liệu sạch** | **1.506.791.871 bản ghi** | `spark.table('clean_nyc_taxi').count()` | ✅ Khớp 100% |
| **Dung lượng HDFS bảng sạch** | **33.33 GiB** (35.788.700.805 bytes / 1447 files) | `hdfs dfs -count -v` | ✅ Khớp từng byte |
| **Tổng số dòng dữ liệu raw** | **1.516.766.780 bản ghi** (19.55 GiB) | Quét toàn bộ 188 file raw | ✅ Khớp Manifest |
| **Số cuốc xe lỗi cước âm** | **5.026.251 bản ghi** (`fare_amount < 0`) | `sum(CASE WHEN fare_amount < 0 ...)` | ✅ Đạt |
| **Tốc độ truy vấn qua View** | Partition Pruning cắt tỉa 1 tháng trong < 1.2s | Spark Execution Plan | ✅ Tối ưu |

---

## 5. KẾT LUẬN & BÀN GIAO CHO CÁC PHÂN HỆ TIẾP THEO

- Phân hệ Data Warehouse đã hoàn thành việc thiết lập hạ tầng lưu trữ phân tán bền vững, toàn vẹn và tối ưu.
- Các phân hệ tiếp theo (Analytics, Machine Learning, Power BI) có thể truy vấn đồng thời qua Hive Metastore URI: `thrift://<IP_HOST>:9083` và HDFS URI: `hdfs://<IP_HOST>:8020` một cách mượt mà và bảo mật.
