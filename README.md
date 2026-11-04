# Nền Tảng Big Data & Học Máy — NYC TLC Yellow Taxi

Hệ thống xử lý, phân tích và dự báo nhu cầu giao thông công cộng quy mô lớn dựa trên dữ liệu NYC TLC Yellow Taxi (188 tháng, hơn 1.5 tỷ bản ghi) thông qua kiến trúc phân tán Hadoop, Hive, PySpark và Power BI.

---

## 🏗️ Kiến Trúc Hệ Thống & Phân Hệ Chức Năng

| Phân hệ | Thư mục | Chức năng chính | Trạng thái | Báo cáo chi tiết |
|---|---|---|:---:|:---:|
| **1. Data Warehouse** | [`1-warehouse/`](1-warehouse/) | Kiến trúc cụm Hadoop 3.4.1 HDFS, Hive Metastore, thiết kế phân vùng Parquet `year=/month=` | ✅ Hoàn thành | [Báo cáo Kho Dữ Liệu](1-warehouse/BAO_CAO_DATA_WAREHOUSE.md) |
| **2. Data Processing** | [`2-processing/`](2-processing/) | Pipeline PySpark ETL tải dữ liệu, kiểm định chất lượng, lọc sạch ngoại lai và nạp kho | ✅ Hoàn thành | [Báo cáo Xử Lý Dữ Liệu](2-processing/BAO_CAO_DATA_PROCESSING.md) |
| **3. Analytics & ML** | [`3-analytics/`](3-analytics/) | Spark SQL đa chiều, mô hình GBTRegressor và đối soát Baseline bằng RMSE/MAE/MAPE | ✅ Hoàn thành | [Báo cáo Analytics & ML](3-analytics/BAO_CAO_ANALYTICS_ML.md) |
| **4. BI & Dashboard** | [`4-bi/`](4-bi/) | Dashboard trực quan tương tác 3 trang, Data Model DAX và tích hợp báo cáo điều hành | ✅ Hoàn thành | [Báo cáo BI & Dashboard](4-bi/BAO_CAO_BI_DASHBOARD.md) |

---

## 📊 Quy Mô Dữ Liệu & Điểm Giao Kỹ Thuật

- **Dữ liệu đã làm sạch:** Parquet Snappy phân vùng `year=/month=`, **25 cột** (23 cột vật lý + `year`/`month`), 188 tháng (`2011-01 → 2026-08`), **1.506.791.871 dòng** (33.59 GiB). Schema chuẩn: [`2-processing/docs/schema.md`](2-processing/docs/schema.md).
- **Kho dữ liệu phân tán:**
  - Bảng `default.clean_nyc_taxi`: 1.506.791.871 dòng sạch.
  - Bảng `default.raw_nyc_taxi`: 1.516.766.780 dòng gốc TLC, bao gồm 5.026.251 cuốc xe có `fare_amount < 0` (0.33%).
  - View `default.clean_nyc_taxi_tlc`: View tương thích tên cột và kiểu thời gian cho các ứng dụng phân tích.
- **Quy ước chuẩn hóa:**
  - `day_of_week`: Quy ước Spark (**1 = Chủ Nhật**, 2 = Thứ Hai, ..., 7 = Thứ Bảy).
  - `timezone`: Giờ địa phương New York (`America/New_York`), không kèm offset.

---

## 🚀 Hướng Dẫn Vận Hành Từng Phân Hệ

1. **Khởi động cụm Data Warehouse:** Xem hướng dẫn tại [`1-warehouse/README.md`](1-warehouse/README.md).
2. **Chạy Pipeline ETL:** Xem hướng dẫn tại [`2-processing/README.md`](2-processing/README.md).
3. **Chạy Pipeline Analytics & ML:** Chạy `.\run_pipeline.ps1` trong thư mục [`3-analytics/`](3-analytics/).
4. **Mở Dashboard Trực Quan Hóa:** Mở trực tiếp [`4-bi/dashboard.html`](4-bi/dashboard.html) trên trình duyệt hoặc nạp vào Power BI Desktop.
