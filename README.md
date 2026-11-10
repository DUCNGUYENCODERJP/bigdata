# Nền Tảng Big Data & Học Máy — NYC TLC Yellow Taxi

Hệ thống xử lý, phân tích và dự báo nhu cầu giao thông công cộng quy mô lớn dựa trên dữ liệu NYC TLC Yellow Taxi (188 tháng, hơn 1.5 tỷ bản ghi) thông qua kiến trúc phân tán Hadoop, Hive, PySpark và Power BI. Bài tập lớn 4 người: dữ liệu đi từ raw → làm sạch bằng PySpark → warehouse phân vùng → Spark SQL/ML → dashboard.

---

## 🏗️ Kiến trúc hệ thống, phân công và trạng thái

| Phân hệ | Thư mục | Người | Chức năng chính | Trạng thái | Báo cáo chi tiết |
|---|---|---|---|:---:|:---:|
| **1. Data Warehouse** | [`1-warehouse/`](1-warehouse/) | Người 1 | Kiến trúc cụm Hadoop 3.4.1 HDFS, Hive Metastore, thiết kế phân vùng Parquet `year=/month=` | ✅ xong, đã nghiệm thu — Người 2 dựng thay trên Docker ngày 2026-11-03 | [Báo cáo Kho Dữ Liệu](1-warehouse/BAO_CAO_DATA_WAREHOUSE.md) |
| **2. Data Processing** | [`2-processing/`](2-processing/) | Người 2 | Pipeline PySpark ETL tải dữ liệu, kiểm định chất lượng, lọc sạch ngoại lai và nạp kho | ✅ xong, đã nghiệm thu | [Báo cáo Xử Lý Dữ Liệu](2-processing/BAO_CAO_DATA_PROCESSING.md) |
| **3. Analytics & ML** | [`3-analytics/`](3-analytics/) | Người 3 | Spark SQL đa chiều, EDA, GBTRegressor và đối soát Baseline bằng RMSE/MAE/MAPE | ✅ xong | [Báo cáo Analytics & ML](3-analytics/BAO_CAO_ANALYTICS_ML.md) |
| **4. BI & Dashboard** | [`4-bi/`](4-bi/) | Người 4 | Dashboard trực quan tương tác 3 trang, Data Model DAX, tích hợp báo cáo điều hành | ✅ xong | [Báo cáo BI & Dashboard](4-bi/BAO_CAO_BI_DASHBOARD.md) |

---

## 📊 Quy mô dữ liệu và điểm giao kỹ thuật

- **Dữ liệu đã làm sạch:** Parquet Snappy phân vùng `year=/month=`, **25 cột** (23 cột vật lý + `year`/`month`), 188 tháng (`2011-01 → 2026-08`), **1.506.791.871 dòng** (33.59 GiB). Schema chuẩn: [`2-processing/docs/schema.md`](2-processing/docs/schema.md).
- **Kho dữ liệu phân tán:** Hadoop 3.4.1 + Hive Metastore chạy Docker trên máy của Người 2.
  - Bảng `default.clean_nyc_taxi`: 1.506.791.871 dòng sạch.
  - Bảng `default.raw_nyc_taxi`: 1.516.766.780 dòng gốc TLC, bao gồm 5.026.251 cuốc xe có `fare_amount < 0` (0.33%).
  - View `default.clean_nyc_taxi_tlc`: View trùng tên cột với file gốc, dành cho code `3-analytics`. Cách nối: [`1-warehouse/README.md`](1-warehouse/README.md).
- **Quy ước chuẩn hóa:**
  - `day_of_week`: Quy ước Spark (**1 = Chủ Nhật**, 2 = Thứ Hai, ..., 7 = Thứ Bảy).
  - `timezone`: Giờ địa phương New York (`America/New_York`), không kèm offset.
- **Bàn giao của Người 2 cho cả nhóm**: [2-processing/docs/ban_giao_nhom.md](2-processing/docs/ban_giao_nhom.md) (bản ngắn) và [2-processing/docs/handover.md](2-processing/docs/handover.md) (bản kỹ thuật; mục 9 là trạng thái mới nhất, có đính chính các mục cũ).
- **Dữ liệu không nằm trong repo** (33 GB sạch + 20 GB raw). Lấy lại từ đầu: mục 4 của `2-processing/docs/handover.md`; lấy từ kho đang chạy: `1-warehouse/README.md`.

---

## 🚀 Hướng dẫn vận hành từng phân hệ

1. **Khởi động cụm Data Warehouse:** Xem hướng dẫn tại [`1-warehouse/README.md`](1-warehouse/README.md).
2. **Chạy Pipeline ETL:** Xem hướng dẫn tại [`2-processing/README.md`](2-processing/README.md).
3. **Chạy Pipeline Analytics & ML:** Chạy `.\run_pipeline.ps1` trong thư mục [`3-analytics/`](3-analytics/).
4. **Mở Dashboard:** `.\run_dashboard.ps1` ở gốc repo — dashboard đọc trực tiếp Hive đang chạy; [`4-bi/dashboard.html`](4-bi/dashboard.html) là bản tĩnh và [`4-bi/`](4-bi/) nạp được vào Power BI Desktop.

---

## 🤝 Làm việc chung

Quy tắc để không giẫm chân nhau (thư mục ai người nấy sửa, nhánh theo tên, bắt buộc Pull Request): [CONTRIBUTING.md](CONTRIBUTING.md).
