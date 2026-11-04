# Business Intelligence & Dashboard Integration

Thư mục dành cho việc xây dựng Dashboard, kết nối kho dữ liệu / kết quả Machine Learning và trực quan hóa toàn hệ thống.

---

## 📁 Cấu trúc thư mục mô-đun

```
4-bi/
├── dashboard.html             # Giao diện Dashboard Web tương tác trực tiếp
├── README.md                  # Giới thiệu phân hệ BI
├── BAO_CAO_BI_DASHBOARD.md    # Báo cáo nghiệm thu & Kịch bản thuyết trình bảo vệ đồ án
│
├── assets/                    # Tài nguyên giao diện Web Dashboard
│   ├── css/
│   │   └── dashboard.css      # Toàn bộ CSS giao diện, màu sắc, bố cục
│   └── js/
│       ├── app.js             # Điều hướng tab và sự kiện chung
│       ├── heatmap.js         # Thuật toán ma trận nhiệt 24x7
│       └── charts.js          # Cấu hình biểu đồ Chart.js (thị phần, xu hướng, ML)
│
├── dax/                       # Bộ công thức DAX cho Power BI Desktop (chia theo chức năng)
│   ├── 01_calculated_columns.dax   # Đổi tên thứ, định dạng giờ, tính sai số
│   ├── 02_kpi_measures.dax         # Thước đo tổng chuyến, cước TB, thời gian TB
│   └── 03_ml_metrics_measures.dax  # Thước đo so sánh % cải thiện RMSE/MAE/MAPE
│
└── power_query/               # Script nạp dữ liệu Power Query (M) cho từng bảng
    ├── 01_taxi_zone_lookup.m
    ├── 02_fare_travel_by_hour.m
    ├── 03_demand_heatmap.m
    ├── 04_analytics_summary.m
    ├── 05_predictions.m
    └── 06_metrics.m
```
