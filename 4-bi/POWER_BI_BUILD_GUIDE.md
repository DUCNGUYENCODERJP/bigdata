# Hướng dẫn và nghiệm thu report Power BI

Report dùng workbook cục bộ được tạo từ kết quả Spark toàn kỳ. Cách này giữ
nguồn refresh rõ ràng và không cần Hive ODBC trong lúc HiveServer2 đóng phiên.

## Nạp dữ liệu

Sau khi `run_full_history_evaluation.py` hoàn tất, tạo workbook:

```powershell
3-analytics/.venv/Scripts/python.exe `
  3-analytics/package_power_bi_data.py `
  3-analytics/artifacts/full_history_<run_id>
```

Workbook mặc định là `4-bi/power_bi_data/NYC_Taxi_Full_History.xlsx`. Nó có các
bảng Excel `taxi_zone_lookup`, `analytics_summary`, `fare_travel_by_hour`,
`demand_heatmap`, `monthly_trips`, `zone_demand`, `predictions`, `metrics` và
`run_manifest`; nếu có artifact đối soát, thêm `raw_reconciliation`. Nguồn
`taxi_zone_lookup` dùng file zone lookup của NYC TLC; các
bảng còn lại được tạo từ tổng hợp Spark và kết quả đánh giá ML.

Trong Power BI Desktop chọn **Home → Excel workbook**, mở workbook trên, chọn
các bảng trên rồi **Load**. Không nạp raw trips vào Power BI. Các query M tương
ứng nằm trong `power_query/`; nếu dựng query thủ công, tạo `00_full_history_workbook`
trước rồi tạo các query còn lại. Measures và calculated columns nằm trong `dax/`.

Khi có dữ liệu tháng mới, chạy lại phân tích toàn kỳ và đóng gói workbook; mở
report rồi chọn **Refresh**. Workbook giữ nguyên tên và đường dẫn để report đọc
bộ dữ liệu mới.

## Mô hình quan hệ

Tạo quan hệ một-nhiều, lọc một chiều:

| Bảng phía 1 | Bảng phía nhiều | Khóa |
|---|---|---|
| `taxi_zone_lookup[LocationID]` | `zone_demand[PULocationID]` | Mã zone đón |
| `taxi_zone_lookup[LocationID]` | `analytics_summary[PULocationID]` | Mã zone đón |
| `taxi_zone_lookup[LocationID]` | `predictions[PULocationID]` | Mã zone đón |

Không nối `demand_heatmap` với bảng zone. `fare_travel_by_hour` và `monthly_trips`
là tổng hợp độc lập theo giờ và theo tháng.

## Trang 1 — nhu cầu và vận hành

- KPI cards: `[Total_Trips]`, `[City_Avg_Fare]`, `[City_Avg_Duration]`.
- Line chart: `monthly_trips[month]` và `monthly_trips[trip_count]`.
- **Map/Azure Maps:** Location `zone_demand[MapLocation]`, Legend
  `zone_demand[Borough]`, Size `[Zone_Trip_Count]`.
- **Matrix heatmap:** Rows `demand_heatmap[Day_Name]`, Columns
  `demand_heatmap[Hour_Display]`, Values `[Heatmap_Trip_Count]`; sắp theo
  `Day_Sort_Order` và `Hour_Sort_Order`, tô nền theo thang màu.
- Line chart cước và thời lượng theo giờ từ `fare_travel_by_hour`.

`MapLocation` thêm zone, borough và thành phố để định vị điểm. Đây là bubble map
theo khu vực; bản đồ tô kín ranh giới cần geometry taxi-zone GeoJSON/TopoJSON.

## Trang 2 — đánh giá dự báo

- Table hoặc grouped bar chart so sánh RMSE, MAE, MAPE theo `metrics[model]`.
- Hiển thị baseline và GBT cùng test set, cấu hình/split từ `run_manifest`.
- Chart dự báo theo zone hoặc giờ dùng `predictions[actual_test_trips]`,
  `predictions[baseline_prediction]`, `predictions[gbt_prediction]`. Bảng này
  cộng gộp toàn bộ test period theo zone × giờ; không gắn nhãn thành chuỗi ngày.
- Chỉ số cải thiện âm là GBT có lỗi cao hơn baseline; không gắn nhãn tối ưu mặc định.

## Trang 3 — đối soát nguồn

Nếu workbook có bảng `raw_reconciliation`, thêm card `[Raw_Reconciliation_Delta]`
và table theo `month`, `manifest_rows`, `hive_raw_rows`,
`delta_hive_minus_manifest`. Tổng delta toàn kỳ phải bằng 0.

## Kiểm tra trước khi bàn giao

- Workbook `power_bi_data/NYC_Taxi_Full_History.xlsx` đã được tạo. Đã xác nhận
  lookup 265 dòng, monthly trips 188 dòng, heatmap 168 dòng, metrics 2 dòng,
  predictions 6.257 dòng và raw reconciliation 188 dòng (delta tổng bằng 0).
- Chưa refresh các bảng trong Power BI Desktop: connector Excel bị kẹt ở bước
  Next vì nút nằm trong cửa sổ WebView2 con không thể nhắm tới qua công cụ UI.
- Xác nhận ba quan hệ zone dùng `LocationID` duy nhất ở dimension.
- Map có điểm tại NYC và tooltip zone/borough; Matrix có đủ 168 ô và thứ tự
  Thứ Hai → Chủ Nhật, 00:00 → 23:00.
- RMSE/MAE/MAPE có hai dòng model và dùng cùng test set.
- Raw/manifest có 188 tháng và delta bằng 0.
- Chưa có `4-bi/NYC_Taxi_Analytics.pbix`; cần hoàn tất bước nạp workbook rồi lưu
  report native trong Power BI Desktop.

Microsoft mô tả Power BI Project dạng thư mục (PBIP) trong
[tài liệu dự án Power BI](https://learn.microsoft.com/en-us/power-bi/developer/projects/projects-overview).
