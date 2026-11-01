# Schema dữ liệu Yellow Taxi đã làm sạch

Sinh tự động từ schema thật của partition trên đĩa, không viết tay.

- Sinh lúc: 2026-10-29T19:21:14+07:00
- Nguồn: NYC TLC Yellow Taxi, phạm vi manifest 188 tháng
- Output: `D:\BIGDATA\2-processing\data\clean\trips`, Parquet Snappy, phân vùng `year=/month=`
- Partition đối chiếu: 188; partition lệch schema: 0
- Tổng dòng trên đĩa: 1,506,791,871
- Ngưỡng làm sạch: duration trong (0, 180] phút, trip_distance trong [0, 100] dặm, average_speed <= 100 mph cho chuyến >= 1 phút, max_drop_rate = 8%, nạp theo năm 2025 = 12%

## Cột vật lý trong file (23)

| Tên cột | Kiểu Spark | Đơn vị / miền giá trị | Ý nghĩa |
|---|---|---|---|
| `pickup_datetime` | `timestamp_ntz` | — | Thời điểm đón theo giờ địa phương NYC. |
| `pickup_date` | `date` | — | Ngày pickup, suy ra từ pickup_datetime. |
| `pickup_hour_ts` | `timestamp` | — | pickup_datetime làm tròn xuống đầu giờ. |
| `dropoff_datetime` | `timestamp_ntz` | — | Thời điểm trả khách theo giờ địa phương NYC. |
| `day` | `int` | 1-31 | Ngày trong tháng theo pickup. |
| `hour` | `int` | 0-23 | Giờ pickup trong ngày, 0-23. |
| `day_of_week` | `int` | 1=CN .. 7=T7 | Thứ theo Spark: 1=Chủ nhật, 7=Thứ bảy. |
| `pickup_location_id` | `int` | LocationID TLC | Mã TLC taxi zone tại điểm đón. |
| `is_unknown_zone` | `boolean` | — | True khi một LocationID không có trong lookup, hoặc lookup ghi Borough 'Unknown' / Zone 'N/A'. |
| `pickup_borough` | `string` | — | Borough tại điểm đón từ Taxi Zone Lookup. |
| `pickup_zone` | `string` | — | Tên zone tại điểm đón. |
| `dropoff_location_id` | `int` | LocationID TLC | Mã TLC taxi zone tại điểm trả. |
| `dropoff_borough` | `string` | — | Borough tại điểm trả từ Taxi Zone Lookup. |
| `dropoff_zone` | `string` | — | Tên zone tại điểm trả. |
| `trip_distance` | `double` | dặm | Quãng đường dặm; giá trị 0 được chuyển thành null. |
| `is_distance_missing` | `boolean` | — | True khi trip_distance gốc bằng 0 trước chuẩn hóa. |
| `trip_duration_min` | `double` | phút | Thời lượng chuyến theo phút. |
| `is_short_trip` | `boolean` | — | True khi thời lượng dưới short_trip_max_minutes; dòng vẫn giữ. |
| `passenger_count` | `bigint` | — | Số hành khách khai báo; 0 đổi thành null; chỉ mô tả, không làm feature. |
| `fare_amount` | `double` | USD | Cước USD. |
| `total_amount` | `double` | USD | Tổng tiền USD. |
| `tip_amount` | `double` | USD | Tip USD. |
| `tolls_amount` | `double` | USD | Phí cầu đường USD. |

## Cột phân vùng (2)

`year` và `month` là cột phân vùng Hive-style; Spark nối chúng khi đọc nên DataFrame đầu ra có 25 cột.

| Tên cột | Kiểu Spark | Ý nghĩa |
|---|---|---|
| `year` | `int` | Năm pickup; cột phân vùng. |
| `month` | `int` | Tháng pickup; cột phân vùng. |

## Cờ chất lượng

- `is_distance_missing`: trip_distance gốc bằng 0 đã được set null, dòng vẫn giữ.
- `is_short_trip`: chuyến dưới ngưỡng vẫn giữ, chỉ gắn cờ để lọc khi làm feature.
- `is_unknown_zone`: id không có trong lookup HOẶC lookup gán Borough 'Unknown' / Zone 'N/A'.

## Feature cho mô hình

- `passenger_count` chỉ dùng mô tả, tuyệt đối không đưa vào features dự báo.
