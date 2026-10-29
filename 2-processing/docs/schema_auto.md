# Schema dữ liệu sạch

Schema dưới đây được sinh từ DataFrame đầu ra thực tế của pipeline.

| Cột | Kiểu Spark | Ý nghĩa |
|---|---|---|
| `pickup_datetime` | `timestamp_ntz` | Thời điểm đón theo giờ địa phương của chuyến. |
| `pickup_date` | `date` |  |
| `pickup_hour_ts` | `timestamp` |  |
| `dropoff_datetime` | `timestamp_ntz` | Thời điểm trả khách theo giờ địa phương của chuyến. |
| `year` | `int` | Năm pickup; partition năm. |
| `month` | `int` | Tháng pickup; partition tháng. |
| `day` | `int` | Ngày trong tháng theo pickup. |
| `hour` | `int` | Giờ trong ngày theo pickup (0-23). |
| `day_of_week` | `int` | Thứ theo Spark dayofweek: 1=Chủ nhật, 7=Thứ bảy. |
| `pickup_location_id` | `int` | Mã TLC taxi zone tại điểm đón. |
| `is_unknown_zone` | `boolean` | True khi một LocationID không có trong lookup, hoặc lookup ghi Borough 'Unknown' / Zone 'N/A' cho zone đó. |
| `pickup_borough` | `string` | Quận tại điểm đón từ Taxi Zone Lookup. |
| `pickup_zone` | `string` | Tên taxi zone tại điểm đón. |
| `dropoff_location_id` | `int` | Mã TLC taxi zone tại điểm trả. |
| `dropoff_borough` | `string` | Quận tại điểm trả từ Taxi Zone Lookup. |
| `dropoff_zone` | `string` | Tên taxi zone tại điểm trả. |
| `trip_distance` | `double` | Quãng đường khai báo, đơn vị dặm; giá trị 0 được chuyển thành null. |
| `is_distance_missing` | `boolean` | True khi trip_distance gốc bằng 0 trước chuẩn hóa. |
| `trip_duration_min` | `double` | Thời lượng từ pickup đến dropoff, đơn vị phút. |
| `is_short_trip` | `boolean` | True khi thời lượng chuyến dưới ngưỡng short_trip_max_minutes; dòng vẫn được giữ. |
| `passenger_count` | `bigint` | Số hành khách khai báo; 0 được đổi thành null; chỉ mô tả, không dùng làm feature. |
| `fare_amount` | `double` | Giá cước tính theo đồng USD. |
| `total_amount` | `double` | Tổng tiền chuyến tính theo đồng USD. |
| `tip_amount` | `double` | Tiền tip tính theo đồng USD. |
| `tolls_amount` | `double` | Phí cầu đường tính theo đồng USD. |
