# Tài liệu bàn giao — phần Xử lý dữ liệu (Người 2)

**Gửi:** Người 1 (Data Warehouse), Người 3 (Analytics & ML), Người 4 (BI & Integration)
**Ngày:** 2026-10-29 · **Trạng thái:** hoàn thành phần kỹ thuật, còn 1 mục chờ Người 1 chốt (xem mục 6)

---

## 1. Tóm tắt 30 giây

Toàn bộ Yellow Taxi TLC từ **2011-01 đến 2026-08 (188 tháng)** đã được tải, chuẩn hoá, làm sạch bằng PySpark và ghi thành **Parquet phân vùng `year=/month=`**: **1.506.791.871 dòng**, **33,59 GiB**, **188/188 partition**, **0 lệch schema giữa các partition**, chỉ loại **0,658%** dòng. Code, tài liệu schema và báo cáo nghiệm thu ở:

> **https://github.com/DUCNGUYENCODERJP/bigdata** (branch `main`)

Dữ liệu 34 GB **không nằm trên GitHub** — GitHub chặn file >100 MB và tài khoản Drive nhóm chỉ có 5 GB. Cách lấy dữ liệu: mục 6.

---

## 2. Bạn nhận được gì

| Thành phần | Đường dẫn | Ghi chú |
|---|---|---|
| Pipeline | `src/` | download → quality → clean → transform → write → postcheck |
| Kiểm thử | `tests/` | 34 test, chạy `python -m pytest -q`, không cần mạng |
| Cấu hình | `config.yaml` | ngưỡng làm sạch, đường dẫn, sink local/hive |
| Wrapper batch | `scripts/run_batch.ps1` | chạy 188 tháng, tự resume, không fail cả batch |
| Schema | `docs/schema.md` | sinh từ schema thật trên đĩa, không viết tay |
| Nghiệm thu | `reports/warehouse_summary.md` | bảng 188 tháng: raw/clean/tỷ lệ loại/cờ |
| Nhật chạy | `reports/run_log.csv` | từng tháng: `done`/`skipped`/`flagged`/`failed` |
| Manifest nguồn | `reports/source_manifest.csv` | URL TLC + schema gốc từng tháng |
| Kiểm tra toàn vẹn | `reports/data_transfer_manifest.csv` | SHA-256 của 2894 file Parquet/`.crc` |

---

## 3. Schema dữ liệu sạch: 25 cột

23 cột vật lý trong file Parquet + 2 cột phân vùng (`year`, `month`) mà Spark tự nối khi đọc.

| Nhóm | Cột |
|---|---|
| Thời gian | `pickup_datetime`, `dropoff_datetime` (timestamp, **giờ địa phương NYC `America/New_York`**, không kèm offset), `pickup_date`, `pickup_hour_ts` (làm tròn xuống đầu giờ), `year`, `month`, `day`, `hour` (0–23), `day_of_week` (**1 = Chủ nhật … 7 = Thứ bảy**, theo chuẩn Spark) |
| Không gian | `pickup_location_id`, `dropoff_location_id` (mã TLC), `pickup_borough`, `pickup_zone`, `dropoff_borough`, `dropoff_zone` (tra Taxi Zone Lookup) |
| Đo lường | `trip_distance` (dặm), `trip_duration_min` (phút), `passenger_count`, `fare_amount`, `total_amount`, `tip_amount`, `tolls_amount` (USD) |
| Cờ chất lượng | `is_short_trip`, `is_distance_missing`, `is_unknown_zone` |

**9 cột TLC đã bỏ có chủ đích:** `VendorID`, `RatecodeID`, `store_and_fwd_flag`, `payment_type`, `extra`, `mta_tax`, `improvement_surcharge`, `congestion_surcharge`, `airport_fee`. Lý do: không ổn định giữa các năm (nhiều tháng 2011–2016 không có cột) và không cần cho dự báo. **Nếu Người 3/4 cần cột nào, báo trước khi chốt — thêm cột nghĩa là chạy lại toàn bộ batch (~7,2 giờ).**

**Ba cờ chất lượng — dùng để lọc, không phải dữ liệu thiếu cần điền:**
- `is_short_trip`: chuyến dưới 1 phút, vẫn giữ dòng.
- `is_distance_missing`: `trip_distance` gốc bằng 0 → đã set null, vẫn giữ dòng.
- `is_unknown_zone`: LocationID không có trong lookup, **hoặc** lookup ghi `Borough='Unknown'` / `Zone='N/A'`. Không gần bằng 0 (đo 18.235 dòng tháng 2024-01, 343.572 dòng tháng 2011-02).

---

## 4. Ngưỡng làm sạch đã áp dụng

`duplicate` được loại trước, sau đó 7 rule còn lại chạy **theo thứ tự** trong `cleaning.rule_order`; mỗi dòng chỉ bị tính ở rule đầu tiên khớp nên các con số không chồng lấp:

| # | Rule | Điều kiện loại |
|---|---|---|
| 1 | `duplicate` | trùng toàn bộ dòng |
| 2 | `trip_distance` | `< 0` hoặc `> 100` dặm (giá trị 0 không bị loại mà set null + cờ) |
| 3 | `trip_duration` | `<= 0` phút, `> 180` phút, hoặc dropoff ≤ pickup |
| 4 | `fare_amount` | **`fare_amount < 0`** |
| 5 | `total_amount` | **`total_amount < 0`** |
| 6 | `average_speed` | `> 100 mph`, chỉ xét chuyến ≥ 1 phút |
| 7 | `pickup_month` | pickup không thuộc đúng tháng đang xử lý |
| 8 | `required_fields` | null ở `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `PULocationID`, `DOLocationID`, `fare_amount` |

Chuyến dưới 1 phút **vẫn giữ**, chỉ gắn `is_short_trip`. Cổng cuối: tổng tỷ lệ loại vượt **8%** thì tháng đó không ghi partition mà bị đánh dấu `flagged` để người xem quyết; **riêng năm 2025 giới hạn là 12%** (lý do ở mục 7).

---

## 5. Số liệu nghiệm thu

| Chỉ tiêu | Giá trị |
|---|---|
| Partition | **188/188**, `year=2011…2026/month=1…12` |
| Dòng trên đĩa | **1.506.791.871** |
| Dòng raw theo manifest | 1.516.766.780 |
| Dòng bị loại | 9.974.909 = **0,658%** (1.516.766.780 raw − 1.506.791.871 clean) |
| Postcheck `invalid` / `missing` | **0 / 0** cho cả 188 tháng |
| Lệch schema giữa partition | **0** (chuẩn đối chiếu: 2024-01) |
| Kích thước | 36.068.314.549 B = 33,59 GiB (1447 file Parquet 33,33 GiB + 1447 `.crc` 0,26 GiB) |
| Kiểm thử | 34/34 xanh |
| Thời gian dựng lại toàn bộ | ~7,2 giờ (đo: ~19,7 giây cho 1 triệu dòng raw, gồm tải + làm sạch + ghi + đọc lại kiểm tra) |

Lệnh tự nghiệm thu sau khi chạy xong:

```powershell
.\.venv\Scripts\python.exe src\warehouse_report.py --from 2011-01 --to 2026-08 --schema-doc docs\schema.md
```

Exit code 0 = đủ 188 partition, không lệch schema, `raw - removed` khớp số dòng trên đĩa.

---

## 6. Việc còn treo — Người 1 chốt giúp

188 partition **đang nằm trên máy Người 2** (`D:\BIGDATA\2-processing\data\clean\trips`), chưa ở hệ thống chung. Ba phương án:

| | Phương án | Ai làm | Chi phí |
|---|---|---|---|
| **A** | Chép cây Parquet lên HDFS rồi **đăng ký** thành bảng Hive, không ghi lại dữ liệu | Người 1 | ~vài phút sau khi copy |
| **B** | Người 2 bật `sink.type: hive` và chạy lại batch ghi thẳng vào bảng | Người 2 | ~7,2 giờ |
| **C** | Mỗi máy tự build từ repo | từng người | ~7,2 giờ + 55 GB ổ mỗi máy |

**A rẻ nhất.** Hai lệnh phía Người 1 sau khi cây `trips` đã ở HDFS:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi.trips USING parquet
LOCATION 'hdfs://<namenode>:8020/user/<user>/warehouse/trips';
MSCK REPAIR TABLE nyc_taxi.trips;   -- nạp 188 partition từ tên thư mục
```

**Lưu ý trung thực:** nhánh `hive` trong `src/writer.py` và `src/pipeline.py` đã cài đặt nhưng **chưa từng chạy thật và không có unit test**; chỉ đường dẫn `local` là đã nghiệm thu 188/188. Nếu chọn B, cần chốt trước loại metastore: **Thrift shared** (`hive-site.xml` / `spark.sql.hive.metastoreURIs`) hay **embedded Derby** — Derby mặc định của Spark chỉ một tiến trình dùng được và không chia sẻ cho cả nhóm.

Câu hỏi cho Người 1: **(1)** đích là bảng `nyc_taxi.trips` hay một đường dẫn HDFS? **(2)** metastore loại nào, có sẵn chưa? **(3)** máy nào/ai chạy lệnh ghi (máy nguồn Windows không có client HDFS, chỉ có `winutils` cho PySpark local)?

Chi tiết kỹ thuật chuyển 33 GB lên HDFS (kèm `dfs.replication`, cách loại file `.crc`, cách đối chiếu SHA-256): `docs/handover.md` mục 5.

---

## 7. Lưu ý riêng cho từng người

**Người 3 (Analytics & ML):**
- ⚠️ **Quan trọng, đọc trước khi pool dữ liệu 2025:** từ 2025 nguồn TLC bắt đầu kê khai cuốc huỷ/refund có `fare_amount < 0`. Rule 4 **loại** các dòng đó, nên tỷ lệ loại của 2025-05→11 là **8,0–11,0%** (đỉnh 2025-11: 11,00%) thay vì ~2,5% như các tháng khác. Vì vượt cổng 8%, năm 2025 được nâng giới hạn lên **12%** để vẫn ghi được partition.
- Tỷ lệ dòng bị loại vì `fare_amount < 0` trên tổng raw từng tháng 2025 (đo từ `run_log.csv`): 01 = 4,15%, 02 = 5,10%, 03 = 5,03%, 04 = 4,70%, 05 = 7,08%, 06 = 6,37%, 07 = 6,34%, 08 = 7,31%, 09 = 5,94%, 10 = 7,25%, **11 = 9,44%**, 12 = 1,12%. Tham chiếu: 2026-08 = 0,42%.
- Hệ quả: **warehouse không còn dòng nào có `fare_amount < 0` hay `total_amount < 0`** — nếu bạn cần phân tích cuốc huỷ thì phải lấy từ raw, không có trong clean. Đây là quyết định làm sạch, không phải dữ liệu thiếu.
- `passenger_count` **tuyệt đối không đưa vào features** dự báo — chỉ dùng thống kê mô tả.
- **2009–2010 (24 tháng) không có trong warehouse**: schema nguồn hai năm đó thiếu `PULocationID`/`DOLocationID`. Cần hai năm đó thì phải bàn riêng.
- Đọc theo phân vùng rẻ hơn nhiều: `spark.read.parquet(path).filter("year=2024 and month=1")` chỉ quét đúng partition đó.

**Người 4 (BI & Integration):**
- `day_of_week` đánh số **1 = Chủ nhật** (chuẩn Spark), không phải 1 = Thứ hai. Nhớ quy đổi khi vẽ trục tuần.
- Thời gian là **giờ địa phương NYC, không có info trong file** về offset; hai ngày chuyển sang/giờ mùa hè mỗi năm có những giờ trùng hoặc thiếu.
- `pickup_hour_ts` đã có sẵn để aggregate theo giờ; `hour` là 0–23.
- Power BI không đọc Parquet phân vùng một cách tự nhiên — nên qua một lớp Spark SQL/DuckDB tổng hợp rồi mới nối. Lớp đó thuộc phần bạn; tôi cam kết tới Parquet + schema ổn định.

**Người 1 (Data Warehouse):**
- Output là **Parquet Snappy, Hive-style `year=/month=`**, 25 cột như mục 3, đồng nhất mọi partition — bạn có thể `CREATE TABLE ... USING parquet LOCATION` mà không cần tôi ghi lại.
- Muốn pipeline tự ghi vào bảng Hive thì đổi `sink.type: hive` + `sink.hive_table` trong `config.yaml` (điều kiện: xem mục 6).

---

## 8. Môi trường để chạy lại từ đầu

- Python 3.9+ (đã kiểm chứng 3.13), `python -m venv .venv` rồi `pip install -r requirements.txt` — PySpark 4.0.1 có trong đó, không cần thêm gì.
- **JDK 17** (bắt buộc; JDK 21/25 không chạy được với Spark ở cấu hình này).
- Windows: `winutils.exe` + `hadoop.dll` (Hadoop 3.4.0) và `HADOOP_HOME`. Linux: không cần.
- ~55 GB ổ trống (20 GB raw + 34 GB clean) nếu build toàn bộ.
- Sửa 6 đường dẫn tuyệt đối trong `config.yaml` (`paths.*`, `hadoop_home`).

```powershell
Set-Location <thu-muc-repo>
.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force -KeepAwake
```

Gián đoạn thì chạy lại cùng lệnh bỏ `-Force`: tháng đã xong được postcheck rồi bỏ qua, tháng lỗi chỉ bị đánh dấu `failed` chứ không làm dừng cả batch.

---

## 9. Đầu mối

Chủ sở hữu: **Người 2 — Xử lý dữ liệu**. Mọi câu hỏi về ngưỡng làm sạch, danh sách cột đầu ra, phạm vi tháng: hỏi tôi trước khi tự đổi, vì đổi một cột hoặc một ngưỡng nghĩa là chạy lại 188 tháng và `docs/schema.md` phải sinh lại.
