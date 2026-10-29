# Bàn giao phần xử lý dữ liệu (Người 2)

Phạm vi: pipeline tải – kiểm tra – làm sạch – ghi warehouse cho **NYC TLC Yellow Taxi, 188 tháng từ 2011-01 đến 2026-08**.

## 1. Trạng thái nghiệm thu

| Chỉ tiêu | Giá trị | Bằng chứng |
|---|---|---|
| Partition trên đĩa | **188/188**, `year=/month=` | `reports/warehouse_summary.md` |
| Lệch schema giữa các partition | **0** | `docs/schema.md` (sinh từ schema thật) |
| Dòng trên đĩa | **1.506.791.871** | `reports/warehouse_summary.md` |
| Dòng bị loại | 9.974.909 = **0,658%** trên 1.516.766.780 dòng raw | `reports/run_log.csv` |
| Postcheck (`invalid`, `missing`) | **0 / 0** cho cả 188 tháng | `reports/run_log.csv`, `src/output_validation.py` |
| Kiểm thử | **34/34** xanh | `tests/` |
| Kích thước output | 36.068.314.549 B = 2894 file (1447 Parquet 35.788.700.805 B + 1447 `.crc` 279.613.744 B) | `reports/data_transfer_manifest.csv` |
| Nơi 34 GB đang nằm | máy nguồn `D:\BIGDATA\2-processing\data\clean\trips`, **chưa** ở hệ thống chung của nhóm | mục 5 và 7 |

Dữ liệu raw không nằm trong repo: **54 GB** (20 GB raw + 34 GB clean), trong raw có **101 file > 100 MB** — GitHub chặn cứng ở 100 MB/file và khuyến nghị repo < 1 GB. Đường lấy dữ liệu sạch: xem mục 4.

## 2. Những gì có trong repo

```
src/            pipeline: download.py, quality_check.py, clean.py, writer.py,
                output_validation.py, pipeline.py, batch_runner.py, warehouse_report.py
tests/          34 test (pytest), fixture tự dựng DataFrame, không cần mạng
config.yaml     đường dẫn, ngưỡng làm sạch, schema baseline, sink
scripts/        run_batch.ps1 — wrapper PowerShell cho batch dài
docs/           schema.md (23 cột vật lý + 2 cột phân vùng), handover.md
reports/        warehouse_summary.md, run_log.csv, source_manifest.csv
requirements.txt
```

Không lên GitHub (và không nên lên): `.venv/`, `data/`, `.tmp/`, transcript chạy batch (~900 KB), `reports/quality_summary.md` và `reports/quality_2024-01.md` là báo cáo mốc cũ, số đã bị `warehouse_summary.md` thay thế.

## 3. Bộ máy người nhận cần có

- **Python 3.9+** (môi trường đã kiểm chứng là 3.13) rồi tạo `.venv` riêng: `python -m venv .venv` và `pip install -r requirements.txt`. **PySpark 4.0.1 có trong `requirements.txt`, không cần cài thêm gì khác.**
- **JDK 17** — bắt buộc. JDK 21 trở lên (kể cả Java 25) không dùng được với Spark ở đây.
- **Hadoop binary cho Windows**: `winutils.exe` + `hadoop.dll` (Hadoop 3.4.0) và `HADOOP_HOME` trỏ tới thư mục `bin` chứa chúng.
- **~55 GB ổ trống** nếu tái tạo toàn bộ (20 GB raw + 34 GB clean), chưa tính vùng tạm Spark.

Đường dẫn máy-cụ-thể phải sửa: `config.yaml` có 6 mục `paths:`/`hadoop_home` dạng tuyệt đối (`D:/BIGDATA/2-processing/...`, `D:/tmp/spark`, `D:/hadoop`), và `JAVA_HOME` trong `scripts/run_batch.ps1` (đã tham số hoá: `-JavaHome`, `-TempDir`). `run_batch.ps1` tự suy ra thư mục dự án từ `$PSScriptRoot`, nên clone về đâu cũng chạy được.

## 4. Tái tạo warehouse từ số không

```powershell
Set-Location <thu-muc-repo>
.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force -KeepAwake
```

Runner tự tải thiếu → làm sạch → ghi Parquet → postcheck, theo thứ tự tháng mới trước, **không dừng cả batch khi một tháng lỗi** (ghi `status=failed` vào `reports/run_log.csv`). Gián đoạn thì chạy lại cùng lệnh bỏ `-Force`: tháng đã xong sẽ được postcheck và bỏ qua.

Chi phí đã đo trên máy nguồn: **~19,7 s cho mỗi 1 triệu dòng raw** (gồm tải + làm sạch + ghi + đọc lại kiểm tra), **toàn bộ 188 tháng mất ~7,2 giờ**. Chạy xong thì nghiệm thu bằng một lệnh:

```powershell
.\.venv\Scripts\python.exe src\warehouse_report.py --from 2011-01 --to 2026-08 --schema-doc docs\schema.md
```

Exit code 0 nghĩa là 188/188 partition, không lệch schema, `raw - removed` khớp số dòng trên đĩa.

## 5. Bàn giao dữ liệu 34 GB (kênh máy chủ / HDFS của nhóm)

**Không `hdfs dfs -put` được từ máy Windows nguồn.** Thư mục `HADOOP_HOME` ở đây chỉ có `winutils.exe` + `hadoop.dll` phục vụ PySpark local, không có `hadoop.cmd`/`hdfs.cmd`/`etc/hadoop/core-site.xml`; muốn dùng trực tiếp phải tải trọn bản phân phối Hadoop. Vì vậy chiều chuyển luôn là: staging → chép lên node của cluster → `put` từ chính node đó.

**Bước 1 — staging, bỏ sidecar `.crc`** (chạy trên máy nguồn, cần ~34 GB ổ trống):

```powershell
robocopy "D:\BIGDATA\2-processing\data\clean\trips" "D:\staging\trips" /E /XF *.crc
```

Được 1447 file Parquet, 35.788.700.805 B = **33,33 GiB** thay vì 2894 file / 33,59 GiB. `.crc` là checksum sidecar kiểu Hadoop-local, HDFS tự tính checksum của nó nên mang lên chỉ tổ thêm 1447 object rác.

**Bước 2 — chép lên node**: `scp -r`, `rsync -a --info=progress2`, hoặc ổ cứng nếu cluster không cùng mạng. Trên LAN 1 Gbps, 33 GB mất cỡ 5–8 phút; qua WiFi thì chậm hơn nhiều.

**Bước 3 — đưa lên HDFS từ node** (chú ý hệ số nhân bản):

```bash
hdfs dfs -mkdir -p /user/<user>/warehouse
hdfs dfs -put -D dfs.replication=2 /path/staging/trips /user/<user>/warehouse/trips
```

Replication mặc định là **3**, tức 33 GB thành ~100 GB chiếm chỗ trên cluster; đặt 1–2 cho bài tập nhóm.

**Bước 4 — xác minh** số học khớp bảng dưới đây rồi mới báo cho nhóm:

```bash
hdfs dfs -count  /user/<user>/warehouse/trips
# in ra "<số thư mục> <số file> <tổng byte> <đường dẫn>"
# sau bước 1 (đã bỏ .crc) phải ra đúng:   204  1447  35788700805
# 204 = 16 thư mục year= + 188 thư mục month=
```

Nếu cluster giữ nguyên cả `.crc` (không chạy bước 1) thì con số phải là `204 2894 36068314549`. Đối chiếu từng file thì dùng `reports/data_transfer_manifest.csv` — nó có `relpath`, `size_bytes`, `sha256` của từng file.

| Năm | GiB | Năm | GiB | Năm | GiB | Năm | GiB |
|---|---|---|---|---|---|---|---|
| 2011 | 3,7 | 2015 | 3,4 | 2019 | 2,1 | 2023 | 0,96 |
| 2012 | 3,6 | 2016 | 3,1 | 2020 | 0,60 | 2024 | 1,01 |
| 2013 | 3,5 | 2017 | 2,7 | 2021 | 0,77 | 2025 | 1,20 |
| 2014 | 3,6 | 2018 | 2,5 | 2022 | 0,99 | 2026 | 0,73 |

**Con đường rẻ hơn nếu cluster có internet và ≥ 55 GB trống:** đừng chuyển byte nào — clone repo này lên node, sửa 6 đường dẫn trong `config.yaml`, bỏ `hadoop_home` (Linux không cần winutils), rồi `python src/batch_runner.py --from 2011-01 --to 2026-08 --force`. Pipeline chỉ phụ thuộc Python 3.9+, JDK 17 và PySpark 4.0.1; đã kiểm chứng toàn bộ 188 tháng trên Windows, chi phí ~7,2 giờ.

## 6. Bốn điều phải nói thẳng với nhóm

1. **Người 1 (Data Warehouse):** output là **Parquet Hive-style trên đĩa local**, chưa phải Hive Metastore/HDFS. Hoặc trỏ thẳng `spark.read.parquet("<duong-dan>/data/clean/trips")`, hoặc đổi `sink.type: hive` (đã hỗ trợ sẵn trong `config.yaml` + `src/writer.py`) để pipeline tự `saveAsTable` — khi đó schema và phân vùng giữ nguyên.
2. **24 tháng 2009–2010 bị loại có chủ đích**: schema nguồn hai năm đó không có `PULocationID`/`DOLocationID`, mà thiết kế warehouse bắt buộc hai cột này. Ai cần hai năm đó phải quyết định riêng (geocode ngược hoặc bỏ cột zone).
3. **Người 3 (Analytics & ML) — quan trọng:** từ 2025-05 đến 2025-11 có cụm cuốc huỷ/refund với `fare_amount < 0` chiếm **6,3–9,4%** dòng, nên ngưỡng loại năm 2025 được nâng lên **12%** (`cleaning.max_drop_rate_by_year`) trong khi các năm khác vẫn 8%. Nếu pool 2025 với năm khác mà không biết điều này, số liệu lệch. Tháng 2025-12 trở lại 1,12% và 2026-08 là 0,42% — đây là thay đổi cách kê khai của nguồn, không phải lỗi tải.
4. **Người 3 / Người 4:** `passenger_count` chỉ dùng thống kê mô tả, **tuyệt đối không đưa vào feature** dự báo. Ba cờ chất lượng `is_short_trip`, `is_distance_missing`, `is_unknown_zone` nên được dùng làm bộ lọc khi dựng feature, không phải dữ liệu thiếu cần điền.

## 7. Phần còn treo — cần Người 1 xác nhận rồi mới ghi

Toàn bộ 188 partition **đang nằm trên máy nguồn** ở `data/clean/trips` và chưa ở đâu trong hệ thống chung của nhóm. Chốt đích ghi là việc của Người 1, có ba phương án:

| Phương án | Ai làm | Chi phí | Ghi chú |
|---|---|---|---|
| A. Đăng ký cây Parquet có sẵn thành bảng Hive | Người 1 | gần 0, không ghi lại byte nào | Khuyến nghị. Chép 33 GB lên HDFS theo mục 5 rồi chạy SQL ở dưới. |
| B. Tôi đổi `sink.type: hive` rồi chạy lại batch | Người 2 | ~7,2 giờ ghi lại 188 tháng | Nhánh này **chưa từng chạy thật và không có unit test** (xem `src/writer.py`, `src/pipeline.py:59`); chỉ làm sau khi Người 1 xác nhận metastore. |
| C. Mỗi người tự build từ repo | từng người | ~7,2 giờ + 55 GB ổ mỗi máy | Không phải chuyển dữ liệu, nhưng máy nào cũng phải có JDK 17 và tải lại từ TLC. |

Hai lệnh mà Người 1 cần, sau khi cây `trips` đã ở HDFS:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi.trips
USING parquet
LOCATION 'hdfs://<namenode>:8020/user/<user>/warehouse/trips';

MSCK REPAIR TABLE nyc_taxi.trips;   -- nạp 188 partition year=/month= từ tên thư mục
```

Ba câu hỏi phải có trả lời trước khi ghi:

1. **Đích là gì**: bảng Hive `nyc_taxi.trips` (tên đang khai trong `config.yaml`) hay chỉ một đường dẫn HDFS mà mọi người `spark.read.parquet()`?
2. **Metastore loại nào**: Thrift shared (`spark.sql.hive.metastoreURIs` / `hive-site.xml`) hay embedded Derby? Derby mặc định của Spark chỉ một tiến trình dùng được và **không chia sẻ cho cả nhóm**, nên nếu Người 1 chọn hướng B thì phải có metastore thật.
3. **Ai chạy lệnh ghi và chạy ở máy nào**: nếu là B thì cần một máy Linux có client HDFS, vì máy nguồn hiện không có (`D:\hadoop` chỉ có `winutils.exe` + `hadoop.dll` cho PySpark local).

## 8. Liên hệ

Chủ sở hữu slice: Người 2 (data processing). Mọi câu hỏi về ngưỡng làm sạch, cột đầu ra hay phạm vi tháng đều do Người 2 quyết; thay đổi số dòng đã loại phải chạy lại `warehouse_report.py` để cập nhật `docs/schema.md`.
