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
Set-Location <repo>/2-processing
.\scripts\run_batch.ps1 -From 2011-01 -To 2026-08 -Force -KeepAwake
```

Runner tự tải thiếu → làm sạch → ghi Parquet → postcheck, theo thứ tự tháng mới trước, **không dừng cả batch khi một tháng lỗi** (ghi `status=failed` vào `reports/run_log.csv`). Gián đoạn thì chạy lại cùng lệnh bỏ `-Force`: tháng đã xong sẽ được postcheck và bỏ qua.

Chi phí đã đo trên máy nguồn: **~19,7 s cho mỗi 1 triệu dòng raw** (gồm tải + làm sạch + ghi + đọc lại kiểm tra), **toàn bộ 188 tháng mất ~7,2 giờ**. Chạy xong thì nghiệm thu bằng một lệnh:

```powershell
.\.venv\Scripts\python.exe src\warehouse_report.py --from 2011-01 --to 2026-08 --schema-doc docs\schema.md
```

Exit code 0 nghĩa là 188/188 partition, không lệch schema, `raw - removed` khớp số dòng trên đĩa.

## 5. Bàn giao dữ liệu 34 GB (kênh máy chủ / HDFS của nhóm)

**Máy nguồn không có `hdfs` CLI.** `HADOOP_HOME` chỉ gồm `winutils.exe` + `hadoop.dll` phục vụ PySpark local; `winutils` không liên quan gì tới khối dữ liệu HDFS. Thiếu CLI **không** có nghĩa không đẩy được vào cluster: hai client nói chuyện trực tiếp đều đã có sẵn trên máy này, không tải thêm gì.

| Cách | Client có sẵn | Cổng phải tới được từ máy nguồn | Giữ nguyên tên file / số byte? |
|---|---|---|---|
| 1. Spark HDFS native | PySpark 4.0.1 bundle `hadoop-client-api/runtime` (chứa `DistributedFileSystem`) | `8020` NameNode RPC + `9866` DataNode block transfer | **Không** — Spark ghi lại, đặt tên file khác, số byte thay đổi |
| 2. WebHDFS REST | `requests` (dependency của dự án, `requirements.txt`), `op=CREATE` rồi theo redirect 307 | `9870` NameNode UI + `9864` DataNode HTTP | **Có** — ghi từng byte đúng tên file gốc |

Chọn **cách 2**, vì chỉ nó giữ cho `reports/data_transfer_manifest.csv` (sha256 từng file) và các con số ở Bước 3 còn nghĩa. WebHDFS PUT không có bản staging, nên HDFS chỉ cần đúng 33,33 GiB.

**Bẫy Docker — phải chốt trước khi chuyển byte nào.** Khi client hỏi block nằm ở DataNode nào, NameNode trả về **địa chỉ DataNode tự quảng bá**; cluster trong Docker thường trả tên container mà máy ngoài không resolve được → lỗi `Bad connection to BlockPool` hoặc timeout, dù `8020` telnet thông. Redirect 307 của WebHDFS cũng chứa đúng địa chỉ đó (`dfs.datanode.hostname`), nên **cả hai cách trên đều dính**, không có đường nào miễn nhiễm. Fix phía cluster: đặt `dfs.datanode.hostname = <IP LAN của laptop>` và publish cổng `9864` (hoặc `9866` nếu đi Spark). Kiểm chứng bằng **một file 24 MB**, không phải bằng 33 GiB.

**Dung lượng**: Docker Desktop trên Windows đặt đĩa WSL2 (`.vhdx`) ở ổ do người dùng cấu hình — nếu nó đang ở `C:` thì mỗi GiB ghi vào HDFS là một GiB ghi lên `C:`, và vhdx **không tự co lại**. Phải xác định vhdx nằm ở ổ nào và còn trống bao nhiêu **GiB** (Windows hiển thị GiB, không phải GB) trước khi bắt đầu.

**Bước 1 — loại sidecar `.crc`**: thư mục trên đĩa có **2894** file; 1447 file `.crc` là checksum kiểu Hadoop-local, HDFS tự tính checksum riêng nên mang lên chỉ thêm object rác. Script chuyển bỏ theo hậu tố, không chép staging. Con số đúng để đối chiếu: 1447 file Parquet, 35.788.700.805 B = **33,33 GiB** (toàn bộ kể cả `.crc` là 36.068.314.549 B = 33,59 GiB).

**Bước 2 — hệ số nhân bản**: cluster có **một** DataNode nên đặt `dfs.replication=1`. Mặc định là **3** → block sẽ vĩnh viễn ở trạng thái under-replicated và cảnh báo ồn; với 1 DataNode thì 2 hay 3 cũng không tạo thêm bản sao nào.

**Bước 3 — xác minh** số học khớp rồi mới báo cho nhóm:

```bash
hdfs dfs -count  /data/nyc_taxi/clean/trips
# in ra "<số thư mục con> <số file> <tổng byte> <đường dẫn>"
# đã bỏ .crc thì phải ra đúng:   204  1447  35788700805
# 204 = 16 thư mục year= + 188 thư mục month=
```

Nếu cluster giữ nguyên cả `.crc` thì con số phải là `204 2894 36068314549`. Đối chiếu từng file thì dùng `reports/data_transfer_manifest.csv` — nó có `relpath`, `size_bytes`, `sha256` của từng file.

| Năm | GiB | Năm | GiB | Năm | GiB | Năm | GiB |
|---|---|---|---|---|---|---|---|
| 2011 | 3,7 | 2015 | 3,4 | 2019 | 2,1 | 2023 | 0,96 |
| 2012 | 3,6 | 2016 | 3,1 | 2020 | 0,60 | 2024 | 1,01 |
| 2013 | 3,5 | 2017 | 2,7 | 2021 | 0,77 | 2025 | 1,20 |
| 2014 | 3,6 | 2018 | 2,5 | 2022 | 0,99 | 2026 | 0,73 |

**Con đường rẻ hơn nếu cluster có internet và ≥ 55 GB trống:** đừng chuyển byte nào — clone repo này lên node, sửa 6 đường dẫn trong `config.yaml`, bỏ `hadoop_home` (Linux không cần winutils), rồi `python src/batch_runner.py --from 2011-01 --to 2026-08 --force`. Pipeline chỉ phụ thuộc Python 3.9+, JDK 17 và PySpark 4.0.1; đã kiểm chứng toàn bộ 188 tháng trên Windows, chi phí ~7,2 giờ.

## 6. Bốn điều phải nói thẳng với nhóm

1. **Người 1 (Data Warehouse):** 188 partition đã thành bảng Hive **trên máy nguồn** (`src/register_warehouse.py` → `nyc_taxi.trips`, external table trỏ vào `data/clean/trips`, nghiệm thu 1.506.791.871 dòng). Metastore ở đó là Derby nhúng, **chỉ một tiến trình** truy cập nên không phục vụ cả nhóm; bản dùng chung vẫn phải dựng trên HDFS theo mục 7, hoặc mỗi người `spark.read.parquet("<duong-dan>/data/clean/trips")`.
2. **24 tháng 2009–2010 bị loại có chủ đích**: schema nguồn hai năm đó không có `PULocationID`/`DOLocationID`, mà thiết kế warehouse bắt buộc hai cột này. Ai cần hai năm đó phải quyết định riêng (geocode ngược hoặc bỏ cột zone).
3. **Người 3 (Analytics & ML) — quan trọng:** từ 2025-05 đến 2025-11 có cụm cuốc huỷ/refund với `fare_amount < 0` chiếm **6,3–9,4%** dòng, nên ngưỡng loại năm 2025 được nâng lên **12%** (`cleaning.max_drop_rate_by_year`) trong khi các năm khác vẫn 8%. Nếu pool 2025 với năm khác mà không biết điều này, số liệu lệch. Tháng 2025-12 trở lại 1,12% và 2026-08 là 0,42% — đây là thay đổi cách kê khai của nguồn, không phải lỗi tải.
4. **Người 3 / Người 4:** `passenger_count` chỉ dùng thống kê mô tả, **tuyệt đối không đưa vào feature** dự báo. Ba cờ chất lượng `is_short_trip`, `is_distance_missing`, `is_unknown_zone` nên được dùng làm bộ lọc khi dựng feature, không phải dữ liệu thiếu cần điền.

## 7. Đích ghi — đã chốt về thiết kế, còn chờ kiểm chứng đường truyền

Toàn bộ 188 partition **vẫn đang nằm trên máy nguồn** ở `data/clean/trips`, chưa có byte nào ở hệ thống chung. Thông số đã thống nhất với Người 1:

| Hạng mục | Giá trị chốt |
|---|---|
| Cluster | Hadoop 3.4.1 chạy Docker trên laptop Windows của Người 1 (1 NameNode + 1 DataNode) |
| Đường dẫn Clean | `/data/nyc_taxi/clean/trips` (theo quy ước Raw/Clean của nhóm, không dùng `/user/<user>/...`) |
| Bảng Hive | `nyc_taxi.trips`, **external table**, location `hdfs://<IP-LAN>:8020/data/nyc_taxi/clean/trips` |
| Metastore | PostgreSQL chứa metadata, Hive Metastore phục vụ Thrift `9083` |
| `dfs.replication` | `1` |
| Cách chuyển | WebHDFS PUT từng file từ máy nguồn, bỏ `.crc` (mục 5) |
| Kerberos / HiveServer2 | **chưa kết luận**; phương án đọc `spark.read.parquet("hdfs://...")` trực tiếp thì không cần HiveServer2 |

Chưa kiểm chứng: IP LAN, firewall, **địa chỉ DataNode tự quảng bá**, cổng WebHDFS, dung lượng còn trống (mục 5 có lý do từng cái). Chưa chuyển gì trước khi test một file 24 MB thông.

Hai lệnh mà Người 1 cần, sau khi cây `trips` đã ở HDFS và `-count` khớp:

```sql
CREATE TABLE IF NOT EXISTS nyc_taxi.trips
USING parquet
LOCATION 'hdfs://<IP-LAN>:8020/data/nyc_taxi/clean/trips';

MSCK REPAIR TABLE nyc_taxi.trips;   -- nạp 188 partition year=/month= từ tên thư mục
```

Ngoại hình lệnh này dễ gây hiểu hai chỗ: `USING parquet` + `LOCATION` trong Spark SQL **đã là external table** (DROP bảng không xoá dữ liệu), không cần từ khoá `EXTERNAL` của Hive; và external table chỉ **đăng ký metadata trỏ vào Parquet có sẵn** — nó không tự sinh hay tự sửa dữ liệu, không chạy `MSCK REPAIR` thì query thấy bảng rỗng.

Nhánh `sink.type: hive` trong `src/writer.py` dùng `saveAsTable`, tức tạo **managed table** trong Hive warehouse chứ không phải external ở `/data/...`; nó **chưa từng chạy thật và không có unit test**. Với đích đã chốt ở trên thì không cần chạy lại pipeline: ghi file byte-nguyên rồi đăng ký bảng rẻ hơn và kiểm chứng được.

## 8. Liên hệ

Chủ sở hữu slice: Người 2 (data processing). Mọi câu hỏi về ngưỡng làm sạch, cột đầu ra hay phạm vi tháng đều do Người 2 quyết; thay đổi số dòng đã loại phải chạy lại `warehouse_report.py` để cập nhật `docs/schema.md`.

## 9. Cập nhật 2026-11-03 — kho chung đã chạy thật, mục 5 và 7 có chỗ sai

Mảnh 1 đã được dựng và nghiệm thu trên chính máy nguồn (Người 2 làm thay Người 1 theo thỏa thuận nhóm). Cluster: Hadoop 3.4.1 + Hive Metastore trong Docker Compose, toàn bộ tài liệu ở [../../1-warehouse/README.md](../../1-warehouse/README.md). Bốn chỗ mục 5 và 7 đoán sai, giữ nguyên chữ ở trên để ai đọc sau biết cái nào không còn đúng:

| Mục cũ | Nói thế | Thực tế đã chạy |
|---|---|---|
| 5 | Chọn WebHDFS PUT vì chỉ nó giữ nguyên byte | **Sai lựa chọn.** Client HDFS native giữ nguyên byte y như WebHDFS và nhanh gấp 5,5 lần: `HdfsCopy` 4 luồng đo **66,3 MiB/s** (33,1 GiB / 499 s), WebHDFS PUT chỉ **12,0 MiB/s**. 1447 file đã lên HDFS **khớp từng byte** với đĩa. |
| 5 | Bẫy tất yếu: `dfs.datanode.hostname` phải là IP LAN | **Sai một phần.** Đặt IP thô thì client Windows coi DataNode là "local node" và loại khỏi pipeline ngay (`could only be written to 0 of the 1 minReplication nodes`). Phải dùng **alias** trỏ về đúng IP: `192-168-<a>-<b>.sslip.io` (DNS wildcard, resolve được cả trong container lẫn ngoài Windows, không phải sửa `hosts`). |
| 7 | Metastore: PostgreSQL | **Derby nhúng trong container** — đủ cho một người ghi, nhưng chỉ **một JVM** mở được nó (`hive -e`/`beeline` song song sẽ fail). Mọi metadata phải đi qua Thrift 9083 tức qua Spark. Nếu cả nhóm query đồng thời thì vẫn phải đổi sang PostgreSQL như mục 7 ghi. |
| 7 | Bảng tên `nyc_taxi.trips` | **`default.clean_nyc_taxi`** (database `default` để `spark.table("clean_nyc_taxi")` chạy ngay không cần `USE`). External table, location `hdfs://<IP>:8020/data/nyc_taxi/clean/trips`, 25 cột, **188/188 partition**, `count(*) = 1.506.791.871` đúng mốc bàn giao. |

Số đã kiểm chứng bằng truy vấn thật từ Spark chạy trên Windows (không phải suy từ log chép file):

- `1-warehouse/src/verify_access.py` → **PASS**: 188 partition, toàn bảng 1.506.791.871, pruning đúng (`year=2026/month=8` chỉ đọc 1 thư mục), 2011-01 = 13.441.123, 2020-03 = 2.988.041, 2026-08 = 3.277.598.
- Raw của TLC cũng đã lên HDFS: **`default.raw_nyc_taxi`** — 189 file / **20.986.383.705 B** (19,55 GiB), khớp từng byte với `2-processing/data/raw`; **188/188 partition**; bảng có **19 cột dữ liệu + year/month** sinh từ `source_manifest.csv` (không mở 188 file). Một scan thật qua đủ 188 file (587 s) cho **1.516.766.780 dòng** = đúng tổng footer, và **5.026.251 dòng `fare_amount < 0`** — đúng thứ Người 3 cần mà clean đã loại.
- **Hai cột `RatecodeID` và `passenger_count` không có trong bảng raw.** Lý do đo được chứ không phải suy: hai cột này là INT64 ở 133 tháng rồi DOUBLE ở 55 tháng, mà Spark từ chối đọc file INT64 bằng khai báo `double` (`FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH`, SQLSTATE KD001 — `1-warehouse/tools/probe_type_promotion.py`). `3-analytics` không dùng cả hai (`config.py`/`model.py` loại `passenger_count` có chủ đích).
- View `clean_nyc_taxi_tlc` không chỉ đổi tên mà còn **cast lại hai cột thời gian về `TIMESTAMP`**, vì bảng sạch mang kiểu `TIMESTAMP_NTZ` do Parquet lưu micros; `analysis.py` của Người 3 làm `CAST(tpep_dropoff_datetime AS BIGINT)` và Spark 4 từ chối cast từ `TIMESTAMP_NTZ` (`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`). Sau khi view cast: công thức của họ cho 17,1653 phút ở partition 2026-08, **khớp tuyệt đối** (lệch 0,000000) với cột `trip_duration_min` đã lưu.

### Điều Người 3 phải biết (mới, phát hiện khi đọc `3-analytics/`)

`data_loader.py` của Người 3 kiểm tra các cột **tên gốc TLC** (`PULocationID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `fare_amount`), còn `clean_nyc_taxi` dùng tên đã chuẩn hoá → đọc trực tiếp sẽ gặp `ValueError: Table 'clean_nyc_taxi' is missing required columns`. Không sửa code của Người 3; mảnh 1 dựng **view** `default.clean_nyc_taxi_tlc` (lớp tên + khai lại kiểu thời gian, không chiếm dung lượng, không đụng dữ liệu). Người 3 chỉ cần thêm tên view vào tham số đã có sẵn trong `main.py`:

```powershell
spark-submit --conf spark.hadoop.hive.metastore.uris=thrift://<IP>:9083 `
  --conf spark.hadoop.fs.defaultFS=hdfs://<IP>:8020 `
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
  --conf spark.sql.catalogImplementation=hive `
  main.py --clean-table clean_nyc_taxi_tlc --raw-table raw_nyc_taxi
```

`--output-base hdfs:///user/hive/warehouse/nyc_taxi_analytics` mặc định dùng được luôn vì `fs.defaultFS` đã trỏ vào cluster. Hai lưu ý trung thực: (a) `tpep_*` trong raw là mốc thời gian **UTC** theo cách TLC ghi file, nên `year()/hour()` suy từ cột có thể lệch vài giờ so với partition `year=/month=` — số của clean (đã chuẩn sang giờ địa phương NYC) mới là số để báo cáo; (b) view chỉ đổi tên và khai lại kiểu cho đúng những gì file Parquet đang mang, **không** tính lại đơn vị hay giá trị nào.

Mảnh 1 đã chạy **chính code của Người 3** thay họ trước khi họ bật máy: `1-warehouse/src/verify_person3_pipeline.py` import nguyên `analysis.py`, `data_loader.py`, `evaluation.py`, `feature_engineering.py`, `model.py`, `writer.py` trong `3-analytics/` (chỉ đọc, không sửa một dòng nào) và gọi đúng thứ tự như `main.py` trên partition 2024-01. **PASS 6/6 nhóm hàm trong 521 s**: `load_clean_trips` qua view → 2.924.072 dòng; 4 câu SQL → 5.296 dòng analytics / 24 dòng theo giờ / 168 ô heatmap; `load_raw_trips` + cancelled → 37.448 cuốc huỷ của 2024-01; feature + chronological split → train 62.528 / test 14.884; GBT + metrics; `writer.py` ghi 2 Parquet + 1 CSV lên HDFS (`/tmp/nyc_taxi_analytics_smoke`).

Hai điều Người 3 nên đọc trước khi tốn một ngày:
1. **`CAST(tpep_dropoff_datetime AS BIGINT)` trong `analysis.py` không chạy được trực tiếp trên `clean_nyc_taxi`** (`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` vì cột là `TIMESTAMP_NTZ`). View `clean_nyc_taxi_tlc` đã cast sang `TIMESTAMP`, và số nó cho khớp tuyệt đối với cột `trip_duration_min` (17,1653 phút ở 2026-08, lệch 0,000000). Qua view thì không phải sửa code.
2. **Với cấu hình smoke một tháng + `maxIter=10`, GBT tệ hơn baseline**: RMSE 48,00 so với 29,48, MAPE 923% so với 89%. Đây là hệ quả của việc chỉ huấn luyện trên `2024-01` (77.412 dòng zone×ngày), không phải tín hiệu model sai — chạy cả năm rồi mới kết luận.

Còn một va chạm môi trường: `.venv` của mảnh 2 không có `numpy` nên `pyspark.ml` (tức `evaluation.py`) không import được; máy này phải dùng Python 3.11 hệ thống (pyspark 4.0.1 + numpy 2.4.6 + pyarrow 24.0.0). Env của Người 3 chỉ cần có numpy là chạy bình thường.

### Việc còn treo sau mục 9

- `raw_nyc_taxi` đã đối chiếu **tổng** 1.516.766.780 = đúng tổng footer 188 tháng trong `source_manifest.csv`, và một scan thật đã đọc trọn cả 188 partition; **chưa** so số dòng *từng tháng* với manifest.
- Docker Desktop trên máy này hai lần tự tắt engine (containers `Exited (255)`) — cách hồi phục ghi ở README mảnh 1, mục 7.
- Máy đổi mạng Wi-Fi làm IP host thay đổi (đã quan sát 192.168.100.19 → 192.168.36.64 → về cũ): chạy `1-warehouse/tools/Set-Host.ps1` rồi đăng ký lại bảng, vì Derby ghi chết location của database.

