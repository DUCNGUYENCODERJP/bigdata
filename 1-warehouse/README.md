# Slice 1 — Data Warehouse (Hadoop + Hive trên Docker)

Mảnh này dựng **kho dữ liệu thật** cho đề tài: HDFS chứa Parquet đã làm sạch của Người 2,
Hive Metastore đăng ký bảng phân vùng, để Người 3 và Người 4 chỉ cần 2 dòng cấu hình là
query được `clean_nyc_taxi`. Toàn bộ chạy bằng Docker Compose trên máy cá nhân, dữ liệu
nằm trong `1-warehouse/data/` (ổ D:), không ghi lên C:.

Người thực hiện: Người 2 (làm thay phần Người 1 theo thỏa thuận nhóm), hoàn thành 2026-11-03.

## 1. Trạng thái nghiệm thu

| Chỉ tiêu | Giá trị | Cách đo |
|---|---|---|
| Cluster | 5 container: `nyc-namenode`, `nyc-datanode`, `nyc-metastore`, `nyc-hiveserver2`, `nyc-client` | `docker compose ps` |
| Dữ liệu trên HDFS | **1447 file / 35.788.700.805 B / 205 thư mục** tại `/data/nyc_taxi/clean/trips` | `hdfs dfs -count -v`, `HdfsCopy` tự đối chiếu |
| Khớp với đĩa local | **khớp từng byte** với `2-processing/data/clean/trips` | `[verify]` trong log copy, `PASS` |
| Bảng Hive | `default.clean_nyc_taxi`, 25 cột, external table trên HDFS | `src/register_hive_tables.py` |
| Partition đăng ký | **188/188** (`year=/month=`) | `SHOW PARTITIONS` |
| Dòng toàn bảng qua HDFS | **1.506.791.871** = đúng mốc bàn giao mảnh 2 | `spark.table(...).count()` |
| Dữ liệu raw trên HDFS | **189 file / 20.986.383.705 B** (19,55 GiB) tại `/data/nyc_taxi/raw/trips`, khớp từng byte với `2-processing/data/raw` | `HdfsCopy ... tlc` + `[verify] PASS` |
| Bảng raw | `default.raw_nyc_taxi`, **19 cột dữ liệu + year/month**, 188 partition, schema union do Spark đọc được mọi tháng | `src/register_raw_nyc_taxi.py` |
| Dòng toàn bảng raw | **1.516.766.780** = đúng tổng footer 188 tháng trong `source_manifest.csv` | một scan thật qua cả 188 file (587 s) |
| Cuốc huỷ chỉ có ở raw | **5.026.251** dòng `fare_amount < 0` (0,33%) | `sum(CASE WHEN fare_amount < 0 ...)` |
| Cầu nối tên cột | view `default.clean_nyc_taxi_tlc` (19 cột), partition pruning xuyên qua view, `2026-08 = 3.277.598` khớp bảng gốc | `src/register_compat_views.py` |
| Code của Người 3 chạy trên kho này | **PASS 6/6 nhóm hàm** (= 8 bước của `main.py`) trên partition 2024-01, 521 s: 2.924.072 dòng sạch → 5.296 dòng analytics → 37.448 cuốc huỷ từ raw → split 62.528/14.884 → GBT + metrics → `writer.py` ghi Parquet/CSV lên HDFS | smoke run lịch sử |
| Truy cập kiểu client | `spark.table("clean_nyc_taxi")` chạy được từ máy Windows, không cần biết đường dẫn file | `src/verify_access.py` |

## 2. Sơ đồ cluster và cổng

| Service | Ảnh | Vai trò | Cổng host |
|---|---|---|---|
| `namenode` | `apache/hadoop:3.4.1` | NameNode, giữ metadata HDFS | `8020` (RPC), `9870` (UI + WebHDFS) |
| `datanode` | `apache/hadoop:3.4.1` | 1 DataNode, replication=1, chứa Clean + Raw (56,98 GB theo `dfsadmin -report`) | `9866` (block native), `9864` (WebHDFS data), `9867` (ipc) |
| `metastore` | `apache/hive:3.1.3` | Hive Metastore Thrift + Derby nhúng | `9083` (Thrift) |
| `hiveserver2` | `apache/hive:3.1.3` | Hive SQL endpoint cho ODBC / Power BI | `10000` |
| `client` | `apache/hadoop:3.4.1` | Shell phụ vụ `docker exec` (`hdfs dfs ...`), không cần cài Hadoop trên Windows | — |

Volume: `./data/namenode`, `./data/datanode`, `./data/metastore` (có `metastore_db` của Derby).
Config: `./config/{core,hdfs,hive}-site.xml` mount vào cả 3 node.

## 3. Cách chạy

```powershell
Set-Location D:\BIGDATA\1-warehouse
docker compose up -d
docker compose ps               # 5 service phai "Up"
```

HiveServer2 phụ thuộc Metastore khỏe mạnh và cung cấp SQL qua cổng `10000`:

```powershell
docker compose up -d hiveserver2
```

Từ Power BI, tạo ODBC DSN `NYC_Taxi_Hive` trỏ đến `192.168.100.17:10000`,
schema `default`, không xác thực. Cấu hình DSN này được dùng bởi các truy vấn
trong `4-bi/power_query/`.

`docker compose up -d` du dung cho lan chay thu 2 tro di (mac dinh `IS_RESUME=true`).
**Rieng lan dau tien**, khi catalog Derby con trong, phai cho schematool tao schema truoc:

```powershell
$env:IS_RESUME = "false";  docker compose up -d metastore
Remove-Item Env:IS_RESUME      # de luon dung che do resume tu day tro di
```

Luu y: `IS_RESUME=false docker compose up ...` chi chay trong Git Bash. Trong PowerShell
phai dung `$env:` nhu tren, con khong bi loi "The term 'IS_RESUME=false' is not recognized".

Chép dữ liệu sạch lên HDFS (nguyên byte, có resume — chạy lại chỉ chép file thiếu):

```powershell
Set-Location D:\BIGDATA\1-warehouse\tools
& "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot\bin\javac" `
    -cp "D:\BIGDATA\2-processing\.venv\Lib\site-packages\pyspark\jars\*" `
    -d classes HdfsCopy.java
& "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot\bin\java" "-Dhadoop.home.dir=D:\hadoop" `
    -cp "D:\BIGDATA\2-processing\.venv\Lib\site-packages\pyspark\jars\*;classes" `
    HdfsCopy "D:\BIGDATA\2-processing\data\clean\trips" /data/nyc_taxi/clean/trips hdfs://<IP_may>:8020 4
```

Đăng ký bảng và kiểm tra:

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"
$env:HADOOP_HOME = "D:\hadoop"
$env:PYTHONIOENCODING = "utf-8"
$py = "D:\BIGDATA\2-processing\.venv\Scripts\python.exe"
& $py src\register_hive_tables.py      --host <IP_may>    # clean_nyc_taxi
& $py src\register_raw_nyc_taxi.py     --host <IP_may>    # raw_nyc_taxi (union schema)
& $py src\register_compat_views.py     --host <IP_may>    # view cho code 3-analytics
& $py src\verify_access.py             --host <IP_may>    # demo: chi spark.table(...) roi query
```

Raw lên HDFS bằng cùng `HdfsCopy` nhưng thêm tham số `tlc` ở cuối để đổi tên đường
`yellow_tripdata_2024-01.parquet` → `year=2024/month=1/part-2024-01.parquet` (chỉ đổi chỗ ghi,
nội dung file vẫn nguyên byte). File không theo khuôn (ví dụ `taxi_zone_lookup.csv`) bị dồn vào
`_meta/` để không lẫn vào cây partition. Bắt buộc chạy trong **Git Bash** với
`MSYS_NO_PATHCONV=1` — không thì MSYS biến `/data/nyc_taxi/...` thành
`C:/Program Files/Git/data/...` và file không bao giờ tới HDFS:

```bash
cd /d/BIGDATA/1-warehouse
MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*' \
java "-Dhadoop.home.dir=D:\hadoop" `
     -cp "D:/BIGDATA/2-processing/.venv/Lib/site-packages/pyspark/jars/*;tools/classes" `
     HdfsCopy "D:/BIGDATA/2-processing/data/raw" /data/nyc_taxi/raw/trips hdfs://<IP_may>:8020 4 tlc
```

## 4. Người 3 / Người 4 cần đặt gì

Chỉ ba cấu hình, không cần file dữ liệu, không cần biết đường dẫn Parquet:

```python
spark = (SparkSession.builder.master("local[*]").appName("analytics")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://<IP_may>:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://<IP_may>:8020")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .enableHiveSupport().getOrCreate())

df = spark.table("clean_nyc_taxi")     # 188 partition, 25 cột
```

`<IP_may>` là IP Wi-Fi của máy đang chạy cluster (xem mục 6). `dfs.client.use.datanode.hostname`
**bắt buộc** khi Spark chạy ngoài mạng Docker — thiếu nó client nhắm vào IP nội bộ `172.18.x`
của container và báo `could only be written to 0 of the 1 minReplication nodes`.

### Bảng có trong kho

| Bảng | Loại | Dữ liệu | Dùng cho |
|---|---|---|---|
| `default.clean_nyc_taxi` | external, 25 cột, 188 partition | `/data/nyc_taxi/clean/trips` (33,33 GiB) | phân tích chính; cột đã chuẩn hoá |
| `default.raw_nyc_taxi` | external, **19 cột dữ liệu + year/month** (union của 21 tên cột TLC), 188 partition | `/data/nyc_taxi/raw/trips` (19,55 GiB) | cuốc huỷ `fare_amount < 0` mà clean đã loại: **5.026.251 dòng** |
| `default.clean_nyc_taxi_tlc` | **view** (không chiếm chỗ) | như bảng sạch | code `3-analytics` đòi tên cột gốc TLC |

Ba điều phải biết khi dùng `raw_nyc_taxi` (đều đã đo, không phải suy đoán):

- **Thiếu `RatecodeID` và `passenger_count`.** Hai cột này là INT64 ở 133 tháng rồi
DOUBLE ở 55 tháng. Một lần đọc thử trực tiếp đã đo đủ 15 ca (mỗi cặp khai-báo ↔ kiểu-file thật)
  và không kiểu nào đọc được cả hai phía: `double ← INT64` chết giữa chừng
  (`FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH`, SQLSTATE KD001),
  `bigint ← DOUBLE` chết ngay khi mở cột (`SchemaColumnConvertNotSupportedException`),
  còn `int ← INT64` cũng chết — nên `bigint` chỉ cứu được INT32+INT64, `double` chỉ cứu
  được INT32+DOUBLE. Bảng bỏ hai cột đó. Code của
  Người 3 không đụng tới: `config.py` và `model.py` ghi rõ `passenger_count` bị loại
  khỏi feature một cách có chủ đích.
- **Thời gian trong raw bẩn hơn clean.** Scan toàn bảng cho `tpep_pickup_datetime`
  chạy từ `2001-01-01 07:01:48` tới `2098-09-11 09:23:31` — mảnh 2 đã lọc các mốc
  ngoài đời thực đó ở bảng sạch. Khi phân tích cuốc huỷ, hãy tự giới hạn
  `year BETWEEN 2011 AND 2026` và loại timestamp null.
- **Chênh raw − clean = 9.974.909 dòng**, trong đó chỉ 5.026.251 là `fare_amount < 0`;
  phần còn lại do các luật khác của mảnh 2 (timestamp null, zone null, khoảng
  cách/vượt thời gian bất hợp lý) — xem `2-processing/docs/schema.md`.

Tên cột của bảng sạch là `pickup_location_id` / `pickup_datetime` / `dropoff_datetime`, còn
`3-analytics/data_loader.py` kiểm tra `PULocationID` / `tpep_pickup_datetime` /
`tpep_dropoff_datetime` → đọc trực tiếp sẽ gặp
`ValueError: Table 'clean_nyc_taxi' is missing required columns`. View
`clean_nyc_taxi_tlc` (tạo bằng `src/register_compat_views.py`) đè lên đúng bảng đó và
**sửa cả kiểu**, nên Người 3 **không phải sửa code**:
`analysis.py::_trip_duration_minutes` làm `CAST(tpep_dropoff_datetime AS BIGINT)`, mà
Parquet lưu timestamp dạng micros nên Spark 4 suy ra `TIMESTAMP_NTZ` — NTZ **không**
cast sang BIGINT (`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`, đã gặp thật khi chạy
smoke run tháng `2024-01`). View cast về `TIMESTAMP` theo
`spark.sql.session.timeZone = America/New_York`; giá trị giờ đồng hồ giữ nguyên, và
`avg(độ lệch)` qua view khớp **tuyệt đối** với cột `trip_duration_min` có sẵn
(17,1653 phút ở partition 2026-08, lệch 0,000000).

`main.py` đã có sẵn cờ, không cần tham số mới:

```powershell
spark-submit --conf spark.hadoop.hive.metastore.uris=thrift://<IP_may>:9083 `
  --conf spark.hadoop.fs.defaultFS=hdfs://<IP_may>:8020 `
  --conf spark.hadoop.dfs.client.use.datanode.hostname=true `
  --conf spark.sql.catalogImplementation=hive `
  main.py --clean-table clean_nyc_taxi_tlc --raw-table raw_nyc_taxi
```

`--output-base hdfs:///user/hive/warehouse/nyc_taxi_analytics` dùng được ngay vì `fs.defaultFS`
đã trỏ vào cluster này. Hai giới hạn phải nói rõ: thời gian trong **raw** là mốc TLC ghi (UTC),
nên `year()/hour()` suy từ cột có thể lệch vài giờ so với partition `year=/month=`; và view
chỉ đổi tên + khai lại kiểu, không tính lại bất kỳ giá trị nào.

### Lệnh trên chạy ở máy nào? (đọc trước khi mất một buổi chiều)

Lệnh `spark-submit` ở mục này **được kiểm chứng trên chính máy đang chạy cluster** (máy Người 2,
`D:\hadoop` có sẵn `winutils.exe` + `hadoop.dll`, JDK 17 ở
`C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot`, spark-submit đến từ Python 3.11 hệ
thống — bản có `numpy` nên `pyspark.ml` chạy được). Ba thứ đó **không đi theo repo**.

Nếu Người 3/4 chạy từ PC riêng thì trên máy đó phải tự có đủ bốn thứ, thiếu một là chết:

1. **JDK 17** (không phải Java mới nhất trong PATH — máy này có sẵn Java 25 và Spark 4.0.1
   không dùng được với nó), đặt `JAVA_HOME` trỏ vào bản 17.
2. **`HADOOP_HOME` kiểu Windows**: một thư mục có `bin\winutils.exe` và `bin\hadoop.dll`
   (Hadoop 3.4.x). Không có thì Spark báo `ExitCodeException ... winutils.exe`.
3. **pyspark 4.0.1 + numpy** trong cùng một interpreter (numpy là bắt buộc vì `pyspark.ml`;
   `.venv` của mảnh 2 KHÔNG có numpy nên không dùng được cho phần ML).
4. Cùng **mạng LAN/Wi-Fi** với máy chủ, vì kho không có địa chỉ công cộng.

**Những gì ĐÃ đo được về network (kiểm tra lại 2026-11-03):** 8020 và 9083 đang listen trên
`::` (mọi interface) và `0.0.0.0` do Docker công bố; cổng DataNode 9864/9866/9867 cũng đã mở;
Wi-Fi của máy này đang ở profile **Public** nhưng rule *Docker Desktop Backend* cho phép
Inbound-Allow trên đúng profile đó, `RemoteAddress = Any`; alias
`192-168-<a>-<b>.sslip.io` mà DataNode quảng bá resolve ra đúng IP hiện hành.

**Những gì CHƯA nghiệm thu: chưa có một máy vật lý thứ hai kết nối vào kho.** Toàn bộ PASS
hôm nay là client chạy trên chính máy chủ (đi qua IP LAN) hoặc trong mạng Docker. Nên nếu
Người 3 báo không nối được, nghi theo thứ tự này trước khi đổ cho cluster:

1. Máy đó và máy chủ **có cùng một Wi-Fi/LAN** không (khác subnet là không nối được, kể cả
   khi cổng mở).
2. Mạng đó **resolve được `*.sslip.io`** không — đây là DNS công cộng; mạng công ty/trường
   chặn DNS ngoài sẽ khiến NameNode vẫn nối được nhưng DataNode chết (triệu chứng: ghi file
   báo `could only be written to 0 of the 1 minReplication nodes`).
3. IP máy chủ **đã đổi** sau lần kiểm tra này không → chạy `tools\Set-Host.ps1` rồi xin IP mới.
4. Docker Desktop trên máy chủ **tự tắt engine** đã xảy ra thật (containers `Exited (255)`) →
   mở lại app, `docker compose up -d`, dữ liệu volume không mất.

Báo nguyên văn thông báo lỗi cho Người 2 thay vì mô tả, vì ba ca này trông rất giống nhau.

### Đã chạy thay Người 3 trên kho này (kết quả thật, không phải suy)

Một lần smoke run đã import nguyên `analysis.py`, `data_loader.py`, `evaluation.py`,
`feature_engineering.py`, `model.py`, `writer.py` trong `3-analytics/` (không sửa một dòng
nào) rồi gọi đúng thứ tự như `main.py`, giới hạn partition
`2024-01` và `maxIter=10` cho nhanh. Sau 521 s: **PASS 6/6 nhóm hàm** —

| Bước | Số đo |
|---|---|
| `load_clean_trips("clean_nyc_taxi_tlc")` | 2.924.072 dòng (raw tháng này 2.964.624 → clean giữ 98,6%) |
| 4 câu SQL analytics | `analytics_summary` 5.296 dòng, `fare theo giờ` 24, `heatmap` 168 ô |
| `load_raw_trips("raw_nyc_taxi")` + cancelled | 37.448 cuốc huỷ `fare_amount < 0` của 2024-01 |
| feature + chronological split | 77.412 dòng zone×ngày → train 62.528 / test 14.884 |
| GBT (maxIter=10) vs baseline | RMSE **48,00** vs 29,48 — GBT **tệ hơn** baseline ở cấu hình smoke này |
| `writer.py` → HDFS | 2 Parquet + 1 CSV ghi vào `/tmp/nyc_taxi_analytics_smoke` |

Con số RMSE/MAPE không phải lỗi của kho: nó là hệ quả của việc chỉ huấn luyện trên
**một tháng** với `maxIter=10` (MAPE 923% cho thấy test period có thang đo khác hẳn
train). **`--max-iter` mặc định của `main.py` là 50, không phải 10** — con số 48,00 ở
dòng trên chỉ mô tả cấu hình smoke, đừng dùng nó để kết luận "GBT tệ". Người 3 nên chạy
cả năm rồi mới đánh giá model; việc của mảnh 1 là chứng minh **đường dữ liệu thông** —
và nó thông, kể cả bước ghi output lên HDFS.

## 5. Số đo thực tế trên máy này (i7, Wi-Fi, Docker Desktop WSL2)

| Đường truyền | Thông lượng | Ghi chú |
|---|---|---|
| `HdfsCopy` native, 4 luồng, Windows → HDFS | **66,3 MiB/s** trung bình (33,1 GiB / 499 s); đỉnh 94 MiB/s | đã chọn làm đường chuyển 34 GB |
| `HdfsCopy -layout tlc`, Raw 19,55 GiB | 15,9 MiB/s (20.986.383.705 B / 1259 s) | cùng tool, file to hơn nên chậm hơn; nghiệm thu vẫn `PASS` khớp byte |
| Đọc một file Parquet, 1 luồng native | 30,4 MiB/s | `DnProbe` |
| WebHDFS `PUT` từ Windows | 12,0 MiB/s | chậm hơn 5,5 lần, chỉ dùng khi không có client native |
| `hdfs dfs -put` từ container qua bind mount | 3,4 MiB/s | chậm nhất; đọc file Windows qua mount 9P |
| `count(*)` toàn bảng 1,506 tỷ dòng | 152 s cho cả đăng ký + đếm | Spark đọc footer Parquet, không quét file |
| Quét thật 19,55 GiB raw (`count` + min/max timestamp + đếm `fare_amount<0`) qua đủ 188 file | **587 s** cho cả đăng ký + scan (~34 MiB/s) | đây là bằng chứng duy nhất có giá trị rằng schema union đọc được mọi tháng |

Dung lượng chiếm trên đĩa: đúng bằng dữ liệu (`33,3 GiB`), không nhân bản. `dfs.replication=1`
vì chỉ có 1 DataNode; để 2–3 thì mọi block báo `under-replicated` và tốn gấp đôi chỗ.

Số đo `hdfs dfsadmin -report` hai thời điểm trong ngày 2026-11-03:

| Chỉ số | Khi Raw đang chuyển dở | Sau khi Raw xong |
|---|---|---|
| Configured Capacity | 729,82 GB (783.635.443.712 B) | 729,82 GB (không đổi) |
| DFS Used | 51,80 GB | **56,98 GB** (61.183.602.688 B) = Clean 33,3 + Raw 19,55 + 3,7 GB rác benchmark `/bench` |
| DFS Remaining | 541,31 GB | 536,75 GB |
| Live datanodes | 1 | 1 |
| Under-replicated / corrupt / missing blocks | — | **0 / 0 / 0** |

## 6. Khi máy đổi mạng (IP thay đổi) — chạy một lệnh

```powershell
Set-Location D:\BIGDATA\1-warehouse\tools
.\Set-Host.ps1                    # tu phat hien IP, sua config, restart node
.\Set-Host.ps1 -RebuildCatalog    # dung khi do table loi "MinProgress"/ treo 15 phut
```

Script sửa `dfs.datanode.hostname` (thành `<ip-dấu-chấm>.sslip.io`) và
`metastore.warehouse.dir`, rồi restart `namenode`/`datanode`/`metastore`. Sau đó **phải**
chạy lại `register_hive_tables.py --host <IP mới>` vì location của database đã bị ghi chết
trong catalog Derby. Khi cần dựng catalog sach, `-RebuildCatalog` **đổi tên** thư mục
`metastore_db` cũ thành `metastore_db_ipcu_<timestamp>` chứ không xoá.

## 7. Những chỗ đã làm hỏng và cách tránh (đọc trước khi sửa config)

- **`apache/hive:standalone-metastore-4.1.0` trở lên không dùng được với Spark 4.0.1.** Spark
  mang Hive client 2.3.9 và gọi Thrift method `get_table(db, tbl)`; Hive 4.1 đã xoá method đó
  (chỉ còn `get_table_req`, `get_table_meta`). Triệu chứng: `SHOW DATABASES` vẫn chạy, nhưng
  đụng tới table là `Invalid method name: 'get_table'`. Ảnh `3.1.3` giữ API cũ nên đã chốt ở đây.
- **Derby nhúng chỉ một JVM mở được.** Không chạy `hive -e`/`beeline` trỏ vào `metastore_db`
  khi metastore đang chạy: `Another instance of Derby may have already booted the database`.
  Mọi truy vấn metadata phải đi qua Thrift (tức là qua Spark).
- **`host.docker.internal` trên Windows máy này phân giải về `192.168.110.5` — không có gì nghe
  ở đó** (entry cũ trong `C:\Windows\System32\drivers\etc\hosts`). Đừng bao giờ quảng bá
  DataNode bằng tên này; dùng `<ip-dấu-chấm>.sslip.io` (DNS wildcard công cộng, trả về đúng IP,
  resolve được cả từ Windows lẫn trong container, không phải sửa hosts).
- **Không quảng bá DataNode bằng đúng IP thô.** Khi hostname trùng chuỗi địa chỉ của chính
  client, DFSClient coi DataNode là "local node" và loại khỏi pipeline ngay từ `addBlock`.
  Alias `sslip.io` tránh điều này vì khác chuỗi dù cùng IP.
- **`IS_RESUME` bị baked vào container lúc `up`.** Nếu lần trước chạy `IS_RESUME=false` rồi lần
  sau chỉ `docker compose restart metastore`, schematool chạy lại trên DB đã có schema và fail
  `FUNCTION 'NUCLEUS_ASCII' already exists`. Dùng `up -d` (nó recreate) thay vì `restart`.
- **Docker Desktop trên máy này tự tắt engine** (containers `Exited (255)`, pipe
  `dockerDesktopLinuxEngine` mất). Cách hồi phục: khởi động lại Docker Desktop, chờ
  `docker info` thành công, rồi `docker compose up -d`. Dữ liệu trên volume không mất.
- Cảnh báo `The configured local directories are not expected to be URIs ... [D:/tmp/spark]`
  của Spark là vô hại (Spark tự chấp nhận đường dẫn Windows), không phải lỗi.

## 8. Cấu trúc thư mục

```
1-warehouse/
  docker-compose.yml          4 service nhu muc 2
  config/core-site.xml        fs.defaultFS = hdfs://namenode:8020 (chi dung TRONG container)
  config/hdfs-site.xml        replication=1, alias DataNode, WebHDFS, tat permission check
  config/hive-site.xml        Derby duong dan tuyet doi tren volume, warehouse.dir theo IP host
  src/register_hive_tables.py  tao bang external + MSCK REPAIR + doi chieu 188 partition/1,506 ty dong
  src/register_raw_nyc_taxi.py bang raw_nyc_taxi tu 188 file raw khong cung schema
                              (DDL sinh tu source_manifest.csv cua man 2, khong mo file)
  src/register_compat_views.py view clean_nyc_taxi_tlc: doi ten cot ve kieu file goc cho 3-analytics
  src/verify_access.py        gia lam nguoi 3: chi spark.table("clean_nyc_taxi") roi query
  tools/HdfsCopy.java         chep local -> HDFS nguyen byte, nhieu luong, co resume, tu nghiem thu
                              (them tu "tlc" o cuoi de doi ten file TLC thanh year=/month=)
  tools/Set-Host.ps1          dong bo IP khi may doi mang (+ tuy chon dung lai catalog)
  data/                       HDFS namenode/datanode + catalog Derby (KHONG len git)
```

## 9. Việc còn treo của mảnh 1

- **Metastore là Derby nhúng**: một JVM mở một lúc, và **một người ghi**. Nếu cả 4 cùng chạy
  Spark rồi tạo bảng/dữ liệu đồng thời thì phải chuyển sang PostgreSQL (đổi
  `javax.jdo.option.Connection*` trong `config/hive-site.xml` rồi chạy lại `schematool`).
  Chỉ đọc (`spark.table`) thì không sao — Thrift phục vụ nhiều client cùng lúc.
- **Cluster chạy trên máy cá nhân**, không phải máy chủ chung: khi Người 1 nhận lại phần này,
  copy nguyên thư mục `1-warehouse/` sang máy khác rồi chạy mục 3 + 6. Kho của nhóm chỉ tồn tại
  lúc máy này bật và Docker đang chạy — buổi demo cần người mang máy.
- **Kết nối từ máy khác CHƯA được nghiệm thu bằng một máy thật.** Đã kiểm tra cấu hình tới mức:
  cổng listen mọi interface, firewall cho phép Inbound trên đúng profile Wi-Fi hiện tại, alias
  sslip.io resolve đúng (mục 4 có bảng số đo). Nhưng mọi client đã chạy hôm nay đều ở trên
  chính máy chủ hoặc trong mạng Docker, nên "Người 3 ngồi PC riêng mà nối được" mới là giả
  thuyết có cơ sở, chưa phải số đo thật. Ca này chỉ đóng được khi có người thứ hai chạy thật.
- **Rác do quá trình dựng kho để lại, chưa xoá theo đúng quy tắc "không xoá file"**:
  trên HDFS có `/bench` (3,7 GB file ghi khi đo thông lượng), `/probe` (28 B) và
  `/tmp/nyc_taxi_analytics_smoke` (output thử của smoke run, không phải
  bảng chính thức); trên đĩa có
  `data/metastore/metastore_db_ipcu_20261103*` (catalog cũ bị đổi tên khi dựng lại sau khi
  IP đổi) và `2-processing/.tmp/` (log probe/copy). Ai muốn sạch thì xoá hộ, mặc định để nguyên.
- **Ảnh `apache/hive:standalone-metastore-4.2.1` (1,38 GB) vẫn nằm trong Docker nhưng không dùng**:
  thử trước khi chốt 3.1.3, Thrift của nó đã xoá `get_table` nên Spark 4.0.1 không nói
  chuyện được. `docker rmi` nếu cần chỗ.
- **Môi trường Python của Người 3 khác máy này một chỗ**: `2-processing/.venv` không cài
  `numpy` nên không import được `pyspark.ml` (mà `3-analytics/evaluation.py` cần).
  smoke run này vì vậy đã chạy bằng Python 3.11 hệ thống của máy
  (`C:\Users\ThinkPad\AppData\Local\Programs\Python\Python311`, đã có pyspark 4.0.1 +
  numpy 2.4.6 + pyarrow 24.0.0). Người 3 chỉ cần env của chính mình có numpy là chạy được.
- **`4-bi/` chưa có dữ liệu riêng**: Người 4 đọc thẳng hai bảng trong kho (mục 4) hoặc Parquet
  output của Người 3 ghi ra `/user/hive/warehouse/nyc_taxi_analytics`.
