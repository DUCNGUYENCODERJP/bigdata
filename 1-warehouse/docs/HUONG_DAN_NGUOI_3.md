# Huong dan nhanh cho Nguoi 3 - kho dang BAT (kiem chung lai sang 05/11/2026)

May cua Nguoi 2 dang chay cluster.

B1: Ba gia tri ket noi cua hom nay. IP Wi-Fi cua may nay da doi ba lan trong ba ngay 02→05/11
(192.168.100.19 → 192.168.36.64 → ve 192.168.100.19 → 192.168.100.17), nen truoc khi chay thi doi
chieu bang `ipconfig` tren may Nguoi 2 hoac xin lai; ba gia tri duoi day KHONG phai hang so:

```
hive.metastore.uris              = thrift://192.168.100.17:9083
fs.defaultFS                     = hdfs://192.168.100.17:8020
dfs.client.use.datanode.hostname = true
```

B2: Chi can ba cau hinh tren, khong can file du lieu, khong can biet duong dan Parquet:

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.master("local[*]").appName("analytics")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://192.168.100.17:9083")
    .config("spark.hadoop.fs.defaultFS", "hdfs://192.168.100.17:8020")
    .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    .enableHiveSupport().getOrCreate())

trips = spark.table("clean_nyc_taxi_tlc")   # 1.506.791.871 dong, 188 partition, ten cot dung chuan TLC
raw   = spark.table("raw_nyc_taxi")         # 1.516.766.780 dong, 19 cot, 5.026.251 dong fare_amount < 0
```

B3: `clean_nyc_taxi_tlc` la **view** dung tren bang sach `clean_nyc_taxi`: giu ten cot dung chuan
TLC va tra `TimestampType` (bang sach luu TIMESTAMP_NTZ, ma `analysis.py` tinh thoi luong bang
`CAST(tpep_dropoff_datetime AS BIGINT)` - Spark 4 tu choi cast NTZ sang BIGINT). Vi vay chay pipeline
thi tro vao view, **khong doi file nao trong `3-analytics/`**:

```
main.py --clean-table clean_nyc_taxi_tlc --raw-table raw_nyc_taxi
```

B4: `raw_nyc_taxi` **khong co** `RatecodeID` va `passenger_count` (133 thang luu INT64, 55 thang
luu DOUBLE, khong kieu Parquet nao doc duoc ca hai - do 15 ca do bang
`1-warehouse/tools/probe_type_promotion.py`). `Airport_fee`/`store_and_fwd_flag` co khai bao nhung
rieng thang 2024-01 da co 140.162 dong null o CA HAI cot, nen dung `IS NOT NULL` truoc khi group;
chi tiet va ly do o muc "Hai dieu rut ra cho ai query raw_nyc_taxi" trong doan dau file do.

B5: kho che o day: `1-warehouse/README.md` (muc 3 = bat kho, muc 4 = cau hinh client, muc 6 = khi
may doi IP/reboot, muc 9 = viec con treo). Nguoi 2 bat kho bang nut play cua Docker Desktop (stack
`nyc-warehouse`) hoac `docker compose up -d`, roi kiem tra `docker compose ps`: ca 4 phai
`Up ... (healthy)`. Neu metastore o trang thai `Restarting` thi do loi `IS_RESUME` (log ghi
`FUNCTION 'NUCLEUS_ASCII' already exists`), chua bang `docker compose up -d metastore` - **khong**
chay `schematool` thu cong vi Derby chi mot JVM mo duoc. Neu IP may Nguoi 2 doi thi phai
`tools/Set-Host.ps1 -RebuildCatalog` va dang ky lai bang, luc do moi thu tu lenh nam o muc 6 README.
