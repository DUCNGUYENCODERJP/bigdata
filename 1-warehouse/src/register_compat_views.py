"""Tạo view `clean_nyc_taxi_tlc` để code của Người 3 chạy thẳng trên bảng sạch.

Lý do: `3-analytics/data_loader.py` kiểm tra các cột tên gốc của file TLC —
`PULocationID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `fare_amount` —
trong khi bảng `clean_nyc_taxi` của mảnh 1 dùng tên đã chuẩn hoá
(`pickup_location_id`, `pickup_datetime`, `dropoff_datetime`). Đọc trực tiếp thì
hàm `_assert_columns` báo `Table 'clean_nyc_taxi' is missing required columns`.

View này là **lớp tên** đè lên đúng bảng đó (không copy dữ liệu, không chiếm thêm
chỗ, không sửa file nào trong `3-analytics/`). Người 3 chỉ cần đổi một dòng trong
`config.py`:

    clean_table: str = "clean_nyc_taxi_tlc"

Chạy lại bao nhiêu lần cũng được: `CREATE OR REPLACE VIEW` chỉ ghi metadata.

    $env:PYTHONIOENCODING = "utf-8"
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        D:\\BIGDATA\\1-warehouse\\src\\register_compat_views.py --host 192.168.100.19
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType

SOURCE_TABLE = "clean_nyc_taxi"
VIEW_NAME = "clean_nyc_taxi_tlc"
SPARK_LOCAL_DIR = os.environ.get("SPARK_LOCAL_DIR", "D:/tmp/spark")
HADOOP_HOME = os.environ.get("HADOOP_HOME", "D:/hadoop").replace("\\", "/")

# Cột mà 3-analytics thực sự đụng tới (data_loader + analysis + feature_engineering).
# Cac cot con lai cua bang sach van giu nguyen ten de quy truy van khong bi mat.
# Nguoi 3 dung `F.col("tpep_dropoff_datetime").cast("long")` de tinh thoi luong
# (analysis.py::_trip_duration_minutes). Bang sach duoc dang ky bang kieu Spark
# suy ra tu Parquet, ma Parquet luu timestamp kieu gioi tinh (INT64 micros) ->
# Spark 4 sinh ra TIMESTAMP_NTZ, va NTZ **khong cast duoc sang BIGINT**:
# DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION. View nay cast ve TIMESTAMP (co mui
# gio theo spark.sql.session.timeZone = America/New_York) dung nhu cai ma code
# cua ho doi. Gia tri wall-clock khong doi vi du ly + gio dem cung mot mui; chi
# so chenh lech (thoi luong cuoc) thi mui gio tru di nhau, khong anh huong.
VIEW_SQL = f"""
CREATE OR REPLACE VIEW `{VIEW_NAME}` AS
SELECT
    CAST(pickup_datetime AS TIMESTAMP)  AS tpep_pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    pickup_location_id   AS PULocationID,
    dropoff_location_id  AS DOLocationID,
    `hour`                 AS pickup_hour,
    day_of_week,
    fare_amount,
    trip_distance,
    trip_duration_min,
    total_amount,
    tip_amount,
    tolls_amount,
    passenger_count,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    `year`,
    `month`
FROM `{SOURCE_TABLE}`
"""

# Nguoi 3 dung ten goc, nen bang raw khong can view: no da co san
# tpep_pickup_datetime / PULocationID / fare_amount.
REQUIRED_BY_PERSON3 = ("PULocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
                       "fare_amount")
DATETIME_COLUMNS = ("tpep_pickup_datetime", "tpep_dropoff_datetime")


def build_spark(host: str) -> SparkSession:
    hdfs_uri = f"hdfs://{host}:8020"
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("register-compat-views")
        .config("spark.local.dir", SPARK_LOCAL_DIR)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{host}:9083")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME)
        .config("spark.sql.warehouse.dir", f"{hdfs_uri}/opt/hive/data/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Tạo view {VIEW_NAME} cho 3-analytics.")
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "192.168.100.19"))
    parser.add_argument("--database", default="default")
    args = parser.parse_args()

    started = time.monotonic()
    spark = build_spark(args.host)
    spark.sql(f"USE `{args.database}`")

    if len(spark.table(SOURCE_TABLE).columns) != 25:
        print(f"[FAIL] {SOURCE_TABLE} không có 25 cột; chạy register_hive_tables.py trước.")
        return 1

    spark.sql(VIEW_SQL)
    view = spark.table(VIEW_NAME)
    # Kiem kieu truc tiep: code cua Nguoi 3 lam `.cast("long")` tren hai cot nay,
    # ma Spark 4 tu choi cast TIMESTAMP_NTZ -> BIGINT. View phai tra TimestampType.
    dtypes = {f.name: f.dataType for f in view.schema.fields}
    not_ts = [n for n in DATETIME_COLUMNS if not isinstance(dtypes.get(n), TimestampType)]
    print(f"[types]  " + ",  ".join(f"{n} = {dtypes[n]}" for n in DATETIME_COLUMNS))
    if not_ts:
        print(f"[FAIL] {not_ts} khong phai TIMESTAMP (mau doi loi cho Nguoi 3 o tren)")
        return 1
    missing = sorted(set(REQUIRED_BY_PERSON3) - set(view.columns))
    print(f"[view]   `{args.database}`.`{VIEW_NAME}`  {len(view.columns)} cột")
    print(f"[view]   cột cần cho 3-analytics: {', '.join(REQUIRED_BY_PERSON3)}")
    if missing:
        print(f"[FAIL] view vẫn thiếu {missing}")
        return 1

    # Partition pruning phai xuyen qua view, neu khong thi Nguoi 3 quet ca 34 GB.
    query = f"SELECT count(*) AS n FROM {VIEW_NAME} WHERE year = 2026 AND month = 8"
    plan = spark.sql(f"EXPLAIN EXTENDED {query}").collect()[0][0]
    pruned = "year=2026/month=8" in plan
    print(f"[pruning] view có cắt partition: {pruned}")
    if not pruned:
        print("[FAIL] kế hoạch đọc không giới hạn vào partition — xem explain trong log")
        print(plan[-1200:])
        return 1

    n = spark.sql(f"SELECT count(*) AS n FROM {VIEW_NAME} WHERE year = 2026 AND month = 8").collect()[0]["n"]
    print(f"[count] 2026-08 qua view = {n:,}  (bang goc la 3,277,598)")

    # Do dung bieu thuc cua Nguoi 3 (analysis.py::_trip_duration_minutes): cast hai
    # cot timestamp ve so va tru nhau. So voi cot trip_duration_min san co cua bang
    # sach — hai cach phai ra cung mot dai luong, neu lech la view doi mui gio sai.
    check = spark.sql(
        f"""SELECT avg((CAST(tpep_dropoff_datetime AS BIGINT) - CAST(tpep_pickup_datetime AS BIGINT)) / 60.0) AS theirs,
                   avg(trip_duration_min) AS stored
            FROM {VIEW_NAME} WHERE year = 2026 AND month = 8"""
    ).collect()[0]
    drift = abs(float(check["theirs"]) - float(check["stored"]))
    print(f"[duration] 2026-08: cong thuc Nguoi 3 = {check['theirs']:.4f} phut, "
          f"cot trip_duration_min = {check['stored']:.4f} phut, lech {drift:.6f}")
    if drift > 0.01:
        print("[FAIL] view lam doi thoi luong — kiem tra spark.sql.session.timeZone")
        return 1

    raw = spark.table("raw_nyc_taxi")
    raw_missing = sorted(set(REQUIRED_BY_PERSON3) - set(raw.columns))
    print(f"[raw]    raw_nyc_taxi {len(raw.columns)} cột, thiếu: {raw_missing or 'không'}")
    if raw_missing:
        return 1

    print(f"[done] {time.monotonic() - started:.0f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
