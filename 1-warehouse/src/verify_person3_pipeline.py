"""Chạy chính mã của Người 3 (3-analytics) trên kho Hive của mảnh 1 để bàn giao.

Script này **không sửa** gì trong `3-analytics/`; nó import các hàm của họ rồi gọi
đúng thứ tự như `main.py`, nhưng giới hạn dữ liệu (một năm, GBT maxIter nhỏ) để
chạy xong trong vài phút trên laptop. Mục đích: chứng minh trước khi Người 3 bật
máy rằng `load_clean_trips` / `load_raw_trips` / 4 hàm analysis / feature split /
model / writer đều chạy được thật trên cluster này.

Cầu nối tên cột: bảng sạch dùng tên chuẩn hoá, còn `data_loader.py` đòi tên gốc
TLC, nên phần clean đọc qua view `clean_nyc_taxi_tlc` (tạo bởi
`register_compat_views.py`). Bảng raw đã có sẵn tên gốc nên dùng trực tiếp.

Vì sao không dùng .venv của mảnh 2: `3-analytics/evaluation.py` import
`pyspark.ml` -> cần `numpy`, mà `2-processing/.venv` không cài numpy (và quy tắc
của đề tài là không cài thêm gói). Máy này có sẵn Python 3.11 cài
pyspark 4.0.1 + numpy 2.4.6 + pyarrow 24.0.0 — đúng bản PySpark mà mảnh 2 đang
dùng — nên script chạy bằng interpreter đó.

    $env:PYTHONIOENCODING = "utf-8"
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    & "C:\\Users\\ThinkPad\\AppData\\Local\\Programs\\Python\\Python311\\python.exe" `
        D:\\BIGDATA\\1-warehouse\\src\\verify_person3_pipeline.py
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F

from warehouse_host import resolve_host

ANALYTICS_DIR = os.environ.get("ANALYTICS_DIR", r"D:\BIGDATA\3-analytics")
CLEAN_TABLE = "clean_nyc_taxi_tlc"
RAW_TABLE = "raw_nyc_taxi"
SMOKE_DIR = os.environ.get("SPARK_LOCAL_DIR", "D:/tmp/spark")
HADOOP_HOME = os.environ.get("HADOOP_HOME", "D:/hadoop").replace("\\", "/")
# Nam demo: du lon de split train/test co nghia, du nho de xong trong vai phut.
DEMO_YEAR = 2024
DEMO_MONTH = 1
MAX_ITER = 10


def build_spark(host: str) -> SparkSession:
    hdfs_uri = f"hdfs://{host}:8020"
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("verify-person3-pipeline")
        .config("spark.local.dir", SMOKE_DIR)
        .config("spark.driver.memory", "3g")
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
    parser = argparse.ArgumentParser(description="Chạy thử pipeline của Người 3 trên kho mảnh 1.")
    parser.add_argument("--host", default=None,
                        help="IP máy chạy cluster; thiếu thì tự phát hiện (warehouse_host.py).")
    parser.add_argument("--hdfs-out", default="/tmp/nyc_taxi_analytics_smoke",
                        help="Thư mục HDFS nhận output cuoi cung (chi de do, khong phai bang chinh thuc).")
    args = parser.parse_args()
    args.host = resolve_host(args.host)

    if not os.path.isdir(ANALYTICS_DIR):
        print(f"[FAIL] Khong thay thu muc ma cua Nguoi 3: {ANALYTICS_DIR}")
        return 1
    sys.path.insert(0, ANALYTICS_DIR)

    # Import tat ca cac module ho da viet — loi o day la loi cua ho, khong phai cua kho.
    from analysis import (build_analytics_summary, build_cancelled_trips_summary,
                          build_demand_heatmap, build_hourly_fare_summary)
    from data_loader import load_clean_trips, load_raw_trips
    from evaluation import compute_metrics, print_comparison_table
    from feature_engineering import build_feature_data, chronological_split
    from model import add_baseline_predictions, add_gbt_predictions, train_gbt
    from writer import write_parquet, write_metrics_csv

    started = time.monotonic()
    spark = build_spark(args.host)
    steps: list[str] = []

    # Step 1 — load qua cau noi ten cot.
    # Chi lay 5 col pipeline cua ho dung toi roi cache: bang sach co 25 cot va 1,5 ty dong,
    # doc full se tran RAM; mot thang 2,9 trieu dong x 5 col thi bo vua va moi lenh dem
    # duoi day doc tu cache thay vi quy lai Parquet gan 10 lan.
    trips = (
        load_clean_trips(spark, CLEAN_TABLE)
        .filter((F.col("year") == DEMO_YEAR) & (F.col("month") == DEMO_MONTH))
        .select("tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID",
                "fare_amount", "trip_distance")
        .cache()
    )
    n_trips = trips.count()
    print(f"[step1] load_clean_trips('{CLEAN_TABLE}') -> {n_trips:,} dong {DEMO_YEAR}-{DEMO_MONTH:02d}")
    steps.append("load_clean_trips")

    # Step 2 — 4 cau analysis, dung nguyen ham cua ho
    analytics_summary = build_analytics_summary(trips)
    hourly = build_hourly_fare_summary(trips)
    heatmap = build_demand_heatmap(trips)
    print(f"[step2] analytics_summary (zone x hour) = {analytics_summary.count():,} dong")
    print(f"[step2] fare theo gio = {hourly.count():,} dong, heatmap = {heatmap.count():,} o")
    steps.append("analysis.build_*")

    # Step 3 — cancelled tu raw (dung ten goc cua ho, khong can view)
    raw_trips = load_raw_trips(spark, RAW_TABLE).filter((F.col("year") == DEMO_YEAR) & (F.col("month") == DEMO_MONTH))
    cancelled = build_cancelled_trips_summary(raw_trips)
    n_cancelled = cancelled.select(F.sum("cancelled_count")).head()[0]
    print(f"[step3] load_raw_trips('{RAW_TABLE}') + cancelled -> {n_cancelled or 0:,} dong fare<0 thang {DEMO_YEAR}-{DEMO_MONTH:02d}")
    steps.append("load_raw_trips + cancelled")

    # Step 4-5 — feature va split
    feature_data = build_feature_data(trips)
    train_data, test_data = chronological_split(feature_data, 0.8)
    n_train, n_test = train_data.count(), test_data.count()
    print(f"[step5] feature rows = {feature_data.count():,}  train = {n_train:,}  test = {n_test:,}")
    if n_train == 0 or n_test == 0:
        print("[FAIL] split rong — khong co du lieu de huan luyen")
        return 1
    steps.append("feature + split")

    # Step 6-7 — baseline + GBT + metrics
    test_with_baseline = add_baseline_predictions(test_data, train_data)
    model = train_gbt(train_data, max_iter=MAX_ITER)
    results = add_gbt_predictions(model, test_with_baseline)
    metrics = [compute_metrics(results, "baseline_prediction", "baseline"),
               compute_metrics(results, "gbt_prediction", "gbt")]
    print_comparison_table(metrics)
    steps.append("GBT + metrics")

    # Step 8 — ghi that len HDFS bang chinh writer.cua.ho
    out = args.hdfs_out
    write_parquet(analytics_summary, f"hdfs://{args.host}:8020{out}/analytics_summary")
    write_parquet(cancelled, f"hdfs://{args.host}:8020{out}/cancelled_trips")
    write_metrics_csv(spark, metrics, f"hdfs://{args.host}:8020{out}/metrics")
    print(f"[step8] writer.py ghi 2 bang + 1 CSV len HDFS {out} -> OK")
    steps.append("writer -> HDFS")

    print(f"[steps] {len(steps)}/{len(steps)} nhom ham cua Nguoi 3 chay duoc: {', '.join(steps)}")
    print(f"[done]  {time.monotonic() - started:.0f} s")
    print("PASS")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
