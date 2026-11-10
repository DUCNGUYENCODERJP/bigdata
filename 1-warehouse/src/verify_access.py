"""Giả làm Người 3: truy van bang sach tren HDFS chi bang 2 dong cau hinh ket noi.

Script nay KHONG doc config cua manh 2, KHONG biet duong dan Parquet - no chi
`spark.table("clean_nyc_taxi")` roi query. Day la bang chung cuoi cung cua manh 1:
chi can node bat cluster, moi thanh vien chay Spark o may minh doc duoc du lieu.

Chay:

    $env:PYTHONIOENCODING = "utf-8"
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        D:\\BIGDATA\\1-warehouse\\src\\verify_access.py --host 192.168.36.64
"""

from __future__ import annotations
import logging

import argparse
import os
import sys
import tempfile

from pyspark.sql import SparkSession

EXPECTED_ROWS = 1_506_791_871
EXPECTED_PARTITIONS = 188


def main() -> int:
    parser = argparse.ArgumentParser(description="Kiem tra spark.table('clean_nyc_taxi') tu may client.")
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "192.168.100.17"),
                        help="IP may dang chay cluster slice 1.")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.master("local[*]").appName("verify-access")
        .config("spark.local.dir", os.environ.get(
            "SPARK_LOCAL_DIR", os.path.join(tempfile.gettempdir(), "nyc-taxi-spark")
        ))
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "America/New_York")
        # Hai dong duoc ghi ra README - toan bo pho con lai nam phia cluster.
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{args.host}:9083")
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{args.host}:8020")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .enableHiveSupport().getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.table("clean_nyc_taxi")
    logging.info(f"[table]  clean_nyc_taxi  cot={len(df.columns)}  partition={df.select('year','month').distinct().count()}")

    total = df.count()
    logging.info(f"[count]  toan bang = {total:,}  (mong doi {EXPECTED_ROWS:,})")

    # Partition pruning: ke hoach vat ly chi ke khai dung partition duoc loc.
    logging.info("[pruning] ke hoach truy van loc year=2026/month=8:")
    df.where("year = 2026 AND month = 8").explain()

    for y, m in ((2011, 1), (2020, 3), (2026, 8)):
        n = df.where(f"year = {y} AND month = {m}").count()
        logging.info(f"[count]  {y}-{m:02d} = {n:,}")

    df.where("year = 2024").groupBy("month").agg(
        {"fare_amount": "avg", "trip_distance": "sum"}
    ).orderBy("month").show(12, truncate=False)

    ok = total == EXPECTED_ROWS
    logging.info("PASS" if ok else "FAIL")
    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
