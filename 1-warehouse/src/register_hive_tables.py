"""Đăng ký cây Parquet đã nằm trên HDFS thành bảng Hive để Người 3 truy vấn.

Đây là mảnh 1 (warehouse): dữ liệu sạch của mảnh 2 được chép nguyên byte lên
`/data/nyc_taxi/clean/trips`, còn script này chỉ tạo **siêu dữ liệu** trong
Hive Metastore — không ghi đè, không chép, không xoá byte dữ liệu nào. Bảng là
external table trỏ thẳng vào thư mục HDFS, nên `DROP TABLE` cũng không xoá dữ liệu.

Sau khi chạy, Người 3 chỉ cần kết nối cùng cluster là có ngay
`spark.table("clean_nyc_taxi")` với 188 partition `year/month`.

Chạy (Python là bản trong .venv của mảnh 2, JDK 17, HADOOP_HOME=D:\\hadoop):

    $env:PYTHONIOENCODING = "utf-8"
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        D:\\BIGDATA\\1-warehouse\\src\\register_hive_tables.py --host 192.168.36.64

Tham số `--host` là IP đang thực tế của máy chạy cluster: Docker Desktop trên
Windows xuất cổng ra mọi interface, nên client ở ngoài mạng container phải dùng
đúng IP đó (xem ghi chú trong config/hdfs-site.xml).
"""

from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import SparkSession, functions as F

HDFS_TRIPS_ROOT = "/data/nyc_taxi/clean/trips"
TABLE_NAME = "clean_nyc_taxi"
PARTITION_COLUMNS = ("year", "month")
EXPECTED_PARTITIONS = 188
EXPECTED_ROWS = 1_506_791_871
SPARK_LOCAL_DIR = os.environ.get("SPARK_LOCAL_DIR", "D:/tmp/spark")
HADOOP_HOME = os.environ.get("HADOOP_HOME", "D:/hadoop").replace("\\", "/")


def build_spark(host: str, app_name: str) -> SparkSession:
    """Spark trên Windows làm driver, đứng ngoài mạng Docker nên mọi địa chỉ phải là IP thật."""
    hdfs_uri = f"hdfs://{host}:8020"
    builder = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.local.dir", SPARK_LOCAL_DIR)
        .config("spark.driver.memory", "3g")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{host}:9083")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        # NameNode trả về kèm ipAddr nội bộ của container DataNode (172.18.x) mà máy
        # Windows không định tuyến được; cờ này buộc client dùng hostname quảng bá.
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.dfs.replication", "1")
        .config("spark.hadoop.hadoop.home.dir", HADOOP_HOME)
        .config("spark.sql.warehouse.dir", f"{hdfs_uri}/opt/hive/data/warehouse")
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def registered_location(spark: SparkSession, qualified: str) -> str | None:
    """Location đang được ghi trong catalog, hoặc None nếu bảng chưa tồn tại."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {qualified}").collect()
    except Exception:
        return None
    found = [r[1] for r in rows if r[0] == "Location"]
    return found[0] if found else None


def main() -> int:
    parser = argparse.ArgumentParser(description="Tạo/nuôi bảng Hive clean_nyc_taxi nằm trên HDFS.")
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "192.168.36.64"),
                        help="IP thật của máy chạy cluster (nơi 8020/9083 mở ra host).")
    parser.add_argument("--skip-count", action="store_true",
                        help="Chỉ đăng ký + đối chiếu partition, không đếm dòng toàn bảng.")
    parser.add_argument("--database", default="default",
                        help="Database Hive chứa bảng; mặc định là default để spark.table('clean_nyc_taxi') chạy ngay.")
    args = parser.parse_args()

    hdfs_uri = f"hdfs://{args.host}:8020"
    location = f"{hdfs_uri}{HDFS_TRIPS_ROOT}"
    qualified = f"`{args.database}`.`{TABLE_NAME}`"
    started = time.monotonic()

    spark = build_spark(args.host, "register-hive-tables")

    # Đọc schema trực tiếp từ footer Parquet trên HDFS thay vì để Spark tự suy trong DDL:
    # CREATE TABLE ... PARTITIONED BY mà không liệt kê cột sẽ ăn ALL_PARTITION_COLUMNS_NOT_ALLOWED
    # vì year/month đã nằm sẵn trong schema suy ra từ tên thư mục.
    data = spark.read.parquet(location)
    data_fields = [f for f in data.schema.fields if f.name not in PARTITION_COLUMNS]
    column_ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in data_fields)

    old = registered_location(spark, qualified)
    if old and not old.startswith(hdfs_uri):
        print(f"[FAIL] {qualified} đã tồn tại và trỏ tới {old} — không phải cluster {hdfs_uri}.")
        print("       Máy vừa đổi IP: location của bảng và của TỪNG partition bị ghi chết trong")
        print("       catalog Derby, `CREATE TABLE IF NOT EXISTS` không sửa được (MSCK REPAIR cũng")
        print("       sẽ treo vì nó đi tìm địa chỉ cũ). Hồi phục bằng một lệnh:")
        print(r"         .\tools\Set-Host.ps1 -RebuildCatalog    # roi chay lai script nay")
        return 1

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            {column_ddl}
        )
        USING parquet
        PARTITIONED BY (year INT, month INT)
        LOCATION '{location}'
        """
    )
    spark.sql(f"MSCK REPAIR TABLE {qualified}")

    partition_count = spark.sql(f"SHOW PARTITIONS {qualified}").count()
    columns = len(spark.table(qualified).columns)
    where = spark.sql(f"DESCRIBE FORMATTED {qualified}").collect()
    table_location = [r[1] for r in where if r[0] == "Location"][0]
    print(f"[table]    {qualified}")
    print(f"[location] {table_location}")
    print(f"[schema]   {columns} cột = {len(data_fields)} cột dữ liệu + year + month")

    if columns != len(data_fields) + 2:
        # IF NOT EXISTS im lặng giữ định nghĩa cũ; đây là external table nên DROP TABLE không xoá dữ liệu.
        print(f"[FAIL] Bảng đã tồn tại với {columns} cột, khác schema trên HDFS ({len(data_fields) + 2}).")
        print(f"       Chạy: spark.sql('DROP TABLE {qualified}') rồi chạy lại script này.")
        return 1
    if not table_location.startswith(hdfs_uri):
        print(f"[FAIL] Location của bảng đang là {table_location} — khác cluster này ({hdfs_uri}).")
        print("       Đây là dấu hiệu máy vừa đổi IP: location bị ghi chết trong catalog Derby,")
        print("       `CREATE TABLE IF NOT EXISTS` không sửa được nó. Chạy:")
        print("         .\\tools\\Set-Host.ps1 -RebuildCatalog   (roi chay lại script nay)")
        return 1
    if partition_count != EXPECTED_PARTITIONS:
        print(f"[FAIL] partition = {partition_count}/{EXPECTED_PARTITIONS} -> cây dữ liệu trên HDFS còn thiếu")
        return 1
    print(f"[partitions] {partition_count}/{EXPECTED_PARTITIONS} -> OK  ({time.monotonic() - started:.0f} s)")

    sample = spark.sql(
        f"SELECT count(*) AS n FROM {qualified} WHERE year = 2024 AND month = 1"
    ).collect()[0]["n"]
    print(f"[count] 2024-01 = {sample:,}  (đọc từ footer Parquet, không quét file)")

    if not args.skip_count:
        total = spark.table(qualified).select(F.count(F.lit(1))).collect()[0][0]
        print(f"[count] toàn bảng = {total:,}  mốc bàn giao của mảnh 2 là {EXPECTED_ROWS:,}")
        if total != EXPECTED_ROWS:
            print("[FAIL] Số dòng không khớp warehouse_summary.md")
            return 1

    print(f"[done] {time.monotonic() - started:.0f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
