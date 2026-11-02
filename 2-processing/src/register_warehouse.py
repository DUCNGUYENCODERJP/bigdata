"""Đăng ký cây Parquet 188 partition sẵn có thành bảng Hive trên máy local.

Không ghi đè, không chép, không xoá byte nào: `LOCATION` trỏ bảng vào đúng
`paths.clean_dir/trips` nên đây là external table - `DROP TABLE` cũng không xoá dữ liệu.
Metastore là Derby nhúng của Spark, tự tạo thư mục `metastore_db` ở thư mục chạy lệnh
(đã nằm trong `.gitignore`), vì vậy phải chạy từ thư mục dự án.

Chạy:
    Set-Location D:\\BIGDATA\\2-processing
    .\\.venv\\Scripts\\python.exe src\\register_warehouse.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from pipeline import load_config
from quality_check import set_runtime_environment


def create_hive_spark(config, warehouse_dir: Path) -> SparkSession:
    """Nhúng bản sao tạo Spark của pipeline, chỉ khác là bật Hive support vô điều kiện."""
    set_runtime_environment(config)
    builder = (
        SparkSession.builder.master(config["runtime"]["spark_master"])
        .appName("register-warehouse")
        .config("spark.local.dir", config["paths"]["spark_local_dir"])
        .config("spark.driver.memory", str(config["runtime"]["driver_memory"]))
        .config("spark.hadoop.hadoop.home.dir", config["hadoop_home"])
        .config("spark.sql.session.timeZone", config["runtime"].get("timezone", "America/New_York"))
        .config("spark.sql.warehouse.dir", warehouse_dir.as_uri())
        .enableHiveSupport()
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> int:
    parser = argparse.ArgumentParser(description="Đăng ký data/clean/trips thành bảng Hive local.")
    parser.add_argument("--skip-count", action="store_true", help="Bỏ bước đếm toàn bảng khi chỉ cần đăng ký lại.")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    config = load_config()
    data_dir = Path(config["paths"]["clean_dir"])
    location = data_dir / "trips"
    if not location.is_dir():
        print(f"[FAIL] Không thấy cây partition Parquet tại: {location}")
        return 1

    database, _, table = config["sink"]["hive_table"].partition(".")
    qualified = f"`{database}`.`{table}`"
    spark = create_hive_spark(config, project_root / "data" / "spark-warehouse")
    started = time.monotonic()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")

    # Đọc schema thật từ footer Parquet thay vì để Spark tự suy trong DDL: CREATE TABLE
    # ... PARTITIONED BY ... LOCATION mà không liệt kê cột sẽ ăn ALL_PARTITION_COLUMNS_NOT_ALLOWED
    # vì năm/tháng đã nằm sẵn trong schema suy ra từ tên thư mục.
    data_fields = [
        field
        for field in spark.read.parquet(str(location)).schema.fields
        if field.name not in ("year", "month")
    ]
    column_ddl = ", ".join(f"`{field.name}` {field.dataType.simpleString()}" for field in data_fields)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified} (
            {column_ddl}
        )
        USING parquet
        PARTITIONED BY (year INT, month INT)
        LOCATION '{location.as_uri()}'
        """
    )
    spark.sql(f"MSCK REPAIR TABLE {qualified}")

    partitions = spark.sql(f"SHOW PARTITIONS {qualified}")
    partition_count = partitions.count()
    columns = len(spark.table(qualified).columns)
    print(f"[register] {qualified}  location={location.as_uri()}")
    print(f"[schema]   {columns} cột = {len(data_fields)} cột dữ liệu + year + month")
    if columns != len(data_fields) + 2:
        # IF NOT EXISTS im lặng giữ định nghĩa cũ; DROP TABLE không xoá dữ liệu vì là external.
        print(f"[FAIL] Bảng đã tồn tại với {columns} cột, khác schema trên đĩa ({len(data_fields) + 2}).")
        print(f"       Chạy: spark.sql('DROP TABLE {qualified}') rồi chạy lại script này.")
        return 1

    expected = 188
    status = "OK" if partition_count == expected else "KHỚT"
    print(f"[partitions] {partition_count}/{expected} -> {status}  ({time.monotonic() - started:.0f} s)")

    if not args.skip_count:
        sample = spark.sql(
            f"SELECT count(*) AS n FROM {qualified} WHERE year = 2024 AND month = 1"
        ).collect()[0]["n"]
        print(f"[count] 2024-01 = {sample:,}  (đọc từ footer Parquet, không quét file)")
        total = spark.table(qualified).select(F.count(F.lit(1))).collect()[0][0]
        print(f"[count] toàn bảng = {total:,}  bằng chứng bàn giao là 1.506.791.871")
        if total != 1_506_791_871:
            print("[FAIL] Số dòng không khớp warehouse_summary.md")
            return 1

    print(f"[done] {time.monotonic() - started:.0f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
