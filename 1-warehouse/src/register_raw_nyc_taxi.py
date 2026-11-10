"""Đăng ký cây Parquet RAW của TLC trên HDFS thành bảng Hive `raw_nyc_taxi`.

Bảng này phục vụ đúng một nhu cầu của Người 3: phân tích chuyến bị huỷ
(`fare_amount < 0`) — những dòng mà mảnh 2 đã loại khỏi `clean_nyc_taxi`.
Vì vậy cần dữ liệu gốc, chưa lọc.

Script chỉ tạo **siêu dữ liệu** trong Hive Metastore. Bảng là external table
trỏ thẳng vào `/data/nyc_taxi/raw/trips`, không chép, không sửa, không xoá byte
dữ liệu nào; `DROP TABLE` cũng không xoá dữ liệu.

Vì sao không dùng `spark.read.option("mergeSchema", "true")`
-----------------------------------------------------------
Raw của TLC **đổi kiểu giữa các năm**: `PULocationID` là INT32 ở 43 tháng đầu rồi
INT64 ở 145 tháng sau, `RatecodeID`/`passenger_count` là INT64 rồi DOUBLE. Spark từ
chối hợp nhất hai loại sau (`CANNOT_MERGE_INCOMPATIBLE_DATA_TYPE`), nên schema của
bảng phải tự dựng từ manifest footer của mảnh 2 theo quy tắc rộng nhất, có đo
xác nhận bằng các lần đọc thử trực tiếp trên cluster:

| Khai báo trong DDL | File Parquet thật | Đọc được? |
|---|---|---|
| `bigint` | INT32 hoặc INT64 | **có** — lên nước đôi INT32 -> bigint an toàn |
| `double` | INT32 hoặc DOUBLE | **có** |
| `double` | INT64 | **KHÔNG** — lỗi khi đọc, không báo trước |
| `timestamp` | INT64 (logical TIMESTAMP) | có |
| `string` | BYTE_ARRAY | có |

`INT64` và `DOUBLE` cùng tên không có kiểu nào đọc được cả hai phía, nên hai cột
đó (`RatecodeID`, `passenger_count`) **bị loại khỏi bảng** và in ra cảnh báo.
Không mất gì cho Người 3: `data_loader.py` chỉ đòi `PULocationID`,
`tpep_pickup_datetime`, `fare_amount`, và cả ba đều còn.

Spark đọc cột Parquet **không phân biệt hoa/thường** (đã đo: khai báo `airport_fee`
đọc được cột `Airport_fee` trong file 2024-01, và ngược lại). TLC đổi cách viết từ
`airport_fee` sang `Airport_fee` từ 2023, nên các cột trùng tên sau khi bỏ qua
hoa/thường được **gộp làm một**, lấy cách viết của đa số tháng — nếu không DDL sẽ
khai báo hai cột cùng trỏ một dữ liệu.

Chạy (Python là bản trong .venv của mảnh 2, JDK 17, HADOOP_HOME=D:\\hadoop):

    $env:PYTHONIOENCODING = "utf-8"
    $env:JAVA_HOME = "C:\\Program Files\\Eclipse Adoptium\\jdk-17.0.15.6-hotspot"
    $env:HADOOP_HOME = "D:\\hadoop"
    D:\\BIGDATA\\2-processing\\.venv\\Scripts\\python.exe `
        D:\\BIGDATA\\1-warehouse\\src\\register_raw_nyc_taxi.py --host 192.168.100.19
"""

from __future__ import annotations
import logging

import argparse
import collections
import csv
import os
import sys
import tempfile
import time
from pathlib import Path

from pyspark.sql import SparkSession

HDFS_RAW_ROOT = "/data/nyc_taxi/raw/trips"
TABLE_NAME = "raw_nyc_taxi"
EXPECTED_PARTITIONS = 188
# Mốc của mảnh 2: dòng raw theo footer Parquet cua 188 thang
# (2-processing/reports/warehouse_summary.md, muc "Raw theo manifest").
EXPECTED_ROWS = 1_516_766_780
DEFAULT_MANIFEST = Path(__file__).resolve().parents[2] / "2-processing" / "reports" / "source_manifest.csv"
# Bon thang cuoi nam 2026 chua tai ve khi lap manifest.
LAST_MONTH = (2026, 8)
SPARK_LOCAL_DIR = os.environ.get("SPARK_LOCAL_DIR", str(Path(tempfile.gettempdir()) / "nyc-taxi-spark"))
HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop").replace("\\", "/")

# Kieu vat ly Parquet -> kieu Spark, dung cho truong hop khong doi duoc.
PHYSICAL_TO_SPARK = {"BYTE_ARRAY": "string", "BOOLEAN": "boolean", "INT32": "int",
                     "INT64": "bigint", "DOUBLE": "double", "FLOAT": "float",
                     "INT96": "timestamp"}
# Cot mang dau "_datetime" duoc ghi bang INT64 (logical TIMESTAMP), khong phai so dem.
DATETIME_SUFFIXES = ("_datetime", "_date", "_time")


def build_spark(host: str) -> SparkSession:
    """Spark trên Windows làm driver, đứng ngoài mạng Docker nên mọi địa chỉ phải là IP thật."""
    hdfs_uri = f"hdfs://{host}:8020"
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("register-raw-nyc-taxi")
        .config("spark.local.dir", SPARK_LOCAL_DIR)
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


def month_in_scope(month: str) -> bool:
    """Tháng thuộc phạm vi bàn giao 2011-01..2026-08 của mảnh 2."""
    try:
        year, mon = month.split("-")
        year, mon = int(year), int(mon)
    except ValueError:
        return False
    return (2011, 1) <= (year, mon) <= LAST_MONTH


def read_column_types(manifest_path: str):
    """Đọc manifest của mảnh 2: tên cột (bỏ qua hoa/thường) -> kiểu vật lý + cách viết.

    Trả về (bang_kieu, bang_cach_viet, so_thang, tong_dong_raw). Manifest ghi du duong
    dan duoc sinh tu footer that, nên không cần mở 188 file Parquet ở đây.
    """
    types: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    spellings: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    months: dict[str, dict] = {}
    with open(manifest_path, newline="", encoding="utf-8-sig") as handle:
        for row in csv.DictReader(handle):
            if not month_in_scope(row.get("month", "")) or not row.get("column_names"):
                continue
            months[row["month"]] = row
            names = row["column_names"].split("|")
            physical = row["physical_types"].split("|")
            if len(names) != len(physical):
                raise ValueError(f"Manifest lech cot/kiểu ở {row['month']}: "
                                 f"{len(names)} ten vs {len(physical)} kieu")
            for name, physical_type in zip(names, physical):
                key = name.lower()
                types[key][physical_type] += 1
                spellings[key][name] += 1
    rows = sum(int(r["row_count"]) for r in months.values())
    return types, spellings, len(months), rows


def merge_column(name: str, physical: collections.Counter) -> str | None:
    """Chọn kiểu rộng nhất đọc được mọi file; None nếu không có kiểu nào an toàn."""
    kinds = set(physical)
    if not kinds <= set(PHYSICAL_TO_SPARK):
        raise ValueError(f"{name}: kiểu Parquet lạ {kinds - set(PHYSICAL_TO_SPARK)}")
    # INT64 (so dem) va DOUBLE (so thuc) khong the cung ve mot kieu: double doc khong
    # duoc file INT64 (da do), bigint thi lam tron phan thap phan. Bo cot.
    if {"INT64", "DOUBLE"} <= kinds or {"INT64", "FLOAT"} <= kinds:
        return None
    if name.endswith(DATETIME_SUFFIXES):
        return "timestamp" if kinds == {"INT64"} or kinds == {"INT96"} else None
    if "INT96" in kinds and kinds != {"INT96"}:
        return None
    if "DOUBLE" in kinds or "FLOAT" in kinds:
        return "double"          # INT32 -> double da duoc do la doc duoc
    if "INT64" in kinds:
        return "bigint"          # INT32 -> bigint da do
    if "INT32" in kinds:
        return "int"
    if "BOOLEAN" in kinds:
        return "boolean" if kinds == {"BOOLEAN"} else None
    return "string"              # BYTE_ARRAY


def build_schema(types: dict, spellings: dict) -> tuple[list[tuple[str, str]], list[str]]:
    """Sinh (danh sach (ten, kieu) dua vao DDL, danh sach cot bi bo).

    `name` hien thi la cach viet cua da so thang, doi chieu kieu dung ten da bo qua
    hoa/thuong (da do: Spark doc cot Parquet khong phan biet hoa/thuong).
    """
    columns: list[tuple[str, str]] = []
    dropped: list[str] = []
    for key in sorted(types, key=str.lower):
        name = max(spellings[key], key=spellings[key].get)
        chosen = merge_column(key, types[key])
        if chosen is None:
            dropped.append(f"{name} {dict(types[key])}")
        else:
            columns.append((name, chosen))
    return columns, dropped


def registered_location(spark: SparkSession, qualified: str) -> str | None:
    """Location đang được ghi trong catalog, hoặc None nếu bảng chưa tồn tại."""
    try:
        rows = spark.sql(f"DESCRIBE FORMATTED {qualified}").collect()
    except Exception:
        return None
    found = [r[1] for r in rows if r[0] == "Location"]
    return found[0] if found else None


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Tạo bảng Hive {TABLE_NAME} trên HDFS.")
    parser.add_argument("--host", default=os.environ.get("WAREHOUSE_HOST", "192.168.100.17"),
                        help="IP thật của máy chạy cluster (nơi 8020/9083 mở ra host).")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST,
                        help="source_manifest.csv của mảnh 2 — nguồn schema union, không mở file Parquet.")
    parser.add_argument("--skip-count", action="store_true",
                        help="Chỉ đăng ký + đối chiếu partition, không đếm dòng toàn bảng.")
    parser.add_argument("--database", default="default")
    args = parser.parse_args()

    hdfs_uri = f"hdfs://{args.host}:8020"
    location = f"{hdfs_uri}{HDFS_RAW_ROOT}"
    qualified = f"`{args.database}`.`{TABLE_NAME}`"
    started = time.monotonic()

    types, spellings, months_count, manifest_rows = read_column_types(str(args.manifest))
    logging.info(f"[manifest] {months_count} thang, {manifest_rows:,} dong raw tong hop duoc tu footer")
    if months_count != EXPECTED_PARTITIONS:
        logging.error(f"[FAIL] manifest chi co {months_count}/{EXPECTED_PARTITIONS} thang trong pham vi")
        return 1

    columns, dropped = build_schema(types, spellings)
    logging.info(f"[schema]   {len(columns)} cột đưa vào bảng: "
          + ", ".join(f"{n}:{t}" for n, t in columns))
    if dropped:
        logging.info(f"[schema]   BO {len(dropped)} cot vi kieu doi giua cac nam, khong co kieu nao doc "
              f"duoc ca hai phia: {dropped}")

    spark = build_spark(args.host)
    old = registered_location(spark, qualified)
    if old and not old.startswith(hdfs_uri):
        logging.error(f"[FAIL] {qualified} đã tồn tại và trỏ tới {old} — không phải cluster {hdfs_uri}.")
        logging.info("       Máy vừa đổi IP; location bị ghi chết trong catalog Derby. Hồi phục:")
        logging.info(r"         .\tools\Set-Host.ps1 -RebuildCatalog    # roi chay lai script nay")
        return 1

    column_ddl = ", ".join(f"`{name}` {spark_type}" for name, spark_type in columns)
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
    registered = spark.table(qualified).columns
    props = spark.sql(f"DESCRIBE FORMATTED {qualified}").collect()
    table_location = [r[1] for r in props if r[0] == "Location"][0]

    logging.info(f"[table]    {qualified}")
    logging.info(f"[location] {table_location}")
    logging.info(f"[columns]  {len(registered)} cột đăng ký")

    if partition_count != EXPECTED_PARTITIONS:
        logging.error(f"[FAIL] partition = {partition_count}/{EXPECTED_PARTITIONS} -> cây raw trên HDFS còn thiếu")
        return 1
    if not table_location.startswith(hdfs_uri):
        logging.error(f"[FAIL] Location không nằm trên HDFS của cluster này: {table_location}")
        return 1
    logging.info(f"[partitions] {partition_count}/{EXPECTED_PARTITIONS} -> OK  ({time.monotonic() - started:.0f} s)")

    sample = spark.sql(f"SELECT count(*) AS n FROM {qualified} WHERE year = 2024 AND month = 1").collect()[0]["n"]
    logging.info(f"[count] 2024-01 = {sample:,}  (mảnh 2 đo raw tháng này là 2.964.624)")

    # Nam duoi day moi la bang chung that: mot scan toan bang buoc Spark doc TRA 188
    # file Parquet bang dung schema khai bao. Chi can mot partition lech kieu la ca
    # job chet, nen day la lieu kiem duy nhat co y nghia voi bang 19,5 GiB nay.
    if not args.skip_count:
        probe = spark.sql(
            f"""SELECT count(*) AS n, min(tpep_pickup_datetime) AS lo,
                       max(tpep_pickup_datetime) AS hi, count(PULocationID) AS co_id,
                       sum(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS am
                FROM {qualified}"""
        ).collect()[0]
        total = int(probe["n"])
        logging.info(f"[scan] 188 partition doc hết: {probe['lo']} .. {probe['hi']}, "
              f"PULocationID khong null = {probe['co_id']:,}, fare_amount<0 = {probe['am']:,}")
        if probe["co_id"] != manifest_rows:
            logging.warning(f"[WARN] số dòng có PULocationID ({probe['co_id']:,}) khác tổng raw trong manifest "
                  f"({manifest_rows:,}) — manifest tính cả dòng thiếu zone id, đây là bình thường")
        logging.info(f"[count] toàn bảng = {total:,}  mốc raw của mảnh 2 là {EXPECTED_ROWS:,}")
        if total != EXPECTED_ROWS:
            logging.error(f"[FAIL] lệch {total - EXPECTED_ROWS:+,} dòng so với warehouse_summary.md")
            return 1

    logging.info(f"[done] {time.monotonic() - started:.0f} s")
    spark.stop()
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
