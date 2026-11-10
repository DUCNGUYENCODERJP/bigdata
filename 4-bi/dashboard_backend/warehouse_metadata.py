"""Partition discovery, validation, and warehouse metadata helpers."""
from __future__ import annotations
import logging

import os
import re

from .context import PROJECT_ROOT


def _quote_table_name(table_name: str) -> str:
    parts = table_name.split(".")
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts):
        raise ValueError(f"Tên bảng Hive không hợp lệ: {table_name!r}")
    return ".".join(f"`{part}`" for part in parts)



def _parse_partition(partition: str) -> tuple[int, int] | None:
    if partition.lower() == "all":
        return None
    match = re.fullmatch(r"year=(\d{4})/month=(\d{1,2})", partition)
    if not match:
        raise ValueError("Phạm vi phải là 'all' hoặc có dạng year=2024/month=1.")
    year, month = (int(value) for value in match.groups())
    if not 1 <= month <= 12:
        raise ValueError(f"Tháng không hợp lệ trong partition: {month}.")
    return year, month



def _partition_month(value: str) -> str | None:
    match = re.fullmatch(r"year=(\d{4})/month=(\d{1,2})", value.strip())
    if not match:
        return None
    year, month = (int(part) for part in match.groups())
    if not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"



def _show_partition_months(spark, table_name: str) -> set[str]:
    source = _quote_table_name(table_name)
    rows = spark.sql(f"SHOW PARTITIONS {source}").collect()
    return {
        month
        for row in rows
        if (month := _partition_month(str(row[0]))) is not None
    }



def discover_warehouse_state(
    warehouse_host: str,
    clean_table: str,
    raw_table: str,
) -> dict:
    """Read only Hive/HDFS metadata; no Parquet rows are scanned."""
    from pyspark.sql import SparkSession

    hdfs_uri = f"hdfs://{warehouse_host}:8020"
    builder = (
        SparkSession.builder.appName("NYC_Taxi_BI_Cache_Watcher")
        .master("local[1]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{warehouse_host}:9083")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.ipc.client.connect.timeout", "10000")
        .config("spark.hadoop.ipc.client.connect.max.retries", "1")
        .config("spark.hadoop.ipc.client.connect.retry.interval", "1000")
        .config("spark.sql.warehouse.dir", f"{hdfs_uri}/opt/hive/data/warehouse")
    )
    hosts_candidates = [
        os.path.join(os.environ["HADOOP_HOME"], "custom_hosts") if os.environ.get("HADOOP_HOME") else "",
        os.path.join(PROJECT_ROOT, "custom_hosts"),
    ]
    hosts_file = next(
        (path for path in hosts_candidates if path and os.path.isfile(path) and " " not in path),
        "",
    )
    if hosts_file:
        hosts_option = f"-Djdk.net.hosts.file={hosts_file.replace(os.sep, '/')}"
        java_options = os.environ.get("_JAVA_OPTIONS", "")
        if hosts_option not in java_options.split():
            os.environ["_JAVA_OPTIONS"] = f"{java_options} {hosts_option}".strip()
        builder = builder.config("spark.executor.extraJavaOptions", hosts_option)

    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        clean_candidates = []
        clean_db, separator, clean_name = clean_table.rpartition(".")
        if separator and clean_name.endswith("_tlc"):
            clean_candidates.append(f"{clean_db}.{clean_name[:-4]}")
        clean_candidates.append(clean_table)

        # Register directories written since the last poll. This updates Hive metadata only.
        repair_candidates = [candidate for candidate in clean_candidates if not candidate.endswith("_tlc")]
        repair_candidates.append(raw_table)
        for candidate in repair_candidates:
            try:
                spark.sql(f"MSCK REPAIR TABLE {_quote_table_name(candidate)}")
            except Exception as error:
                logging.info(f"[CACHE] Hive partition repair skipped for {candidate}: {error}")

        clean_months: set[str] = set()
        errors = []
        for candidate in clean_candidates:
            try:
                clean_months = _show_partition_months(spark, candidate)
                if clean_months:
                    break
            except Exception as error:
                errors.append(f"{candidate}: {error}")
        if not clean_months:
            raise RuntimeError(
                "Không lấy được partition metadata của bảng clean. "
                + " | ".join(errors)
            )

        raw_months = _show_partition_months(spark, raw_table)
        storage_bytes = None
        storage_files = None
        try:
            clean_hdfs_dir = f"{hdfs_uri}/data/nyc_taxi/clean/trips"
            path = spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path(clean_hdfs_dir)
            fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
            summary = fs.getContentSummary(path)
            storage_bytes = int(summary.getLength())
            storage_files = int(summary.getFileCount())
        except Exception as error:
            logging.info(f"[CACHE] Không lấy được HDFS content summary: {error}")
        return {
            "partitions": sorted(clean_months & raw_months),
            "storage_bytes": storage_bytes,
            "storage_files": storage_files,
        }
    finally:
        spark.stop()



def _format_size(byte_count: int) -> str:
    size = float(byte_count)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PiB"



def _month_sort_key(month: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", month)
    if not match:
        raise ValueError(f"Tháng không hợp lệ: {month!r}")
    return int(match.group(1)), int(match.group(2))



def _month_range(first: str, last: str) -> list[str]:
    year, month = _month_sort_key(first)
    last_year, last_month = _month_sort_key(last)
    result = []
    while (year, month) <= (last_year, last_month):
        result.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1
    return result
