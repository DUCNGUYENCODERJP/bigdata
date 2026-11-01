"""Ghi Parquet local hoặc Hive theo cấu hình sink."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def write(df: DataFrame, cfg: dict[str, Any], spark: SparkSession) -> str:
    """Ghi cùng schema tới local Parquet hoặc bảng Hive partitioned."""
    sink = cfg["sink"]
    sink_type = sink["type"].lower()
    partition_columns = ["year", "month"]

    if sink_type == "local":
        destination = Path(cfg["paths"]["clean_dir"]) / "trips"
        # Bắt buộc dynamic: với cùng một thư mục gốc chứa nhiều tháng đã ghi trước đó,
        # static sẽ xóa toàn bộ cây output, chỉ giữ lại các partition vừa ghi.
        if spark.conf.get("spark.sql.sources.partitionOverwriteMode", "static").lower() != "dynamic":
            raise RuntimeError(
                "spark.sql.sources.partitionOverwriteMode phải bằng 'dynamic' trước khi ghi "
                "Parquet local; hiện là "
                f"{spark.conf.get('spark.sql.sources.partitionOverwriteMode', 'static')!r}."
            )
        (
            df.write.format(sink.get("local_format", "parquet"))
            .mode(sink.get("local_mode", "overwrite"))
            .partitionBy(*partition_columns)
            .save(str(destination))
        )
        return str(destination)

    if sink_type == "hive":
        table = sink["hive_table"]
        if "." not in table:
            raise ValueError("sink.hive_table phải có dạng database.table")
        database = table.rsplit(".", 1)[0]
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{database}`")
        (
            df.write.format(sink.get("hive_format", "parquet"))
            .mode(sink.get("hive_mode", "overwrite"))
            .partitionBy(*partition_columns)
            .saveAsTable(table)
        )
        return table

    raise ValueError(f"sink.type không hợp lệ: {sink_type}; dùng local hoặc hive")