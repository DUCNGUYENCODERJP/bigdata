"""
modules/writers.py
------------------
Ghi dữ liệu kết quả ra Parquet và CSV trên HDFS / Local phục vụ phân hệ BI và Dashboard.
"""
import logging

import re
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

_METRICS_SCHEMA = StructType([
    StructField("model", StringType(),  nullable=False),
    StructField("rmse",  DoubleType(),  nullable=False),
    StructField("mae",   DoubleType(),  nullable=False),
    StructField("mape",  DoubleType(),  nullable=True),
])


def write_parquet(data: DataFrame, path: str, table_name: Optional[str] = None) -> None:
    """Write Parquet output and optionally register it as a Hive SQL table."""
    data.write.mode("overwrite").parquet(path)
    logging.info(f"  [writer] ✓ Parquet written → {path}")
    if table_name is not None:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise ValueError(f"Invalid Hive table name: {table_name!r}")
        location = path.replace("'", "''")
        data.sparkSession.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} USING PARQUET LOCATION '{location}'"
        )
        data.sparkSession.sql(f"ALTER TABLE {table_name} SET LOCATION '{location}'")
        data.sparkSession.catalog.refreshTable(table_name)
        logging.info(f"  [writer] ✓ Hive SQL table registered → {table_name}")


def write_metrics_csv(
    spark: SparkSession,
    metrics: List[Dict[str, Optional[float]]],
    path: str,
) -> None:
    """Ghi bảng đánh giá metrics ra CSV phân vùng đơn (coalesce 1) cho Power BI import."""
    rows = [
        (m["model"], float(m["rmse"]), float(m["mae"]),
         float(m["mape"]) if m["mape"] is not None else None)
        for m in metrics
    ]
    metrics_data = spark.createDataFrame(rows, schema=_METRICS_SCHEMA)
    (
        metrics_data.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(path)
    )
    logging.info(f"  [writer] ✓ Metrics CSV written → {path}")
    metrics_data.write.mode("overwrite").saveAsTable("default.metrics")
    logging.info("  [writer] ✓ Hive SQL table registered → default.metrics")
