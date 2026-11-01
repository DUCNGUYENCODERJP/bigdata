"""
writer.py
---------
Output writers for analytics results and model artifacts.

All writing is centralised here so that changing the output format
(e.g. switching from Parquet to ORC, or adding Hive table registration)
only requires editing this single file.

Functions
~~~~~~~~~
write_parquet(data, path)
    Overwrite a Parquet directory on HDFS / local.

write_metrics_csv(spark, metrics, path)
    Write a list of metric dicts to a single-partition CSV (for Excel /
    Power BI import convenience).
"""

from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


# ── Parquet writer ─────────────────────────────────────────────────────────────

def write_parquet(data: DataFrame, path: str) -> None:
    """Write *data* to *path* in Parquet format, overwriting any existing data.

    Parameters
    ----------
    data:
        DataFrame to persist.
    path:
        Target HDFS or local path, e.g.
        ``"hdfs:///user/hive/warehouse/nyc_taxi_analytics/predictions"``.
    """
    data.write.mode("overwrite").parquet(path)
    print(f"  [writer] ✓ Parquet written → {path}")


# ── Metrics CSV writer ─────────────────────────────────────────────────────────

#: Schema for the metrics CSV — fixed so Person 4 can always rely on the
#: same column names and types in Power BI.
_METRICS_SCHEMA = StructType([
    StructField("model", StringType(),  nullable=False),
    StructField("rmse",  DoubleType(),  nullable=False),
    StructField("mae",   DoubleType(),  nullable=False),
    StructField("mape",  DoubleType(),  nullable=True),   # NULL when no +ve actuals
])


def write_metrics_csv(
    spark: SparkSession,
    metrics: List[Dict[str, Optional[float]]],
    path: str,
) -> None:
    """Persist model evaluation metrics as a single-partition CSV.

    The CSV is written with a header row and coalesced to a single file
    for easy import into Excel or Power BI.

    Parameters
    ----------
    spark:
        Active SparkSession (required to create a DataFrame from Python
        objects).
    metrics:
        List of metric dicts as returned by
        :func:`evaluation.compute_metrics`.
    path:
        Target path, e.g.
        ``"hdfs:///user/hive/warehouse/nyc_taxi_analytics/metrics"``.
    """
    rows = [
        (m["model"], float(m["rmse"]), float(m["mae"]),
         float(m["mape"]) if m["mape"] is not None else None)
        for m in metrics
    ]
    (
        spark.createDataFrame(rows, schema=_METRICS_SCHEMA)
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(path)
    )
    print(f"  [writer] ✓ Metrics CSV written → {path}")
