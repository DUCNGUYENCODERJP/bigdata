"""Resolve temporal bounds from the partition metadata present in a DataFrame."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def latest_partition_year(data: DataFrame) -> int | None:
    """Return the newest available `year` partition value, without a fixed cap."""
    if "year" not in data.columns:
        return None
    row = data.agg(F.max(F.col("year").cast("int")).alias("latest_year")).first()
    value = row["latest_year"] if row else None
    return int(value) if value is not None else None
