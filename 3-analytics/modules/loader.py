"""
modules/loader.py
-----------------
Nạp và kiểm tra sơ bộ schema bảng sạch và bảng raw từ Hive Warehouse.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import MIN_DATA_YEAR
from modules.period_bounds import latest_partition_year

_REQUIRED_CLEAN: frozenset = frozenset({
    "PULocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "year",
})

_REQUIRED_RAW: frozenset = frozenset({
    "PULocationID",
    "tpep_pickup_datetime",
    "fare_amount",
    "year",
})


def _assert_columns(df: DataFrame, required: frozenset, table: str) -> None:
    """Báo lỗi nếu bảng Hive thiếu các cột bắt buộc."""
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(
            f"Table '{table}' is missing required columns: {', '.join(missing)}"
        )


def load_clean_trips(spark: SparkSession, table: str) -> DataFrame:
    """Load clean trips from 2011 through the newest available year partition."""
    df = spark.table(table)
    _assert_columns(df, _REQUIRED_CLEAN, table)
    latest_year = latest_partition_year(df)
    if latest_year is None:
        raise ValueError(f"Table '{table}' has no populated year partition.")

    return df.filter(
        F.col("PULocationID").isNotNull()
        & F.col("tpep_pickup_datetime").isNotNull()
        & F.col("year").cast("int").between(MIN_DATA_YEAR, latest_year)
        & F.year("tpep_pickup_datetime").between(MIN_DATA_YEAR, latest_year)
    )


def load_raw_trips(spark: SparkSession, table: str) -> DataFrame:
    """Nạp trực tiếp bảng raw từ Hive SQL, giữ nguyên các dòng fare âm."""
    df = spark.table(table)
    _assert_columns(df, _REQUIRED_RAW, table)
    return df
