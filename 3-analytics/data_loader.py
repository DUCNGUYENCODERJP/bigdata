"""
data_loader.py
--------------
Data loading and basic validation utilities for NYC Taxi trip data.

Responsibilities
~~~~~~~~~~~~~~~~
- Load the clean trip table (fare_amount >= 0, year >= 2011) from Hive.
- Load the raw trip table (all rows, including cancelled) from Hive.
- Validate that required columns are present before processing starts.

All heavy transformations live in analysis.py and feature_engineering.py.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ── Column requirements ────────────────────────────────────────────────────────

_REQUIRED_CLEAN: frozenset = frozenset({
    "PULocationID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
})

_REQUIRED_RAW: frozenset = frozenset({
    "PULocationID",
    "tpep_pickup_datetime",
    "fare_amount",
})


# ── Public API ─────────────────────────────────────────────────────────────────

def load_clean_trips(spark: SparkSession, table: str) -> DataFrame:
    """Load and lightly validate the cleaned trip table from Hive.

    The table is expected to contain only valid trips:
    - fare_amount >= 0 (cancelled / refund rows already excluded)
    - tpep_pickup_datetime year >= 2011

    Any remaining rows with NULL PULocationID or pickup timestamp are
    dropped here as a safety net.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table:
        Hive table name (e.g. "clean_nyc_taxi").

    Returns
    -------
    DataFrame
        Filtered DataFrame ready for analysis and ML.
    """
    df = spark.table(table)
    _assert_columns(df, _REQUIRED_CLEAN, table)

    return df.filter(
        F.col("PULocationID").isNotNull()
        & F.col("tpep_pickup_datetime").isNotNull()
    )


def load_raw_trips(spark: SparkSession, table: str) -> DataFrame:
    """Load the raw trip table from Hive — includes cancelled rows.

    This table is only read during the cancelled-trip analysis step to
    examine why a percentage of rows have fare_amount < 0.

    Parameters
    ----------
    spark:
        Active SparkSession.
    table:
        Hive table name (e.g. "raw_nyc_taxi").

    Returns
    -------
    DataFrame
        Raw DataFrame; no row filtering is applied.
    """
    df = spark.table(table)
    _assert_columns(df, _REQUIRED_RAW, table)
    return df


# ── Internal helpers ───────────────────────────────────────────────────────────

def _assert_columns(df: DataFrame, required: frozenset, table: str) -> None:
    """Raise ValueError if any required columns are missing from *df*."""
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(
            f"Table '{table}' is missing required columns: {', '.join(missing)}"
        )
