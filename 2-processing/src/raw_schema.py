"""Fixed, case-insensitive schema for raw Yellow Taxi month files."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampNTZType,
)


RAW_SCHEMA = StructType(
    [
        StructField("tpep_pickup_datetime", TimestampNTZType(), True),
        StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
        StructField("passenger_count", LongType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        # airport_fee is retained for fixed-schema consistency across months.
        StructField("airport_fee", DoubleType(), True),
    ]
)

PIPELINE_RAW_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
    "tip_amount",
    "tolls_amount",
]

# Fields required to execute cleaning rules, derive duration, and join zones.
# passenger_count, tip_amount, tolls_amount, and airport_fee can be absent and
# are then represented as typed nulls.
REQUIRED_RAW_COLUMNS = (
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
)


def raw_columns_ignored(df: DataFrame) -> list[str]:
    """List source fields outside the supported canonical raw schema."""
    canonical = {field.name.casefold() for field in RAW_SCHEMA.fields}
    return [column for column in df.columns if column.casefold() not in canonical]


def standardize_raw_schema(df: DataFrame, *, context: str | None = None) -> DataFrame:
    """Select canonical raw fields, cast types, and create optional fields as null.

    Passenger counts are kept only when they are finite, integral, and fit in a
    signed bigint. Fractional values, NaN, infinities, out-of-range values, and
    nulls become null; zero remains zero for the cleaner's existing rule.
    """
    actual_by_name: dict[str, list[str]] = {}
    for column in df.columns:
        actual_by_name.setdefault(column.casefold(), []).append(column)

    expressions = []
    for field in RAW_SCHEMA.fields:
        matches = actual_by_name.get(field.name.casefold(), [])
        if len(matches) > 1:
            raise ValueError(
                f"Ambiguous case-insensitive raw column {field.name}: {matches}"
            )
        if matches:
            source = F.col(matches[0])
            if field.name == "passenger_count":
                numeric = source.cast(DoubleType())
                valid = (
                    numeric.isNotNull()
                    & ~F.isnan(numeric)
                    & (numeric >= F.lit(float(-(2**63))))
                    & (numeric < F.lit(float(2**63)))
                    & (numeric == F.floor(numeric))
                )
                value = F.when(valid, numeric.cast(LongType())).otherwise(
                    F.lit(None).cast(LongType())
                )
                expressions.append(value.alias(field.name))
            else:
                expressions.append(source.cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    missing_required = [
        name
        for name in REQUIRED_RAW_COLUMNS
        if not actual_by_name.get(name.casefold())
    ]
    if missing_required:
        where = f" for {context}" if context else ""
        raise ValueError(
            f"Missing required raw columns{where}: {missing_required}"
        )
    return df.select(*expressions)
