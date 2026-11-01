from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from clean import average_speed_violation


def read_local_month_partition(
    spark: SparkSession, destination: str, month: str
) -> DataFrame:
    """Read only one Hive-style year/month partition, retaining partition columns."""
    year, month_number = (int(part) for part in month.split("-"))
    root = Path(destination)
    partition = root / f"year={year}" / f"month={month_number}"
    if not partition.is_dir():
        raise FileNotFoundError(f"Output partition not found: {partition}")
    return (
        spark.read.option("basePath", str(root))
        .parquet(str(partition))
        .filter((F.col("year") == year) & (F.col("month") == month_number))
    )


def verify_clean_partition(
    df: DataFrame,
    config: dict,
    month: str,
    expected_rows: int,
    expected_schema: DataFrame,
) -> dict[str, int]:
    """Verify row count, cleaning invariants, month range and exact output schema.

    Count cờ lấy từ chính partition trên đĩa, gộp một lượt scan để batch khỏi mở phiên Spark thứ hai.
    """
    cleaning = config["cleaning"]
    duration = F.col("trip_duration_min")
    year, month_number = (int(part) for part in month.split("-"))
    invalid = (
        (F.col("trip_distance").isNotNull() & (F.col("trip_distance") < cleaning["trip_distance_min_miles"]))
        | (F.col("trip_distance") > cleaning["trip_distance_max_miles"])
        | (duration <= cleaning["trip_duration_min_minutes"])
        | (duration > cleaning["trip_duration_max_minutes"])
        | (F.col("dropoff_datetime") <= F.col("pickup_datetime"))
        | (F.col("fare_amount") < 0)
        | (F.col("total_amount") < 0)
        # Đúng biểu thức rule average_speed đã dùng, chỉ đổi tên cột sang schema output.
        | average_speed_violation(cleaning, "pickup_datetime", "dropoff_datetime")
        | (F.col("year") != year)
        | (F.col("month") != month_number)
        | F.col("year").isNull()
        | F.col("month").isNull()
    )
    missing = (
        F.col("pickup_datetime").isNull()
        | F.col("dropoff_datetime").isNull()
        | F.col("pickup_location_id").isNull()
        | F.col("dropoff_location_id").isNull()
        | F.col("fare_amount").isNull()
    )
    schema = lambda frame: sorted(
        (field.name, field.dataType.simpleString()) for field in frame.schema.fields
    )
    schema_matches = schema(df) == schema(expected_schema)
    if not schema_matches:
        # Phải kiểm tra schema trước khi agg: partition thiếu cột cờ sẽ làm AnalysisException
        # thay vì báo sai lệch schema rõ ràng cho batch đánh dấu ghi lại.
        raise RuntimeError(
            f"Output verification failed for {month}: schema_matches=False, "
            f"columns={schema(df)}, expected={schema(expected_schema)}"
        )
    checks = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.coalesce(invalid, F.lit(False)), 1).otherwise(0)).alias("invalid"),
        F.sum(F.when(F.coalesce(missing, F.lit(False)), 1).otherwise(0)).alias("missing"),
        # Ba count cờ: batch ghi thẳng vào run_log.csv thay vì mở phiên Spark thứ hai.
        F.sum(F.when(F.col("is_distance_missing") == True, 1).otherwise(0)).alias("distance_missing"),
        F.sum(F.when(F.col("is_unknown_zone") == True, 1).otherwise(0)).alias("unknown_zone"),
        F.sum(F.when(F.col("is_short_trip") == True, 1).otherwise(0)).alias("short_trip"),
        F.min("pickup_datetime").alias("min_pickup"),
        F.max("pickup_datetime").alias("max_pickup"),
    ).first()
    rows = int(checks["rows"] or 0)
    invalid_count = int(checks["invalid"] or 0)
    missing_count = int(checks["missing"] or 0)
    range_matches = all(
        value is not None and value.year == year and value.month == month_number
        for value in (checks["min_pickup"], checks["max_pickup"])
    )
    if rows != expected_rows or invalid_count or missing_count or not range_matches:
        raise RuntimeError(
            f"Output verification failed for {month}: rows={rows} expected_rows={expected_rows}, "
            f"invalid={invalid_count}, missing={missing_count}, pickup_range_matches={range_matches}, "
            f"schema_matches={schema_matches}"
        )
    return {
        "rows": rows,
        "invalid": invalid_count,
        "missing": missing_count,
        "is_distance_missing": int(checks["distance_missing"] or 0),
        "is_unknown_zone": int(checks["unknown_zone"] or 0),
        "is_short_trip": int(checks["short_trip"] or 0),
    }
