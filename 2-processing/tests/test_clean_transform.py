from __future__ import annotations

import sys
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    TimestampNTZType,
)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.clean import _rule_condition, clean, effective_max_drop_rate
from src.quality_check import load_config, set_runtime_environment
from src.raw_schema import RAW_SCHEMA, raw_columns_ignored, standardize_raw_schema
from src.transform import transform
from src.writer import write
from src.output_validation import read_local_month_partition, verify_clean_partition

set_runtime_environment(load_config(Path(__file__).resolve().parents[1] / "config.yaml"))
TEST_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
TEST_SPARK_LOCAL_DIR = str(Path("D:/tmp/spark/pytest"))
Path(TEST_SPARK_LOCAL_DIR).mkdir(parents=True, exist_ok=True)
Path(TEST_SPARK_LOCAL_DIR).mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-clean-transform")
        .config("spark.local.dir", TEST_SPARK_LOCAL_DIR)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def config():
    """Config khớp từng giá trị với config.yaml production."""
    production = load_config(TEST_CONFIG_PATH)
    assert production["cleaning"]["trip_duration_min_minutes"] == 0
    assert production["cleaning"]["short_trip_max_minutes"] == 1
    return {
        "processing_month": "2024-01",
        "cleaning": production["cleaning"],
        "transform": production["transform"],
    }


def _trip_values(pickup=None, dropoff=None, **overrides):
    pickup = pickup or datetime(2024, 1, 15, 10, 0)
    values = {
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": dropoff or pickup + timedelta(minutes=10),
        "passenger_count": 1,
        "trip_distance": 2.0,
        "PULocationID": 1,
        "DOLocationID": 2,
        "fare_amount": 10.0,
        "total_amount": 12.0,
        "tip_amount": 1.0,
        "tolls_amount": 0.0,
    }
    values.update(overrides)
    return values


def _sql_trip_row(values):
    def sql_value(name, value, sql_type):
        if value is None:
            return f"CAST(NULL AS {sql_type}) AS `{name}`"
        if isinstance(value, datetime):
            return f"TIMESTAMP_NTZ '{value:%Y-%m-%d %H:%M:%S}' AS `{name}`"
        if isinstance(value, str):
            return f"'{value}' AS `{name}`"
        return f"CAST({value} AS {sql_type}) AS `{name}`"

    types = {
        "tpep_pickup_datetime": "TIMESTAMP_NTZ",
        "tpep_dropoff_datetime": "TIMESTAMP_NTZ",
        "passenger_count": "BIGINT",
        "trip_distance": "DOUBLE",
        "PULocationID": "INT",
        "DOLocationID": "INT",
        "fare_amount": "DOUBLE",
        "total_amount": "DOUBLE",
        "tip_amount": "DOUBLE",
        "tolls_amount": "DOUBLE",
    }
    return "SELECT " + ", ".join(
        sql_value(name, value, types[name]) for name, value in values.items()
    )


def _sequential_reference(frame, config):
    """Test-only version of the original ordered, one-rule-at-a-time cleaner."""
    month = config["processing_month"]
    current = frame
    raw_rows = current.count()
    stats = []
    for rule in config["cleaning"]["rule_order"]:
        rows_before = current.count()
        if rule == "duplicate":
            following = current.dropDuplicates()
            removed = rows_before - following.count()
            set_null = 0
        else:
            condition = F.coalesce(
                _rule_condition(current, rule, config, month), F.lit(False)
            )
            removed = current.filter(condition).count()
            following = current.filter(~condition)
            set_null = 0
            if rule == "trip_distance":
                zero = F.coalesce(F.col("trip_distance") == 0, F.lit(False))
                set_null = following.filter(zero).count()
                following = following.withColumn("is_distance_missing", zero).withColumn(
                    "trip_distance",
                    F.when(zero, F.lit(None).cast("double")).otherwise(
                        F.col("trip_distance")
                    ),
                )
        stats.append({
            "rule": rule,
            "rows_before": rows_before,
            "removed": removed,
            "distance_set_null": set_null,
            "rows_after": rows_before - removed,
        })
        current = following

    current = current.withColumn(
        "passenger_count",
        F.when(F.col("passenger_count") == 0, F.lit(None)).otherwise(
            F.col("passenger_count")
        ),
    )
    return current, {
        "raw_rows": raw_rows,
        "rules": stats,
        "clean_rows": current.count(),
        "total_removed": raw_rows - current.count(),
    }


def test_clean_counts_each_reason_without_overlap_and_keeps_nullable_passenger(spark, config):
    valid = _trip_values()
    rows = [
        valid,
        valid,
        _trip_values(trip_distance=0.0),
        _trip_values(trip_distance=101.0),
        _trip_values(dropoff=datetime(2024, 1, 15, 10, 0, 30)),
        _trip_values(dropoff=datetime(2024, 1, 15, 13, 1)),
        _trip_values(fare_amount=-1.0),
        _trip_values(total_amount=-1.0),
        _trip_values(trip_distance=10.0, dropoff=datetime(2024, 1, 15, 10, 2)),
        _trip_values(pickup=datetime(2024, 2, 1, 10, 0)),
        _trip_values(passenger_count=0),
        _trip_values(passenger_count=None),
        _trip_values(fare_amount=None),
    ]
    frame = spark.sql(" UNION ALL ".join(_sql_trip_row(row) for row in rows))

    cleaned, stats = clean(frame, config)

    # Với ngưỡng mới: 30 giây và 1 phút 30 giây được giữ (chỉ <= 0 hoặc > 180 phút bị loại),
    # còn chuyến 10 dặm / 2 phút vẫn bị loại vì vượt mốc tốc độ.
    assert stats["raw_rows"] == len(rows)
    assert stats["clean_rows"] == 5
    assert stats["total_removed"] == len(rows) - 5
    assert [rule["removed"] for rule in stats["rules"]] == [
        1, 1, 1, 1, 1, 1, 1, 1
    ]
    assert stats["rules"][1]["distance_set_null"] == 1
    assert stats["drop_rate"] == pytest.approx((len(rows) - 5) / len(rows))
    zero_distance = cleaned.filter("is_distance_missing").select("trip_distance").first()
    assert zero_distance.trip_distance is None
    passengers = [row.passenger_count for row in cleaned.select("passenger_count").collect()]
    # 2 dòng giữ lại có passenger_count null: một dòng 0 chuyển thành null, một dòng null sẵn.
    assert passengers.count(None) == 2
    cleaned.unpersist()


def test_transform_adds_calendar_and_zone_fields_with_snake_case(spark, config):
    pickup = datetime(2024, 1, 15, 10, 0)
    trip = _trip_values(pickup=pickup, dropoff=pickup + timedelta(minutes=12))
    frame = spark.sql(_sql_trip_row(trip))
    zones = spark.sql(
        "SELECT 1 AS LocationID, 'Manhattan' AS Borough, 'Midtown North' AS Zone "
        "UNION ALL SELECT 2, 'Queens', 'JFK Airport'"
    )

    result = transform(frame, zones, config)
    row = result.first()

    assert (row.year, row.month, row.day, row.hour, row.day_of_week) == (2024, 1, 15, 10, 2)
    assert row.pickup_date.isoformat() == "2024-01-15"
    assert row.pickup_hour_ts == datetime(2024, 1, 15, 10, 0)
    assert row.is_unknown_zone is False
    assert row.trip_duration_min == pytest.approx(12.0)
    assert row.is_short_trip is False
    assert (row.pickup_borough, row.pickup_zone) == ("Manhattan", "Midtown North")
    assert (row.dropoff_borough, row.dropoff_zone) == ("Queens", "JFK Airport")
    assert "PULocationID" not in result.columns
    assert "passenger_count" in result.columns


def test_transform_flags_short_trip_below_threshold(spark, config):
    pickup = datetime(2024, 1, 15, 10, 0)
    rows = [
        _trip_values(pickup=pickup, dropoff=pickup + timedelta(seconds=40)),
        _trip_values(pickup=pickup, dropoff=pickup + timedelta(minutes=1)),
        _trip_values(pickup=pickup, dropoff=pickup + timedelta(minutes=10)),
    ]
    frame = spark.sql(" UNION ALL ".join(_sql_trip_row(row) for row in rows))
    zones = spark.sql(
        "SELECT 1 AS LocationID, 'Manhattan' AS Borough, 'Zone A' AS Zone "
        "UNION ALL SELECT 2, 'Queens', 'Zone B'"
    )

    flags = [
        row.is_short_trip
        for row in transform(frame, zones, config).orderBy("trip_duration_min").collect()
    ]

    # Chỉ chuyến dưới 1 phút bị gắn cờ; 1 phút trở lên không bị cờ.
    assert flags == [True, False, False]


def test_standard_raw_schema_is_case_insensitive_and_fills_missing_columns(spark):
    source = spark.sql(
        "SELECT TIMESTAMP_NTZ '2024-01-15 10:25:00' AS Tpep_pickup_datetime, "
        "TIMESTAMP_NTZ '2024-01-15 10:35:00' AS TPEP_DROPOFF_DATETIME, "
        "CAST(1 AS BIGINT) AS PASSENGER_COUNT, CAST(2.5 AS DOUBLE) AS TRIP_DISTANCE, "
        "CAST(264 AS INT) AS pulocationid, CAST(2 AS INT) AS DOLocationID, "
        "CAST(10.0 AS DOUBLE) AS Fare_Amount, CAST(12.0 AS DOUBLE) AS TOTAL_AMOUNT, "
        "CAST(1.25 AS DOUBLE) AS Airport_fee, CAST(99 AS INT) AS VendorID"
    )

    result = standardize_raw_schema(source)
    row = result.first()

    assert [field.name for field in result.schema.fields] == [
        field.name for field in RAW_SCHEMA.fields
    ]
    assert [field.dataType.simpleString() for field in result.schema.fields] == [
        field.dataType.simpleString() for field in RAW_SCHEMA.fields
    ]
    assert row.tpep_pickup_datetime == datetime(2024, 1, 15, 10, 25)
    assert row.PULocationID == 264
    assert row.airport_fee == pytest.approx(1.25)
    assert "cbd_congestion_fee" not in result.columns
    assert row.tip_amount is None
    assert raw_columns_ignored(source) == ["VendorID"]


def test_standard_raw_schema_casts_legacy_numeric_types_and_case(spark):
    source = spark.sql(
        "SELECT TIMESTAMP_NTZ '2011-06-01 10:00:00' AS TPEP_PICKUP_DATETIME, "
        "TIMESTAMP_NTZ '2011-06-01 10:10:00' AS TPEP_DROPOFF_DATETIME, "
        "CAST(1.5 AS DOUBLE) AS Passenger_Count, CAST(2.5 AS DOUBLE) AS TRIP_DISTANCE, "
        "CAST(264 AS BIGINT) AS pulocationid, CAST(2 AS BIGINT) AS DOLocationID, "
        "CAST(10 AS DOUBLE) AS FARE_AMOUNT, CAST(12 AS DOUBLE) AS TOTAL_AMOUNT, "
        "CAST(1 AS INT) AS Airport_Fee, CAST(7 AS INT) AS congestion_surcharge, "
        "CAST('ignored' AS STRING) AS request_source "
        "UNION ALL SELECT TIMESTAMP_NTZ '2011-06-01 10:00:00', "
        "TIMESTAMP_NTZ '2011-06-01 10:10:00', CAST('NaN' AS DOUBLE), 2.5, "
        "CAST(264 AS BIGINT), CAST(2 AS BIGINT), 10.0, 12.0, CAST(1 AS INT), "
        "CAST(7 AS INT), CAST('ignored' AS STRING) "
        "UNION ALL SELECT TIMESTAMP_NTZ '2011-06-01 10:00:00', "
        "TIMESTAMP_NTZ '2011-06-01 10:10:00', CAST(NULL AS DOUBLE), 2.5, "
        "CAST(264 AS BIGINT), CAST(2 AS BIGINT), 10.0, 12.0, CAST(1 AS INT), "
        "CAST(7 AS INT), CAST('ignored' AS STRING) "
        "UNION ALL SELECT TIMESTAMP_NTZ '2011-06-01 10:00:00', "
        "TIMESTAMP_NTZ '2011-06-01 10:10:00', CAST(2.0 AS DOUBLE), 2.5, "
        "CAST(264 AS BIGINT), CAST(2 AS BIGINT), 10.0, 12.0, CAST(1 AS INT), "
        "CAST(7 AS INT), CAST('ignored' AS STRING)"
    )

    result = standardize_raw_schema(source, context="2011-06")
    rows = result.orderBy("passenger_count").collect()

    assert [row.passenger_count for row in rows] == [None, None, None, 2]
    assert result.schema["PULocationID"].dataType == IntegerType()
    assert result.schema["DOLocationID"].dataType == IntegerType()
    assert result.schema["passenger_count"].dataType == LongType()
    assert result.schema["airport_fee"].dataType == DoubleType()
    assert rows[-1].airport_fee == pytest.approx(1.0)
    assert "request_source" not in result.columns
    assert "cbd_congestion_fee" not in result.columns


def test_standard_raw_schema_rejects_missing_required_columns(spark):
    source = spark.sql(
        "SELECT TIMESTAMP_NTZ '2011-06-01 10:00:00' AS tpep_pickup_datetime, "
        "TIMESTAMP_NTZ '2011-06-01 10:10:00' AS tpep_dropoff_datetime, "
        "CAST(2.5 AS DOUBLE) AS trip_distance, CAST(1 AS INT) AS PULocationID, "
        "CAST(2 AS INT) AS DOLocationID, CAST(10 AS DOUBLE) AS fare_amount"
    )
    with pytest.raises(ValueError, match="total_amount"):
        standardize_raw_schema(source, context="2011-06")


def test_transform_adds_unknown_zone_flag_and_pickup_time_columns(spark, config):
    pickup = datetime(2024, 1, 15, 10, 25)
    rows = [
        _trip_values(pickup=pickup, PULocationID=264, DOLocationID=2),
        _trip_values(pickup=pickup, PULocationID=2, DOLocationID=264),
        _trip_values(pickup=pickup, PULocationID=265, DOLocationID=2),
        _trip_values(pickup=pickup, PULocationID=1, DOLocationID=999),
        _trip_values(pickup=pickup, PULocationID=1, DOLocationID=2),
        _trip_values(pickup=pickup, PULocationID=None, DOLocationID=2),
    ]
    frame = spark.sql(" UNION ALL ".join(_sql_trip_row(row) for row in rows))
    zones = spark.sql(
        "SELECT 1 AS LocationID, 'Manhattan' AS Borough, 'Midtown North' AS Zone "
        "UNION ALL SELECT 2, 'Queens', 'JFK Airport' "
        "UNION ALL SELECT 264, 'Unknown', 'N/A' "
        "UNION ALL SELECT 265, 'N/A', 'Outside of NYC'"
    )

    result = transform(frame, zones, config)
    actual = result.collect()
    flags = {
        (row.pickup_location_id, row.dropoff_location_id): row.is_unknown_zone
        for row in actual
    }

    # Cờ suy từ chính lookup: 264 (Borough Unknown / Zone N/A) là zone không xác định ở cả
    # hai chiều; 265 có Zone thật ("Outside of NYC") nên Borough 'N/A' không được tính là lạ;
    # 999 không có trong lookup và pickup null đều bị coi là không xác định.
    assert flags == {
        (264, 2): True,
        (2, 264): True,
        (265, 2): False,
        (1, 999): True,
        (1, 2): False,
        (None, 2): True,
    }
    assert all(row.pickup_hour_ts == datetime(2024, 1, 15, 10, 0) for row in actual)
    assert result.schema["pickup_date"].dataType == DateType()
    assert result.schema["pickup_hour_ts"].dataType.simpleString() in {
        "timestamp", "timestamp_ntz"
    }
    assert result.schema["is_unknown_zone"].dataType == BooleanType()


def test_null_distance_with_valid_duration_and_fare_survives(spark, config):
    pickup = datetime(2024, 1, 15, 10, 0)
    frame = spark.sql(_sql_trip_row(_trip_values(
        pickup=pickup,
        dropoff=pickup + timedelta(minutes=10),
        trip_distance=None,
        fare_amount=10.0,
        total_amount=12.0,
    )))
    cleaned, stats = clean(frame, config)
    row = cleaned.first()
    assert stats["clean_rows"] == 1
    assert stats["total_removed"] == 0
    assert stats["rules"][5]["removed"] == 0
    assert row.trip_distance is None
    assert row.is_distance_missing is False
    cleaned.unpersist()


def test_zero_distance_short_valid_trip_survives_average_speed(spark, config):
    pickup = datetime(2024, 1, 15, 10, 0)
    frame = spark.sql(_sql_trip_row(_trip_values(
        pickup=pickup,
        dropoff=pickup + timedelta(minutes=1, seconds=30),
        trip_distance=0.0,
        fare_amount=10.0,
        total_amount=12.0,
    )))

    cleaned, stats = clean(frame, config)
    row = cleaned.first()

    assert stats["clean_rows"] == 1
    assert stats["rules"][1]["distance_set_null"] == 1
    assert stats["rules"][5]["removed"] == 0
    assert row.trip_distance is None
    assert row.is_distance_missing is True
    cleaned.unpersist()


def test_effective_max_drop_rate_overrides_only_the_configured_year():
    cleaning = {"max_drop_rate": 0.08, "max_drop_rate_by_year": {"2025": 0.12}}

    assert effective_max_drop_rate(cleaning, "2025-11") == 0.12
    assert effective_max_drop_rate(cleaning, "2024-01") == 0.08
    assert effective_max_drop_rate({"max_drop_rate": 0.08}, "2025-05") == 0.08


def test_average_speed_rule_keeps_exact_boundary_trip(spark, config):
    """27.0 dặm / 972 giây = đúng 100 mph: rule giữ, nên postcheck không được báo ngược."""
    pickup = datetime(2024, 1, 15, 10, 0)
    frame = spark.sql(_sql_trip_row(_trip_values(
        pickup=pickup,
        dropoff=pickup + timedelta(seconds=972),
        trip_distance=27.0,
        fare_amount=17.11,
        total_amount=22.76,
    )))

    cleaned, stats = clean(frame, config)

    assert stats["clean_rows"] == 1
    assert stats["rules"][5]["removed"] == 0
    cleaned.unpersist()


def test_postcheck_uses_rule_speed_formula_on_boundary_trip(spark, config):
    """Regression: postcheck từng tính 27.0/(16.2/60)=100.00000000000001 và báo 1 dòng invalid."""
    boundary = spark.sql("""
        SELECT TIMESTAMP_NTZ '2026-06-28 13:21:37' pickup_datetime,
               TIMESTAMP_NTZ '2026-06-28 13:37:49' dropoff_datetime,
               CAST(27.0 AS DOUBLE) trip_distance, CAST(16.2 AS DOUBLE) trip_duration_min,
               CAST(17.11 AS DOUBLE) fare_amount, CAST(22.76 AS DOUBLE) total_amount,
               CAST(17 AS INT) pickup_location_id, CAST(61 AS INT) dropoff_location_id,
               CAST(FALSE AS BOOLEAN) is_distance_missing,
               CAST(FALSE AS BOOLEAN) is_unknown_zone,
               CAST(FALSE AS BOOLEAN) is_short_trip,
               CAST(2026 AS INT) year, CAST(6 AS INT) month
    """)

    assert verify_clean_partition(
        boundary, config, "2026-06", expected_rows=1, expected_schema=boundary
    )["invalid"] == 0


def test_writer_refuses_static_partition_overwrite(spark, tmp_path):
    """Chặn chế độ static vì nó sẽ xóa toàn bộ các partition tháng khác trong cùng cây output."""
    frame = spark.sql(
        "SELECT CAST(2024 AS INT) year, CAST(1 AS INT) month, CAST(1.0 AS DOUBLE) trip_distance"
    )
    assert spark.conf.get("spark.sql.sources.partitionOverwriteMode").lower() == "static"
    cfg = {
        "sink": {"type": "local", "local_format": "parquet", "local_mode": "overwrite"},
        "paths": {"clean_dir": str(tmp_path)},
    }
    with pytest.raises(RuntimeError, match="partitionOverwriteMode"):
        write(frame, cfg, spark)


def test_postcheck_reads_only_requested_month_partition(spark, config, tmp_path):
    months = spark.sql("""
        SELECT TIMESTAMP_NTZ '2024-01-15 10:00:00' pickup_datetime,
               TIMESTAMP_NTZ '2024-01-15 10:10:00' dropoff_datetime,
               CAST(2.0 AS DOUBLE) trip_distance, CAST(10.0 AS DOUBLE) trip_duration_min,
               CAST(10.0 AS DOUBLE) fare_amount, CAST(12.0 AS DOUBLE) total_amount,
               CAST(1 AS INT) pickup_location_id, CAST(2 AS INT) dropoff_location_id,
               CAST(FALSE AS BOOLEAN) is_distance_missing,
               CAST(FALSE AS BOOLEAN) is_unknown_zone,
               CAST(FALSE AS BOOLEAN) is_short_trip,
               CAST(2024 AS INT) year, CAST(1 AS INT) month
        UNION ALL
        SELECT TIMESTAMP_NTZ '2024-01-20 10:00:00',
               TIMESTAMP_NTZ '2024-01-20 10:00:30',
               CAST(2.0 AS DOUBLE), CAST(0.5 AS DOUBLE),
               CAST(10.0 AS DOUBLE), CAST(12.0 AS DOUBLE),
               CAST(1 AS INT), CAST(2 AS INT),
               CAST(TRUE AS BOOLEAN), CAST(FALSE AS BOOLEAN), CAST(TRUE AS BOOLEAN),
               CAST(2024 AS INT), CAST(1 AS INT)
        UNION ALL
        SELECT TIMESTAMP_NTZ '2024-02-15 10:00:00',
               TIMESTAMP_NTZ '2024-02-15 10:00:30',
               CAST(2.0 AS DOUBLE), CAST(0.5 AS DOUBLE),
               CAST(-1.0 AS DOUBLE), CAST(12.0 AS DOUBLE),
               CAST(1 AS INT), CAST(2 AS INT),
               CAST(FALSE AS BOOLEAN), CAST(FALSE AS BOOLEAN), CAST(TRUE AS BOOLEAN),
               CAST(2024 AS INT), CAST(2 AS INT)
    """)
    root = str(tmp_path / "trips")
    months.write.mode("overwrite").partitionBy("year", "month").parquet(root)

    january = read_local_month_partition(spark, root, "2024-01")
    assert january.count() == 2
    # Count cờ đo từ partition trên đĩa, gồm cả tháng khác bị loại bởi partition pruning.
    assert verify_clean_partition(
        january, config, "2024-01", expected_rows=2, expected_schema=months
    ) == {
        "rows": 2,
        "invalid": 0,
        "missing": 0,
        "is_distance_missing": 1,
        "is_unknown_zone": 0,
        "is_short_trip": 1,
    }


def test_postcheck_reports_schema_drift_before_aggregating(spark, config, tmp_path):
    """Partition cũ thiếu cột cờ phải báo schema_matches=False, không được ném AnalysisException."""
    frame = spark.sql(
        "SELECT TIMESTAMP_NTZ '2024-01-15 10:00:00' pickup_datetime, "
        "CAST(2.0 AS DOUBLE) trip_distance, CAST(10.0 AS DOUBLE) trip_duration_min, "
        "CAST(10.0 AS DOUBLE) fare_amount, CAST(2024 AS INT) year, CAST(1 AS INT) month"
    )
    expected = spark.sql(
        "SELECT TIMESTAMP_NTZ '2024-01-15 10:00:00' pickup_datetime, "
        "CAST(2.0 AS DOUBLE) trip_distance, CAST(10.0 AS DOUBLE) trip_duration_min, "
        "CAST(10.0 AS DOUBLE) fare_amount, "
        "CAST(FALSE AS BOOLEAN) is_short_trip, "
        "CAST(2024 AS INT) year, CAST(1 AS INT) month"
    )
    with pytest.raises(RuntimeError, match="schema_matches=False"):
        verify_clean_partition(frame, config, "2024-01", 1, expected)


def test_aggregated_rule_counts_match_sequential_reference(spark, config):
    short = datetime(2024, 1, 15, 10, 0, 30)
    outside = datetime(2024, 2, 1, 10, 0)
    missing_pickup = _trip_values()
    missing_pickup["tpep_pickup_datetime"] = None
    rows = [
        _trip_values(),
        _trip_values(),  # duplicate
        _trip_values(trip_distance=0.0),
        _trip_values(trip_distance=-1.0),
        _trip_values(trip_distance=101.0),
        _trip_values(dropoff=short),  # too short
        _trip_values(dropoff=datetime(2024, 1, 15, 13, 1)),  # too long
        _trip_values(fare_amount=-1.0),
        _trip_values(total_amount=-1.0),
        _trip_values(trip_distance=10.0, dropoff=datetime(2024, 1, 15, 10, 2)),
        _trip_values(pickup=outside),
        _trip_values(fare_amount=None),
        _trip_values(trip_distance=None),  # null distance is retained
        _trip_values(trip_distance=101.0, fare_amount=-5.0),  # first rule wins
        _trip_values(trip_distance=0.0, dropoff=short),  # set null, later rejected
        missing_pickup,
        _trip_values(total_amount=None),
    ]
    frame = spark.sql(" UNION ALL ".join(_sql_trip_row(row) for row in rows))

    expected_frame, expected = _sequential_reference(frame, config)
    actual_frame, actual = clean(frame, config)

    assert actual["raw_rows"] == expected["raw_rows"]
    assert actual["clean_rows"] == expected["clean_rows"]
    assert actual["total_removed"] == expected["total_removed"]
    assert actual["drop_rate"] == pytest.approx(
        expected["total_removed"] / expected["raw_rows"]
    )
    assert [
        (row["rule"], row["removed"], row["distance_set_null"], row["rows_after"])
        for row in actual["rules"]
    ] == [
        (row["rule"], row["removed"], row["distance_set_null"], row["rows_after"])
        for row in expected["rules"]
    ]
    assert sorted(repr(tuple(row)) for row in actual_frame.collect()) == sorted(
        repr(tuple(row)) for row in expected_frame.collect()
    )


def test_randomized_1000_rows_match_sequential_reference(spark, config):
    rng = random.Random(20241021)
    schema = StructType([
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
    ])
    generated = []
    raw_columns = [field.name for field in schema.fields]
    for index in range(900):
        scenario = index % 12
        pickup = datetime(2024, 1, rng.randint(1, 31), rng.randrange(24), rng.randrange(60))
        duration = rng.choice([1.5, 3.0, 10.0, 45.0, 120.0])
        distance = rng.uniform(0.5, 30.0)
        fare = rng.uniform(5.0, 80.0)
        total = fare + rng.uniform(0.0, 20.0)
        if scenario == 1:
            distance = 0.0
        elif scenario == 2:
            distance = -rng.uniform(0.1, 5.0)
        elif scenario == 3:
            distance = rng.uniform(100.1, 250.0)
        elif scenario == 4:
            duration = 0.5
        elif scenario == 5:
            duration = 181.0
        elif scenario == 6:
            fare = -rng.uniform(1.0, 20.0)
        elif scenario == 7:
            total = -rng.uniform(1.0, 20.0)
        elif scenario == 8:
            distance, duration = 30.0, 5.0
        elif scenario == 9:
            pickup = datetime(2024, 2, 1, rng.randrange(24), rng.randrange(60))
        dropoff = pickup + timedelta(minutes=duration)
        row = [
            pickup,
            dropoff,
            rng.choice([1, 2, 3, None]),
            distance,
            rng.choice([1, 2, 3, 132]),
            rng.choice([1, 2, 3, 236]),
            fare,
            total,
            rng.uniform(0.0, 20.0),
            rng.uniform(0.0, 10.0),
        ]
        if scenario == 10:
            row[(index // 12) % len(raw_columns)] = None
        elif scenario == 11:
            row[3] = None
        generated.append(tuple(row))

    generated.extend(generated[:100])
    assert len(generated) == 1000
    sql_rows = []
    for row in generated:
        literals = []
        for value, field in zip(row, schema.fields):
            sql_type = field.dataType.simpleString()
            if value is None:
                literals.append(f"CAST(NULL AS {sql_type})")
            elif isinstance(value, datetime):
                literals.append(f"TIMESTAMP_NTZ '{value:%Y-%m-%d %H:%M:%S}'")
            else:
                literals.append(f"CAST({value!r} AS {sql_type})")
        sql_rows.append("(" + ", ".join(literals) + ")")
    columns_sql = ", ".join(f"`{field.name}`" for field in schema.fields)
    frame = spark.sql(
        f"SELECT * FROM VALUES {', '.join(sql_rows)} AS generated({columns_sql})"
    )

    coverage = frame.agg(
        F.count(F.lit(1)).alias("raw_rows"),
        F.sum(F.when(F.col("trip_distance") == 0, 1).otherwise(0)).alias("distance_zero"),
        F.sum(F.when(F.col("trip_distance") < 0, 1).otherwise(0)).alias("distance_negative"),
        F.sum(F.when(F.col("trip_distance") > 100, 1).otherwise(0)).alias("distance_over_100"),
        F.sum(F.when(
            (F.timestamp_diff("SECOND", "tpep_pickup_datetime", "tpep_dropoff_datetime") / 60.0 < 1)
            | (F.timestamp_diff("SECOND", "tpep_pickup_datetime", "tpep_dropoff_datetime") / 60.0 > 180)
            | (F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime")), 1
        ).otherwise(0)).alias("duration_invalid"),
        F.sum(F.when(F.col("fare_amount") < 0, 1).otherwise(0)).alias("fare_negative"),
        F.sum(F.when(F.col("total_amount") < 0, 1).otherwise(0)).alias("total_negative"),
        F.sum(F.when(
            F.col("trip_distance").isNotNull()
            & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
            & ((F.col("trip_distance") / (
                F.timestamp_diff("SECOND", "tpep_pickup_datetime", "tpep_dropoff_datetime") / 3600.0
            )) > 100), 1
        ).otherwise(0)).alias("speed_over_100"),
        F.sum(F.when(
            (F.year("tpep_pickup_datetime") != 2024)
            | (F.month("tpep_pickup_datetime") != 1), 1
        ).otherwise(0)).alias("pickup_outside_month"),
        *[
            F.sum(F.when(F.col(field.name).isNull(), 1).otherwise(0)).alias(f"null_{field.name}")
            for field in schema.fields
        ],
    ).first().asDict()
    coverage["duplicate_rows"] = (
        frame.groupBy(*frame.columns).count()
        .filter(F.col("count") > 1)
        .agg(F.sum(F.col("count") - 1).alias("duplicate_rows"))
        .first()["duplicate_rows"] or 0
    )
    print("DIAGNOSTIC_COVERAGE", coverage)

    expected_frame, expected = _sequential_reference(frame, config)
    actual_frame, actual = clean(frame, config)

    assert actual["raw_rows"] == 1000
    assert actual["clean_rows"] == expected["clean_rows"]
    assert actual["clean_rows"] == actual_frame.count()
    assert actual["clean_rows"] + actual["total_removed"] == actual["raw_rows"]
    assert [
        (row["rule"], row["removed"], row["distance_set_null"])
        for row in actual["rules"]
    ] == [
        (row["rule"], row["removed"], row["distance_set_null"])
        for row in expected["rules"]
    ]
    assert sorted(repr(tuple(row)) for row in actual_frame.collect()) == sorted(
        repr(tuple(row)) for row in expected_frame.collect()
    )
    actual_only = actual_frame.exceptAll(expected_frame).count()
    expected_only = expected_frame.exceptAll(actual_frame).count()
    print("DIAGNOSTIC_RULES", [
        (expected_rule["rule"], expected_rule["removed"], actual_rule["removed"],
         expected_rule["distance_set_null"], actual_rule["distance_set_null"])
        for expected_rule, actual_rule in zip(expected["rules"], actual["rules"])
    ])
    print("DIAGNOSTIC_COUNTS", {
        "reference_clean_rows": expected["clean_rows"],
        "aggregated_clean_rows": actual["clean_rows"],
        "raw_rows": actual["raw_rows"],
        "total_removed": actual["total_removed"],
        "removed_plus_kept": actual["total_removed"] + actual["clean_rows"],
        "reference_only": expected_only,
        "aggregated_only": actual_only,
    })
    assert actual_only == expected_only == 0
