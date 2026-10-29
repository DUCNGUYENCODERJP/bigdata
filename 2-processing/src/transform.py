"""Chuẩn hóa chuyến taxi và bổ sung thông tin khu vực từ zone lookup."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, functions as F

# Nhãn placeholder trong zone lookup, chuẩn hóa hay chữ và in thường.
# Tách riêng hai cột: Borough 'Unknown' hoặc Zone 'N/A' mới là zone không xác định.
# Zone có tên thật (vd "Outside of NYC") dù Borough ghi 'N/A' vẫn là danh mục hợp lệ.
PLACEHOLDER_BOROUGH_LABELS = frozenset({"unknown"})
PLACEHOLDER_ZONE_LABELS = frozenset({"", "n/a", "na", "nan", "not applicable", "unknown"})


def _placeholder_zone(borough_column, zone_column):
    """Điều kiện True khi chính zone lookup gán nhãn không xác định cho borough hoặc tên zone."""
    return (
        F.lower(F.trim(F.coalesce(borough_column, F.lit("")))).isin(sorted(PLACEHOLDER_BOROUGH_LABELS))
        | F.lower(F.trim(F.coalesce(zone_column, F.lit("")))).isin(sorted(PLACEHOLDER_ZONE_LABELS))
    )


def transform(df: DataFrame, zones_df: DataFrame, cfg: dict[str, Any] | None = None) -> DataFrame:
    """Chuẩn hóa thời gian/tên cột, join borough/zone và chọn schema đầu ra."""
    cfg = cfg or {}
    columns = cfg.get("transform", {}).get("output_columns")
    pickup_column = F.col("tpep_pickup_datetime").cast("timestamp_ntz")
    dropoff_column = F.col("tpep_dropoff_datetime").cast("timestamp_ntz")

    zones = zones_df.select(
        F.col("LocationID").cast("int").alias("zone_id"),
        F.col("Borough").cast("string").alias("borough"),
        F.col("Zone").cast("string").alias("zone"),
    ).dropDuplicates(["zone_id"])

    # Danh sách LocationID hợp lệ, suy từ chính zone lookup (không hardcode ID nào).
    known_zone_ids = [
        row.zone_id
        for row in zones.select("zone_id").distinct().collect()
        if row.zone_id is not None
    ]
    if not known_zone_ids:
        raise ValueError("Zone lookup không chứa LocationID hợp lệ nào")

    result = df
    # is_distance_missing do clean sinh ra; khi transform chạy độc lập (unit test)
    # thì suy lại từ trip_distance gốc = 0 để giữ cùng ngữ nghĩa.
    if "is_distance_missing" not in result.columns:
        result = result.withColumn(
            "is_distance_missing",
            F.coalesce(F.col("trip_distance") == 0, F.lit(False)),
        )
    result = (
        result.withColumn("pickup_datetime", pickup_column)
        .withColumn("dropoff_datetime", dropoff_column)
        .withColumn("pickup_date", F.to_date(pickup_column))
        .withColumn("pickup_hour_ts", F.date_trunc("hour", pickup_column))
        .withColumn("year", F.year(pickup_column).cast("int"))
        .withColumn("month", F.month(pickup_column).cast("int"))
        .withColumn("day", F.dayofmonth(pickup_column).cast("int"))
        .withColumn("hour", F.hour(pickup_column).cast("int"))
        .withColumn("day_of_week", F.dayofweek(pickup_column).cast("int"))
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")
        .withColumn(
            "trip_duration_min",
            F.timestamp_diff("SECOND", F.col("pickup_datetime"), F.col("dropoff_datetime"))
            / F.lit(60.0),
        )
    )
    # Cờ is_short_trip luôn suy từ thời lượng tính lại, kể cả khi clean đã gắn cờ.
    result = result.withColumn(
        "is_short_trip",
        F.coalesce(
            F.col("trip_duration_min")
            < F.lit(float(cfg.get("cleaning", {}).get("short_trip_max_minutes", 1))),
            F.lit(False),
        ),
    )

    pickup_zones = zones.select(
        F.col("zone_id").alias("pickup_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone").alias("pickup_zone"),
    )
    dropoff_zones = zones.select(
        F.col("zone_id").alias("dropoff_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone").alias("dropoff_zone"),
    )
    result = result.join(F.broadcast(pickup_zones), "pickup_location_id", "left").join(
        F.broadcast(dropoff_zones), "dropoff_location_id", "left"
    )
    # is_unknown_zone = id không có trong lookup, HOẶC lookup gán nhãn không xác định
    # cho borough/zone của một trong hai đầu. Định nghĩa này bắt cả zone placeholder
    # (vd Newark Airport cũ) mà chỉ xét "có trong lookup" thì không bao giờ bật.
    unknown_id = (
        ~F.coalesce(F.col("pickup_location_id").isin(known_zone_ids), F.lit(False))
        | ~F.coalesce(F.col("dropoff_location_id").isin(known_zone_ids), F.lit(False))
    )
    result = result.withColumn(
        "is_unknown_zone",
        unknown_id
        | _placeholder_zone(F.col("pickup_borough"), F.col("pickup_zone"))
        | _placeholder_zone(F.col("dropoff_borough"), F.col("dropoff_zone")),
    )

    if not columns:
        raise ValueError("Thiếu transform.output_columns trong config.yaml")
    missing = set(columns).difference(result.columns)
    if missing:
        raise ValueError(f"Thiếu cột đầu ra sau transform: {sorted(missing)}")
    return result.select(*columns)
