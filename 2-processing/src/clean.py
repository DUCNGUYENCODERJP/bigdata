"""Clean taxi trips and report ordered, non-overlapping rule counts."""

from __future__ import annotations

import logging
from functools import reduce
from operator import or_
from typing import Any

from pyspark.sql import DataFrame, functions as F
from pyspark.storagelevel import StorageLevel

LOGGER = logging.getLogger(__name__)


def average_speed_violation(
    cleaning: dict[str, Any],
    pickup_col: str = "tpep_pickup_datetime",
    dropoff_col: str = "tpep_dropoff_datetime",
    distance_col: str = "trip_distance",
):
    """Predicate báo chuyến vượt tốc độ trung bình tối đa, dùng chung cho rule và postcheck.

    Postcheck buộc phải dùng đúng biểu thức này: hai cách viết tương đương về toán học
    (distance/seconds*3600 và distance/(minutes/60)) cho kết quả dấu phẩy động khác nhau
    ở ngay biên ngưỡng, khiến rule giữ dòng 27.0 dặm/972 giây = 100.0 mph nhưng postcheck
    lại tính ra 100.00000000000001 và báo vi phạm.
    """
    duration_seconds = F.timestamp_diff("SECOND", F.col(pickup_col), F.col(dropoff_col))
    duration_hours = duration_seconds / F.lit(3600.0)
    duration_minutes = duration_seconds / F.lit(60.0)
    # Chỉ xét chuyến đủ dài để tốc độ suy ra có ý nghĩa (>= mốc gắn cờ is_short_trip).
    measurable = duration_minutes >= F.lit(
        max(
            float(cleaning.get("short_trip_max_minutes", 0)),
            float(cleaning["trip_duration_min_minutes"]),
        )
    )
    return (
        F.col(distance_col).isNotNull()
        & (duration_hours > 0)
        & measurable
        & (F.col(distance_col) / duration_hours > cleaning["average_speed_max_mph"])
    )


def effective_max_drop_rate(cleaning: dict[str, Any], month: str) -> float:
    """Ngưỡng loại áp dụng cho một tháng: override theo năm thắng ngưỡng chung.

    Cụm 2025-05..11 có 6-9% dòng `fare_amount < 0` là cuốc huỷ/refund thật (đã đo:
    337.160/395.093 dòng trong khoảng -10..0, chỉ 12 dòng có tip, đa số pull = drop),
    nên ngưỡng chung 8% không còn mô tả đúng riêng năm 2025.
    """
    overrides = cleaning.get("max_drop_rate_by_year") or {}
    return float(overrides.get(month[:4], cleaning["max_drop_rate"]))


def _rule_condition(df: DataFrame, rule: str, cfg: dict[str, Any], month: str):
    """Return the Spark predicate for a configured cleaning rule."""
    cleaning = cfg["cleaning"]
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    duration_seconds = F.timestamp_diff("SECOND", pickup, dropoff)

    if rule == "trip_distance":
        distance = F.col("trip_distance")
        return (distance < cleaning["trip_distance_min_miles"]) | (
            distance > cleaning["trip_distance_max_miles"]
        )
    if rule == "trip_duration":
        duration_minutes = duration_seconds / F.lit(60.0)
        return (
            (duration_minutes <= cleaning["trip_duration_min_minutes"])
            | (duration_minutes > cleaning["trip_duration_max_minutes"])
            | (dropoff <= pickup)
        )
    if rule == "fare_amount":
        return F.col("fare_amount") < 0
    if rule == "total_amount":
        return F.col("total_amount") < 0
    if rule == "average_speed":
        return average_speed_violation(cleaning)
    if rule == "pickup_month":
        expected_year, expected_month = (int(part) for part in month.split("-"))
        return (F.year(pickup) != expected_year) | (F.month(pickup) != expected_month)
    if rule == "required_fields":
        required = cleaning["required_non_null_columns"]
        return reduce(or_, (F.col(column).isNull() for column in required), F.lit(False))
    raise ValueError(f"Unsupported cleaning rule: {rule}")


def clean(df: DataFrame, cfg: dict[str, Any]) -> tuple[DataFrame, dict[str, Any]]:
    """Clean a frame, aggregating all ordered rule counts in one scan."""
    month = cfg.get("processing_month")
    if not month:
        raise ValueError("cfg.processing_month must be set before clean()")

    config_order = cfg["cleaning"]["rule_order"]
    if not config_order or config_order[0] != "duplicate":
        raise ValueError("cleaning.rule_order must start with duplicate")

    raw_count = df.count()
    deduplicated = df.dropDuplicates().persist(StorageLevel.MEMORY_AND_DISK)
    try:
        distinct_count = deduplicated.count()
        distance_was_zero = F.coalesce(F.col("trip_distance") == 0, F.lit(False))
        normalized = deduplicated.withColumn(
            "is_distance_missing", distance_was_zero
        ).withColumn(
            "trip_distance",
            F.when(distance_was_zero, F.lit(None).cast("double")).otherwise(
                F.col("trip_distance")
            ),
        )
        stats: dict[str, Any] = {
            "month": month,
            "raw_rows": raw_count,
            "rules": [],
        }
        duplicate_count = raw_count - distinct_count
        expressions = []
        prior_invalid = F.lit(False)
        rule_conditions = {}
        for index, rule in enumerate(config_order):
            if rule == "duplicate":
                stats["rules"].append({
                    "rule": rule,
                    "rows_before": raw_count,
                    "removed": duplicate_count,
                    "distance_set_null": 0,
                    "rows_after": distinct_count,
                })
                continue

            condition = F.coalesce(
                _rule_condition(normalized, rule, cfg, month), F.lit(False)
            )
            exclusive = condition & ~prior_invalid
            rule_conditions[rule] = condition
            expressions.append(
                F.sum(F.when(exclusive, 1).otherwise(0)).alias(f"removed_{index}")
            )
            if rule == "trip_distance":
                expressions.append(
                    F.sum(
                        F.when(
                            F.col("is_distance_missing") & ~prior_invalid, 1
                        ).otherwise(0)
                    ).alias(f"distance_set_null_{index}")
                )
            prior_invalid = prior_invalid | condition

        zero_passenger_survivor = (F.col("passenger_count") == 0) & ~prior_invalid
        expressions.append(
            F.sum(F.when(zero_passenger_survivor, 1).otherwise(0)).alias(
                "zero_passenger_count_set_null"
            )
        )
        aggregate = normalized.agg(*expressions).first().asDict()

        prior_invalid = F.lit(False)
        for index, rule in enumerate(config_order):
            if rule == "duplicate":
                continue
            condition = rule_conditions[rule]
            before = distinct_count - sum(
                int(aggregate.get(f"removed_{earlier}") or 0)
                for earlier in range(index)
                if config_order[earlier] != "duplicate"
            )
            removed = int(aggregate.get(f"removed_{index}") or 0)
            distance_set_null = int(
                aggregate.get(f"distance_set_null_{index}") or 0
            )
            stats["rules"].append({
                "rule": rule,
                "rows_before": before,
                "removed": removed,
                "distance_set_null": distance_set_null,
                "rows_after": before - removed,
            })
            LOGGER.info(
                "clean rule=%s before=%s removed=%s distance_set_null=%s after=%s",
                rule,
                before,
                removed,
                distance_set_null,
                before - removed,
            )
            prior_invalid = prior_invalid | condition

        total_removed = sum(rule["removed"] for rule in stats["rules"])
        clean_rows = raw_count - total_removed
        cleaned = normalized.filter(~prior_invalid)
        if cfg["cleaning"]["zero_passenger_count"] != "set_null":
            raise ValueError("cleaning.zero_passenger_count only supports set_null")
        cleaned = cleaned.withColumn(
            "passenger_count",
            F.when(F.col("passenger_count") == 0, F.lit(None)).otherwise(
                F.col("passenger_count")
            ),
        )

        stats["zero_passenger_count_set_null"] = int(
            aggregate.get("zero_passenger_count_set_null") or 0
        )
        stats["clean_rows"] = clean_rows
        stats["total_removed"] = total_removed
        stats["drop_rate"] = total_removed / raw_count if raw_count else 0.0
        LOGGER.info(
            "clean total raw=%s clean=%s removed=%s rate=%.4f%%",
            raw_count,
            clean_rows,
            total_removed,
            stats["drop_rate"] * 100,
        )
        return cleaned, stats
    finally:
        deduplicated.unpersist(blocking=True)
