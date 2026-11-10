"""
modules/queries_analytics.py
----------------------------
Các câu truy vấn Spark SQL phân tích nhu cầu, giá cước và thời gian di chuyển.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _trip_duration_minutes() -> F.Column:
    """Tính thời lượng chuyến đi theo phút."""
    return (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / F.lit(60.0)


def build_analytics_summary(trips: DataFrame) -> DataFrame:
    """Tổng hợp số chuyến, giá cước TB và thời lượng TB theo (Zone × Giờ)."""
    duration_expr = _trip_duration_minutes()

    return (
        trips
        .filter(
            F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("fare_amount").isNotNull()
        )
        .withColumn("trip_duration_min", duration_expr)
        .filter(F.col("trip_duration_min") >= 0)
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("PULocationID", "pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 4).alias("avg_duration_min"),
        )
        .orderBy("PULocationID", "pickup_hour")
    )


def build_hourly_fare_summary(trips: DataFrame) -> DataFrame:
    """Tổng hợp lưu lượng, cước và thời lượng trung bình theo Giờ trong ngày (toàn TP)."""
    duration_expr = _trip_duration_minutes()

    return (
        trips
        .filter(
            F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("fare_amount").isNotNull()
        )
        .withColumn("trip_duration_min", duration_expr)
        .filter(F.col("trip_duration_min") >= 0)
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.round(F.avg("fare_amount"), 4).alias("avg_fare"),
            F.round(F.avg("trip_duration_min"), 4).alias("avg_duration_min"),
        )
        .orderBy("pickup_hour")
    )


def build_demand_heatmap(trips: DataFrame) -> DataFrame:
    """Tính mật độ cuốc xe theo lưới (pickup_hour × day_of_week)."""
    return (
        trips
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .groupBy("pickup_hour", "day_of_week")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .orderBy("day_of_week", "pickup_hour")
    )
