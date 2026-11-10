"""
modules/features.py
-------------------
Tạo các trường đặc trưng (Feature Engineering) và gom nhóm nhu cầu theo ngày:
(date, PULocationID, pickup_hour, day_of_week) -> label (số chuyến) + date_index.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_feature_data(trips: DataFrame) -> DataFrame:
    """Gom nhóm bản ghi chuyến đi thành các bản ghi nhu cầu theo ngày để huấn luyện ML."""
    return (
        trips
        .withColumn("date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .filter(F.col("date").isNotNull())
        .groupBy("date", "PULocationID", "pickup_hour", "day_of_week")
        .agg(F.count(F.lit(1)).cast("double").alias("label"))
        .withColumn(
            "date_index",
            F.datediff(F.col("date"), F.lit("1970-01-01")),
        )
    )
