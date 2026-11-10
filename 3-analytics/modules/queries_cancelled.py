"""
modules/queries_cancelled.py
----------------------------
Truy vấn phân tích cuốc xe bị hủy / hoàn tiền (fare_amount < 0) từ bảng raw TLC.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import MIN_DATA_YEAR
from modules.period_bounds import latest_partition_year


def build_cancelled_trips_summary(raw_trips: DataFrame) -> DataFrame:
    """Thống kê cuốc xe bị huỷ theo năm, khu vực đón và giờ."""
    latest_year = latest_partition_year(raw_trips)
    if latest_year is None:
        raise ValueError("Raw Hive table must expose a populated year partition.")

    eligible_raw = raw_trips.filter(F.col("fare_amount") < 0)
    eligible_raw = eligible_raw.filter(
        F.col("year").cast("int").between(MIN_DATA_YEAR, latest_year)
    )

    return (
        eligible_raw
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .filter(F.col("pickup_year").between(MIN_DATA_YEAR, latest_year))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_year", "PULocationID", "pickup_hour")
        .agg(F.count(F.lit(1)).alias("cancelled_count"))
        .orderBy("pickup_year", "PULocationID", "pickup_hour")
    )
