"""
modules/baseline_model.py
-------------------------
Mô hình cơ sở (Baseline): Sử dụng trung bình lịch sử theo từng cặp (PULocationID × pickup_hour).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_baseline_predictions(test: DataFrame, train: DataFrame) -> DataFrame:
    """Thêm cột baseline_prediction vào DataFrame test dựa trên trung bình lịch sử từ train."""
    # Tính mean theo từng (PULocationID × pickup_hour) trong train
    zone_hour_means = train.groupBy("PULocationID", "pickup_hour").agg(
        F.avg("label").alias("_zone_hour_mean")
    )

    # Fallback global mean cho các cặp zone/hour chưa từng xuất hiện trong tập train
    global_mean_row = train.agg(F.avg("label").alias("gm")).first()
    global_mean: float = float(global_mean_row["gm"]) if global_mean_row["gm"] is not None else 0.0

    return (
        test
        .join(zone_hour_means, on=["PULocationID", "pickup_hour"], how="left")
        .withColumn(
            "baseline_prediction",
            F.coalesce(F.col("_zone_hour_mean"), F.lit(global_mean)),
        )
        .drop("_zone_hour_mean")
    )
