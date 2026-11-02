"""
feature_engineering.py
-----------------------
Feature engineering and train / test splitting for taxi demand forecasting.

Responsibilities
~~~~~~~~~~~~~~~~
- Aggregate raw trip rows into daily demand records grouped by
  (date, PULocationID, pickup_hour, day_of_week).
- Add a numeric date_index column for chronological splitting and
  long-trend modelling.
- Split data chronologically (no future leakage) at the configured ratio.

NOTE: passenger_count is **intentionally excluded** from all features.
It is unreliable in raw data and introduces noise rather than signal.
"""

from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ── Public API ─────────────────────────────────────────────────────────────────

def build_feature_data(trips: DataFrame) -> DataFrame:
    """Aggregate trips into daily demand records ready for ML.

    Groups trip rows by (date, PULocationID, pickup_hour, day_of_week)
    and counts the number of trips — this becomes the regression *label*.

    A numeric *date_index* (days since 1970-01-01) is added so that:
    - chronological_split() can identify the training / test boundary, and
    - the model can capture long-term temporal trends.

    Schema of the returned DataFrame
    ---------------------------------
    date         DATE    — calendar date
    PULocationID LONG    — pickup zone ID
    pickup_hour  INT     — hour of day (0–23)
    day_of_week  INT     — 1 (Sun) … 7 (Sat)
    label        DOUBLE  — number of trips (regression target)
    date_index   INT     — days since 1970-01-01
    """
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


def chronological_split(
    data: DataFrame,
    train_ratio: float = 0.8,
) -> Tuple[DataFrame, DataFrame]:
    """Split *data* into training and test sets by date order.

    Uses an approximate quantile on *date_index* to find the cutoff date,
    ensuring no future data leaks into the training set.  This is more
    robust than a random split for time-series demand data.

    Parameters
    ----------
    data:
        Output of :func:`build_feature_data`.
    train_ratio:
        Fraction of distinct dates assigned to the training set.
        Default: 0.8 (80 % train / 20 % test).

    Returns
    -------
    (train_df, test_df)
        DataFrames for training and testing respectively.

    Raises
    ------
    ValueError
        If the data is empty or does not span at least two distinct dates.
    """
    if not (0.0 < train_ratio < 1.0):
        raise ValueError(
            f"train_ratio must be in (0, 1); got {train_ratio}"
        )

    cutoffs = data.approxQuantile("date_index", [train_ratio], 0.0)
    if not cutoffs:
        raise ValueError(
            "Cannot split: no aggregated trip records are available."
        )
    cutoff = cutoffs[0]

    train = data.filter(F.col("date_index") <= F.lit(cutoff))
    test = data.filter(F.col("date_index") > F.lit(cutoff))

    # Eagerly check both partitions are non-empty
    if train.limit(1).count() == 0 or test.limit(1).count() == 0:
        raise ValueError(
            "Chronological split produced an empty partition. "
            "Ensure data spans at least two distinct dates."
        )

    return train, test
