"""
analysis.py
-----------
Spark SQL / DataFrame analytical queries for NYC Taxi trip data.

Each public function returns a transformed DataFrame and is idempotent —
it does **not** write any files (that is writer.py's job).

Queries produced here answer the four core analytical questions:
  1. How does demand vary by pickup zone and hour?            → analytics_summary
  2. How do fare / travel time vary across the day?           → hourly_fare_summary
  3. What is the hour × weekday demand pattern?               → demand_heatmap
  4. How prevalent are cancelled trips per zone / hour?       → cancelled_trips_summary
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ── 1. Analytics Summary (zone × hour) ────────────────────────────────────────

def build_analytics_summary(trips: DataFrame) -> DataFrame:
    """Aggregate trip count, average fare and average duration by zone × hour.

    This is the primary analytics table used by Person 4 for Power BI
    zone-level visualisations.

    Schema
    ------
    PULocationID   LONG    — pickup zone ID
    pickup_hour    INT     — hour of day (0–23)
    trip_count     LONG    — number of trips in this group
    avg_fare       DOUBLE  — mean fare amount (USD)
    avg_duration_min DOUBLE— mean trip duration (minutes)

    Notes
    -----
    - Rows with NULL dropoff time or NULL fare are excluded.
    - Rows with a computed negative duration are excluded (data quality).
    - fare_amount < 0 rows should already be absent from the clean table,
      but the filter on dropoff / fare NULL handles any edge cases.
    """
    _duration_expr = _trip_duration_minutes()

    return (
        trips
        .filter(
            F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("fare_amount").isNotNull()
        )
        .withColumn("trip_duration_min", _duration_expr)
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


# ── 2. Hourly Fare & Duration Summary ─────────────────────────────────────────

def build_hourly_fare_summary(trips: DataFrame) -> DataFrame:
    """Aggregate fare and duration statistics by pickup hour only.

    Useful for time-of-day trend line charts in Power BI.

    Schema
    ------
    pickup_hour      INT    — hour of day (0–23)
    trip_count       LONG
    avg_fare         DOUBLE — mean fare (USD)
    avg_duration_min DOUBLE — mean trip duration (minutes)
    """
    _duration_expr = _trip_duration_minutes()

    return (
        trips
        .filter(
            F.col("tpep_dropoff_datetime").isNotNull()
            & F.col("fare_amount").isNotNull()
        )
        .withColumn("trip_duration_min", _duration_expr)
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


# ── 3. Demand Heatmap (hour × day_of_week) ────────────────────────────────────

def build_demand_heatmap(trips: DataFrame) -> DataFrame:
    """Compute trip volume per (pickup_hour × day_of_week) combination.

    Directly powers the Power BI heatmap visual.

    Schema
    ------
    pickup_hour  INT  — hour of day (0–23)
    day_of_week  INT  — 1 = Sunday … 7 = Saturday (Spark convention)
    trip_count   LONG
    """
    return (
        trips
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))
        .groupBy("pickup_hour", "day_of_week")
        .agg(F.count(F.lit(1)).alias("trip_count"))
        .orderBy("day_of_week", "pickup_hour")
    )


# ── 4. Cancelled Trips Summary (raw data) ─────────────────────────────────────

def build_cancelled_trips_summary(raw_trips: DataFrame) -> DataFrame:
    """Analyse cancelled trips (fare_amount < 0) from the raw Hive table.

    These records are excluded from the clean table; this function helps
    quantify the impact (e.g. 5.9–9.4 % row loss in 2025).

    Schema
    ------
    pickup_year      INT  — year of pickup
    PULocationID     LONG — pickup zone
    pickup_hour      INT  — hour of day (0–23)
    cancelled_count  LONG — number of cancelled trips in this group
    """
    return (
        raw_trips
        .filter(F.col("fare_amount") < 0)
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .groupBy("pickup_year", "PULocationID", "pickup_hour")
        .agg(F.count(F.lit(1)).alias("cancelled_count"))
        .orderBy("pickup_year", "PULocationID", "pickup_hour")
    )


# ── Internal helpers ───────────────────────────────────────────────────────────

def _trip_duration_minutes() -> F.Column:
    """Return a Column expression for trip duration in minutes."""
    return (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / F.lit(60.0)
