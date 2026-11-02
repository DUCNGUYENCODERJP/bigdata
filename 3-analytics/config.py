"""
config.py
---------
Centralized configuration for the NYC Taxi Analytics & ML pipeline.
Edit only this file to change table names, paths, or model hyper-parameters.
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class PipelineConfig:
    """All tuneable settings for the analytics and ML pipeline.

    Attributes
    ----------
    clean_table:
        Hive table containing cleaned trip records (fare_amount >= 0,
        year >= 2011).
    raw_table:
        Hive table containing raw trip records, including cancelled rows
        (fare_amount < 0). Used only for the cancelled-trip analysis step.
    output_base:
        Root HDFS / Hive Warehouse path.  Sub-directories are created
        automatically by each writer function.
    app_name:
        Spark application name shown in the Spark UI.
    max_iter:
        Maximum number of boosting iterations for GBTRegressor.
    seed:
        Random seed for reproducibility.
    train_ratio:
        Fraction of dates used for training in the chronological split.
        The remaining (1 - train_ratio) fraction forms the test set.
    feature_cols:
        Ordered list of assembled feature columns fed into GBTRegressor.
        NOTE: 'passenger_count' is intentionally excluded — it introduces
        noise and is unreliable in the raw data.
    """

    # ── Hive tables ────────────────────────────────────────────────────────────
    clean_table: str = "clean_nyc_taxi"
    raw_table: str = "raw_nyc_taxi"

    # ── Output ─────────────────────────────────────────────────────────────────
    output_base: str = "hdfs:///user/hive/warehouse/nyc_taxi_analytics"

    # ── Spark ──────────────────────────────────────────────────────────────────
    app_name: str = "NYC_Taxi_Analytics_ML_Person3"

    # ── GBTRegressor hyper-parameters ──────────────────────────────────────────
    max_iter: int = 50
    seed: int = 42

    # ── Train / test split ─────────────────────────────────────────────────────
    train_ratio: float = 0.8   # 80 % train, 20 % test (chronological)

    # ── Feature columns (order matters for VectorAssembler) ────────────────────
    # 'zone_vector'  : OneHotEncoded PULocationID
    # 'pickup_hour'  : 0–23
    # 'day_of_week'  : 1 (Sun) – 7 (Sat)
    # 'date_index'   : days since 1970-01-01 (captures long-term trend)
    feature_cols: List[str] = field(
        default_factory=lambda: ["zone_vector", "pickup_hour", "day_of_week", "date_index"]
    )


# Module-level default instance — import and use directly in other modules.
DEFAULT_CONFIG = PipelineConfig()
