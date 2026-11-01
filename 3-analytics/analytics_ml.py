"""
analytics_ml.py  (legacy shim)
-------------------------------
**Deprecated** — this file is kept for backward compatibility only.

The pipeline has been refactored into focused modules for maintainability:

    config.py              — tuneable settings & constants
    data_loader.py         — Hive data loading + column validation
    analysis.py            — Spark SQL analytical queries (4 tables)
    feature_engineering.py — ML feature aggregation + chronological split
    model.py               — GBTRegressor pipeline + baseline model
    evaluation.py          — RMSE, MAE, MAPE + comparison table
    writer.py              — Parquet / CSV output writers
    main.py                — Pipeline orchestrator & CLI entry point

HOW TO RUN
----------
Use main.py instead of this file:

    spark-submit main.py \\
        --clean-table clean_nyc_taxi \\
        --raw-table   raw_nyc_taxi \\
        --output-base hdfs:///user/hive/warehouse/nyc_taxi_analytics \\
        --max-iter    50

    # Full options:
    spark-submit main.py --help

This shim re-exports the most commonly used public symbols so that any
existing import statements continue to work without modification.
"""

# Re-export public API from the new modules so that:
#   from analytics_ml import build_feature_data, chronological_split, ...
# still works without change.

from analysis import (                        # noqa: F401  (re-export)
    build_analytics_summary,
    build_cancelled_trips_summary,
    build_demand_heatmap,
    build_hourly_fare_summary,
)
from config import PipelineConfig, DEFAULT_CONFIG  # noqa: F401
from data_loader import load_clean_trips, load_raw_trips  # noqa: F401
from evaluation import compute_metrics, print_comparison_table  # noqa: F401
from feature_engineering import build_feature_data, chronological_split  # noqa: F401
from model import (                            # noqa: F401
    add_baseline_predictions,
    add_gbt_predictions,
    build_gbt_pipeline,
    train_gbt,
)
from writer import write_metrics_csv, write_parquet  # noqa: F401

# ── Legacy entry point ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    import warnings
    warnings.warn(
        "analytics_ml.py is deprecated. Please use main.py instead.\n"
        "  spark-submit main.py --help",
        DeprecationWarning,
        stacklevel=1,
    )
    from main import main
    main()
