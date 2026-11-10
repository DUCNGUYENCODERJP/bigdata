"""
main.py
-------
Entry point for the NYC Taxi Analytics & Demand Forecasting pipeline.

Pipeline Overview
-----------------
 Step 1 │ Load clean trip data from Hive (clean_nyc_taxi_tlc)
 Step 2 │ Spark SQL analysis: demand, fare, duration, heatmap
 Step 3 │ Cancelled-trip analysis from raw Hive table (raw_nyc_taxi)
 Step 4 │ Feature engineering: aggregate into daily demand records
 Step 5 │ Chronological 80/20 train/test split (no future leakage)
 Step 6 │ Train GBTRegressor; generate GBT & Baseline predictions
 Step 7 │ Evaluate: RMSE, MAE, MAPE; write metrics CSV
 Step 8 │ Write all output tables to HDFS/Hive Parquet for downstream BI

Output Tables (all under --output-base/)
-----------------------------------------
 analytics_summary/     — zone × hour demand, fare, duration
 fare_travel_by_hour/   — hourly fare & duration trends
 demand_heatmap/        — hour × weekday demand grid
 cancelled_trips/       — cancelled trip counts (from raw table)
 predictions/           — test-set actual vs. Baseline vs. GBT
 metrics/               — RMSE, MAE, MAPE CSV

Run Example
-----------
    spark-submit main.py \\
        --clean-table default.clean_nyc_taxi_tlc \\
        --raw-table   default.raw_nyc_taxi \\
        --output-base hdfs:///user/hive/warehouse/nyc_taxi_analytics \\
        --max-iter    50

    # Skip cancelled analysis when raw table is unavailable:
    spark-submit main.py --skip-cancelled
"""
import logging

import argparse
import sys

from pyspark.sql import SparkSession

from analysis import (
    build_analytics_summary,
    build_cancelled_trips_summary,
    build_demand_heatmap,
    build_hourly_fare_summary,
)
from data_loader import load_clean_trips, load_raw_trips
from evaluation import compute_metrics, log_comparison_table
from feature_engineering import build_feature_data, chronological_split
from model import add_baseline_predictions, add_gbt_predictions, train_gbt
from writer import write_metrics_csv, write_parquet


# ── CLI Arguments ──────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "NYC Taxi Analytics & Demand Forecasting Pipeline\n"
            "Analyses trip data with Spark SQL and trains a GBTRegressor "
            "to forecast hourly demand by pickup zone."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--clean-table",
        default="default.clean_nyc_taxi_tlc",
        metavar="TABLE",
        help="Hive compatibility view of cleaned trips (default: default.clean_nyc_taxi_tlc).",
    )
    parser.add_argument(
        "--raw-table",
        default="default.raw_nyc_taxi",
        metavar="TABLE",
        help="Hive table of raw trips including cancellations (default: default.raw_nyc_taxi).",
    )
    parser.add_argument(
        "--output-base",
        default="hdfs:///user/hive/warehouse/nyc_taxi_analytics",
        metavar="PATH",
        help="Root HDFS / local path for all output tables.",
    )
    parser.add_argument(
        "--app-name",
        default="NYC_Taxi_Analytics_ML",
        help="Spark application name shown in the UI.",
    )
    parser.add_argument(
        "--max-iter",
        type=int,
        default=50,
        help="GBTRegressor boosting iterations (default: 50).",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Fraction of dates used for training; rest is test (default: 0.8).",
    )
    parser.add_argument(
        "--skip-cancelled",
        action="store_true",
        help="Skip Step 3 (cancelled-trip analysis) when the raw table is unavailable.",
    )
    return parser.parse_args()


# ── Pipeline Steps ─────────────────────────────────────────────────────────────

def _banner(step: int, title: str) -> None:
    logging.info(f"\n{'─'*60}")
    logging.info(f"  Step {step}: {title}")
    logging.info(f"{'─'*60}")


def step_analysis(trips, output_base: str) -> None:
    """Step 2 — Run Spark SQL analytical queries and write results."""
    _banner(2, "Spark SQL Analysis")

    logging.info("  • Building analytics_summary (zone × hour: count, fare, duration)…")
    write_parquet(
        build_analytics_summary(trips),
        output_base + "/analytics_summary",
        "default.analytics_summary",
    )

    logging.info("  • Building fare_travel_by_hour (hourly: count, fare, duration)…")
    write_parquet(
        build_hourly_fare_summary(trips),
        output_base + "/fare_travel_by_hour",
        "default.fare_travel_by_hour",
    )

    logging.info("  • Building demand_heatmap (hour × weekday: count)…")
    write_parquet(
        build_demand_heatmap(trips),
        output_base + "/demand_heatmap",
        "default.demand_heatmap",
    )


def step_cancelled(spark: SparkSession, raw_table: str, output_base: str) -> None:
    """Step 3 — Analyse cancelled trips from the raw Hive table."""
    _banner(3, "Cancelled-Trip Analysis")
    raw_trips = load_raw_trips(spark, raw_table)
    logging.info("  • Counting cancelled trips (fare_amount < 0) by year / zone / hour…")
    write_parquet(
        build_cancelled_trips_summary(raw_trips),
        output_base + "/cancelled_trips",
        "default.cancelled_trips",
    )


def step_ml(
    trips,
    output_base: str,
    max_iter: int,
    train_ratio: float,
    spark: SparkSession,
) -> None:
    """Steps 4–8 — Feature engineering, training, evaluation, and output."""
    # ── Step 4: Feature engineering ───────────────────────────────────────────
    _banner(4, "Feature Engineering")
    logging.info("  • Aggregating trips into daily demand records by zone × hour…")
    feature_data = build_feature_data(trips)

    # ── Step 5: Train / test split ────────────────────────────────────────────
    _banner(5, f"Chronological Split  (train={train_ratio:.0%} / test={1-train_ratio:.0%})")
    train_data, test_data = chronological_split(feature_data, train_ratio)
    logging.info(f"  • Training set: {train_data.count():,} records")
    logging.info(f"  • Test set:     {test_data.count():,} records")

    # ── Step 6: Baseline predictions ──────────────────────────────────────────
    _banner(6, "Baseline Model  (historical mean by zone × hour)")
    logging.info("  • Computing mean trip count per (PULocationID × pickup_hour)…")
    test_with_baseline = add_baseline_predictions(test_data, train_data)

    # ── Step 6: GBT training & predictions ────────────────────────────────────
    _banner(6, f"GBTRegressor  (maxIter={max_iter})")
    logging.info("  • Training GBTRegressor pipeline…")
    gbt_model = train_gbt(train_data, max_iter=max_iter)

    logging.info("  • Generating predictions (negative values clipped to 0)…")
    results = add_gbt_predictions(gbt_model, test_with_baseline).select(
        "date",
        "PULocationID",
        "pickup_hour",
        "day_of_week",
        "label",
        "baseline_prediction",
        "gbt_prediction",
    )

    # ── Step 7: Evaluate ──────────────────────────────────────────────────────
    _banner(7, "Model Evaluation  (RMSE | MAE | MAPE)")
    metrics = [
        compute_metrics(results, "baseline_prediction", "zone_hour_mean_baseline"),
        compute_metrics(results, "gbt_prediction",      "gbt_regressor"),
    ]
    log_comparison_table(metrics)

    # ── Step 8: Write outputs ─────────────────────────────────────────────────
    _banner(8, "Writing Outputs  (Parquet + CSV)")
    write_parquet(results, output_base + "/predictions", "default.predictions")
    write_metrics_csv(spark, metrics, output_base + "/metrics")


# ── Entry Point ────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName(args.app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logging.info("\n" + "═" * 60)
    logging.info("  NYC Taxi Analytics & Demand Forecasting")
    logging.info("═" * 60)
    logging.info(f"  Clean table  : {args.clean_table}")
    logging.info(f"  Raw table    : {args.raw_table}")
    logging.info(f"  Output base  : {args.output_base}")
    logging.info(f"  GBT maxIter  : {args.max_iter}")
    logging.info(f"  Train ratio  : {args.train_ratio:.0%}")

    try:
        # Step 1: Load
        _banner(1, f"Loading clean trips from '{args.clean_table}'")
        trips = load_clean_trips(spark, args.clean_table)
        logging.info("  • Data loaded and validated ✓")

        # Step 2: Analysis
        step_analysis(trips, args.output_base)

        # Step 3: Cancelled-trip analysis
        if args.skip_cancelled:
            logging.info("\n  [Step 3] Skipped (--skip-cancelled flag is set).")
        else:
            step_cancelled(spark, args.raw_table, args.output_base)

        # Steps 4–8: ML pipeline
        step_ml(trips, args.output_base, args.max_iter, args.train_ratio, spark)

        logging.info("\n" + "═" * 60)
        logging.info("  ✅  Pipeline completed successfully.")
        logging.info(f"  📁  Outputs → {args.output_base}")
        logging.info("═" * 60 + "\n")

    except Exception:
        logging.error("\n  ❌  Pipeline failed with the following error:\n")
        logging.exception("Analytics pipeline failed")
        sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
