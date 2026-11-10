"""Run a full-history, time-ordered GBT evaluation and export Power BI tables.

The clean Hive table is scanned once. Daily zone/hour aggregates are persisted
to local disk so analytics summaries, the chronological split, and model
evaluation reuse the compact result instead of rereading 1.5 billion trips.
"""

from __future__ import annotations
import logging

import argparse
import csv
import datetime as dt
import json
import os
import re
import shutil
import sys
import traceback
from pathlib import Path


def _prepare_runtime(project_root: Path) -> str:
    java_candidates = [
        Path(r"C:\Program Files\Java\jdk-17"),
        Path(r"C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot"),
        Path(os.environ.get("JAVA_HOME", "")),
    ]
    java_home = None
    for candidate in java_candidates:
        if not candidate or not (candidate / "bin" / "java.exe").is_file():
            continue
        try:
            release = (candidate / "release").read_text(encoding="utf-8")
            version_line = next(line for line in release.splitlines() if line.startswith("JAVA_VERSION="))
            version = version_line.partition("=")[2].strip().strip('"')
        except (OSError, StopIteration):
            version = candidate.name.removeprefix("jdk-")
        if version.split(".", 1)[0] == "17":
            java_home = candidate
            break
    if java_home is None:
        raise RuntimeError("JDK 17 is required for this PySpark run.")
    os.environ["JAVA_HOME"] = str(java_home)

    hadoop_candidates = [
        Path(r"C:\hadoop"),
        Path(r"D:\hadoop"),
        Path(os.environ.get("HADOOP_HOME", "")),
    ]
    hadoop_home = next(
        (
            candidate
            for candidate in hadoop_candidates
            if candidate and (candidate / "bin" / "winutils.exe").is_file()
        ),
        None,
    )
    if hadoop_home is None:
        raise RuntimeError("Hadoop Windows binaries (bin/winutils.exe) were not found.")
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    os.environ["HADOOP_CONF_DIR"] = str(project_root / "1-warehouse" / "config")

    hosts_candidates = [hadoop_home / "custom_hosts", project_root / "custom_hosts"]
    hosts_file = next((path for path in hosts_candidates if path.is_file() and " " not in str(path)), None)
    hosts_option = f"-Djdk.net.hosts.file={str(hosts_file).replace(os.sep, '/')}" if hosts_file else ""
    existing_options = os.environ.get("_JAVA_OPTIONS", "")
    if hosts_option and hosts_option not in existing_options:
        os.environ["_JAVA_OPTIONS"] = f"{existing_options} {hosts_option}".strip()

    os.environ["PYTHONIOENCODING"] = "utf-8"
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8")
    path_prefix = [str(java_home / "bin"), str(hadoop_home / "bin")]
    os.environ["PATH"] = os.pathsep.join([*path_prefix, os.environ.get("PATH", "")])
    return hosts_option


def _partition_month(value: str) -> str | None:
    match = re.fullmatch(r"year=(\d{4})/month=(\d{1,2})", value.strip())
    if not match:
        return None
    year, month = map(int, match.groups())
    if not 1 <= month <= 12:
        return None
    return f"{year:04d}-{month:02d}"


def _local_file_uri(path: Path) -> str:
    """Return a local-file URI without escaping Windows path characters.

    Hadoop's Windows ``Path`` parser accepts spaces in the URI and escapes them
    internally. ``Path.as_uri()`` pre-escapes spaces, which this runtime then
    treats literally and writes under a wrong ``%20`` directory.
    """
    resolved = path.resolve()
    if os.name == "nt":
        return f"file:///{resolved.as_posix()}"
    return resolved.as_uri()


def _physical_clean_table(clean_table: str) -> str:
    database, separator, table = clean_table.rpartition(".")
    if separator and table.endswith("_tlc"):
        return f"{database}.{table[:-4]}"
    return clean_table


def _shared_months(spark, clean_table: str, raw_table: str) -> list[str]:
    physical_clean = _physical_clean_table(clean_table)

    for table_name in (physical_clean, raw_table):
        try:
            spark.sql(f"MSCK REPAIR TABLE {table_name}")
        except Exception as error:
            logging.info(f"[PARTITIONS] Repair metadata skipped for {table_name}: {error}")

    def read_months(table_name: str) -> set[str]:
        return {
            month
            for row in spark.sql(f"SHOW PARTITIONS {table_name}").collect()
            if (month := _partition_month(str(row[0]))) is not None
        }

    clean_months = read_months(physical_clean)
    raw_months = read_months(raw_table)
    return sorted(clean_months & raw_months)


def _write_csv(data, path: Path) -> None:
    temporary_dir = path.parent / f".{path.stem}_parts"
    if temporary_dir.exists() or path.exists():
        raise FileExistsError(f"Refusing to overwrite an existing export: {path}")
    data.coalesce(1).write.mode("errorifexists").option("header", True).csv(
        _local_file_uri(temporary_dir)
    )
    parts = list(temporary_dir.glob("part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one CSV part at {temporary_dir}; found {len(parts)}.")
    os.replace(parts[0], path)
    shutil.rmtree(temporary_dir)


def _write_small_csv(rows: list[dict], path: Path) -> None:
    """Write tiny driver-side result sets without starting Spark Python workers."""
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite an existing export: {path}")
    if not rows:
        raise ValueError(f"Cannot write an empty export: {path}")
    with path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _weighted_summary(daily, keys: list[str]):
    from pyspark.sql import functions as F

    return (
        daily.groupBy(*keys)
        .agg(
            F.sum("label").cast("long").alias("trip_count"),
            F.sum("fare_sum").alias("fare_sum"),
            F.sum("duration_sum").alias("duration_sum"),
            F.sum("average_count").alias("average_count"),
        )
        .withColumn("avg_fare", F.round(F.col("fare_sum") / F.col("average_count"), 4))
        .withColumn(
            "avg_duration_min",
            F.round(F.col("duration_sum") / F.col("average_count"), 4),
        )
        .drop("fare_sum", "duration_sum", "average_count")
    )


def run(args: argparse.Namespace) -> dict:
    project_root = Path(__file__).resolve().parents[1]
    hosts_option = _prepare_runtime(project_root)

    from pyspark import StorageLevel
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import IntegerType

    analytics_dir = project_root / "3-analytics"
    if str(analytics_dir) not in sys.path:
        sys.path.insert(0, str(analytics_dir))
    from modules.baseline_model import add_baseline_predictions
    from modules.gbt_model import add_gbt_predictions, train_gbt
    from modules.metrics_eval import compute_metrics

    run_id = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or project_root / "3-analytics" / "artifacts" / f"full_history_{run_id}"
    output_dir.mkdir(parents=True, exist_ok=False)
    spark_tmp_dir = project_root / ".spark_tmp"
    spark_tmp_dir.mkdir(parents=True, exist_ok=True)

    host = args.warehouse_host
    hdfs_uri = f"hdfs://{host}:8020"
    builder = (
        SparkSession.builder.appName("NYC_Taxi_Full_History_Model_Evaluation")
        .master(args.master)
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.local.dir", str(spark_tmp_dir).replace("\\", "/"))
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{host}:9083")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.ipc.client.connect.timeout", "10000")
        .config("spark.hadoop.ipc.client.connect.max.retries", "1")
        .config("spark.hadoop.ipc.client.connect.retry.interval", "1000")
        .config("spark.sql.warehouse.dir", f"{hdfs_uri}/opt/hive/data/warehouse")
    )
    if hosts_option:
        builder = builder.config("spark.driver.extraJavaOptions", hosts_option).config(
            "spark.executor.extraJavaOptions", hosts_option
        )
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    started_at = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    status_path = output_dir / "run_status.json"
    status = {"status": "running", "started_at": started_at, "output_dir": str(output_dir)}
    status_path.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    daily = test_with_baseline = predictions = None
    try:
        physical_clean = _physical_clean_table(args.clean_table)
        if args.aggregate_checkpoint is not None:
            source_checkpoint = args.aggregate_checkpoint.resolve()
            if not source_checkpoint.is_dir():
                raise FileNotFoundError(f"Aggregate checkpoint does not exist: {source_checkpoint}")
            logging.info(f"[RESUME] Loading daily aggregate from {source_checkpoint}.")
            daily = spark.read.parquet(_local_file_uri(source_checkpoint)).persist(StorageLevel.DISK_ONLY)
            checkpoint_metadata = daily.agg(
                F.min(F.date_format("date", "yyyy-MM")).alias("period_start"),
                F.max(F.date_format("date", "yyyy-MM")).alias("period_end"),
                F.countDistinct(F.date_format("date", "yyyy-MM")).alias("month_count"),
                F.sum("label").alias("trip_rows"),
            ).first()
            first_month = checkpoint_metadata["period_start"]
            last_month = checkpoint_metadata["period_end"]
            shared_month_count = int(checkpoint_metadata["month_count"] or 0)
            included_trip_rows = int(checkpoint_metadata["trip_rows"] or 0)
            daily_rows = daily.count()
            if not first_month or not last_month or not shared_month_count:
                raise ValueError(f"Aggregate checkpoint is empty: {source_checkpoint}")
            logging.info(
                f"[RESUME] {first_month} → {last_month} ({shared_month_count} months), "
                f"{included_trip_rows:,} trips → {daily_rows:,} daily zone-hour rows."
            )
        else:
            shared_months = _shared_months(spark, args.clean_table, args.raw_table)
            if not shared_months:
                raise RuntimeError("No shared clean/raw partitions were discovered.")
            first_month, last_month = shared_months[0], shared_months[-1]
            shared_month_count = len(shared_months)
            first_year, first_month_number = map(int, first_month.split("-"))
            last_year, last_month_number = map(int, last_month.split("-"))
            partition_filter = (
                (F.col("year") > first_year)
                | ((F.col("year") == first_year) & (F.col("month") >= first_month_number))
            ) & (
                (F.col("year") < last_year)
                | ((F.col("year") == last_year) & (F.col("month") <= last_month_number))
            )

            # Read the normalized physical table instead of the compatibility view.
            # The view exposes TLC timestamp aliases and forces wider timestamp
            # column reads; the physical table already stores pickup_date, hour,
            # day_of_week and trip_duration_min as compact columns.
            trips = spark.table(physical_clean)
            required = {
                "pickup_location_id",
                "pickup_date",
                "hour",
                "day_of_week",
                "fare_amount",
                "trip_duration_min",
                "year",
                "month",
            }
            missing_columns = sorted(required.difference(trips.columns))
            if missing_columns:
                raise ValueError(f"Physical clean table lacks required columns: {', '.join(missing_columns)}")
            trips = trips.filter(
                partition_filter
                & F.col("pickup_location_id").isNotNull()
                & F.col("pickup_date").isNotNull()
                & F.col("hour").isNotNull()
                & F.col("fare_amount").isNotNull()
                & F.col("trip_duration_min").isNotNull()
            )
            trips = (
                trips.withColumnRenamed("pickup_location_id", "PULocationID")
                .withColumnRenamed("pickup_date", "date")
                .withColumnRenamed("hour", "pickup_hour")
                .filter(F.col("trip_duration_min") >= 0)
            )

            logging.info(
                f"[SOURCE] {physical_clean}: {first_month} → {last_month} "
                f"({shared_month_count} shared partitions; projected columns from the physical clean table). "
                "Starting the one-pass daily aggregation."
            )
            daily = (
                trips.groupBy("date", "PULocationID", "pickup_hour", "day_of_week")
                .agg(
                    F.count(F.lit(1)).cast("double").alias("label"),
                    F.sum("fare_amount").alias("fare_sum"),
                    F.sum("trip_duration_min").alias("duration_sum"),
                    F.count(F.lit(1)).alias("average_count"),
                )
                .withColumn("date_index", F.datediff(F.col("date"), F.lit("1970-01-01")))
                .persist(StorageLevel.DISK_ONLY)
            )
            daily_rows = daily.count()
            included_trip_rows = int(
                daily.agg(F.sum("label").alias("trip_rows")).first()["trip_rows"] or 0
            )
        logging.info(
            f"[AGGREGATION] {included_trip_rows:,} trips → {daily_rows:,} daily zone-hour rows."
        )

        aggregate_checkpoint = output_dir / "daily_zone_hour.parquet"
        daily.write.mode("errorifexists").parquet(_local_file_uri(aggregate_checkpoint))
        logging.info(f"[CHECKPOINT] Saved reusable daily aggregate: {aggregate_checkpoint}")

        logging.info("[EXPORT] Building the small Power BI analysis tables from cached aggregates.")
        analytics_summary = _weighted_summary(daily, ["PULocationID", "pickup_hour"])
        fare_by_hour = _weighted_summary(daily, ["pickup_hour"])
        heatmap = (
            daily.groupBy("pickup_hour", "day_of_week")
            .agg(F.sum("label").cast("long").alias("trip_count"))
        )
        zone_totals = (
            daily.groupBy("PULocationID")
            .agg(F.sum("label").cast("long").alias("trip_count"))
        )
        monthly = (
            daily.withColumn("month", F.date_format("date", "yyyy-MM"))
            .groupBy("month")
            .agg(F.sum("label").cast("long").alias("trip_count"))
        )
        exports = {
            "analytics_summary.csv": analytics_summary.orderBy("PULocationID", "pickup_hour"),
            "fare_travel_by_hour.csv": fare_by_hour.orderBy("pickup_hour"),
            "demand_heatmap.csv": heatmap.orderBy("day_of_week", "pickup_hour"),
            "zone_demand.csv": zone_totals.orderBy("PULocationID"),
            "monthly_trips.csv": monthly.orderBy("month"),
        }
        for filename, data in exports.items():
            _write_csv(data, output_dir / filename)
            logging.info(f"[EXPORT] {filename}")

        date_indexes = [
            int(row["date_index"])
            for row in daily.select("date_index").distinct().orderBy("date_index").collect()
        ]
        if len(date_indexes) < 2:
            raise ValueError("Full-history evaluation requires at least two distinct dates.")
        train_date_count = max(1, min(len(date_indexes) - 1, int(len(date_indexes) * args.train_ratio)))
        cutoff = date_indexes[train_date_count - 1]
        cutoff_date = dt.date(1970, 1, 1) + dt.timedelta(days=cutoff)
        train_data = daily.filter(F.col("date_index") <= cutoff)
        test_data = daily.filter(F.col("date_index") > cutoff)
        train_count, test_count = train_data.count(), test_data.count()
        if not train_count or not test_count:
            raise ValueError("Chronological split produced an empty train or test set.")

        logging.info(
            f"[SPLIT] cutoff={cutoff}; dates={train_date_count}/{len(date_indexes)}; "
            f"train={train_count:,}, test={test_count:,}."
        )
        train_data = train_data.withColumn("PULocationID", F.col("PULocationID").cast("string"))
        test_data = test_data.withColumn("PULocationID", F.col("PULocationID").cast("string"))
        test_with_baseline = add_baseline_predictions(test_data, train_data).persist(StorageLevel.DISK_ONLY)
        test_with_baseline.count()

        logging.info(f"[MODEL] Training GBTRegressor maxIter={args.max_iter}.")
        model = train_gbt(train_data, max_iter=args.max_iter, seed=args.seed)
        model_path = output_dir / "gbt_model"
        model.write().save(_local_file_uri(model_path))
        logging.info(f"[MODEL] Persisted trained GBT model to {model_path}.")
        predictions = (
            add_gbt_predictions(model, test_with_baseline)
            .persist(StorageLevel.DISK_ONLY)
        )
        predictions.count()
        metrics = [
            compute_metrics(predictions, "baseline_prediction", "zone_hour_mean_baseline"),
            compute_metrics(predictions, "gbt_prediction", "gbt_regressor"),
        ]
        logging.info(
            "[MODEL] Full-period test metrics: %s",
            json.dumps(metrics, ensure_ascii=False),
        )

        prediction_summary = (
            predictions.groupBy("PULocationID", "pickup_hour")
            .agg(
                F.sum("label").alias("actual_test_trips"),
                F.sum("baseline_prediction").alias("baseline_prediction"),
                F.sum("gbt_prediction").alias("gbt_prediction"),
                F.count(F.lit(1)).alias("test_slots"),
            )
            .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
            .orderBy("PULocationID", "pickup_hour")
        )
        _write_csv(prediction_summary, output_dir / "predictions_zone_hour.csv")
        _write_small_csv(metrics, output_dir / "evaluation_metrics.csv")

        metric_lookup = {row["model"]: row for row in metrics}
        baseline = metric_lookup["zone_hour_mean_baseline"]
        gbt = metric_lookup["gbt_regressor"]
        result = {
            "status": "complete",
            "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "started_at": started_at,
            "warehouse_host": host,
            "clean_table": args.clean_table,
            "physical_clean_table": physical_clean,
            "spark_version": spark.version,
            "spark_master": args.master,
            "spark_driver_memory": args.driver_memory,
            "spark_shuffle_partitions": args.shuffle_partitions,
            "spark_application_id": spark.sparkContext.applicationId,
            "java_home": os.environ.get("JAVA_HOME"),
            "period_start": first_month,
            "period_end": last_month,
            "shared_partition_count": shared_month_count,
            "included_trip_rows": included_trip_rows,
            "daily_zone_hour_rows": daily_rows,
            "distinct_dates": len(date_indexes),
            "train_date_count": train_date_count,
            "test_date_count": len(date_indexes) - train_date_count,
            "train_ratio": args.train_ratio,
            "train_cutoff_date_index": cutoff,
            "train_cutoff_date": cutoff_date.isoformat(),
            "train_count": train_count,
            "test_count": test_count,
            "gbt_max_iter": args.max_iter,
            "gbt_seed": args.seed,
            "mape_rule": "MAPE includes rows with actual demand > 0 only.",
            "metrics": metrics,
            "improvement_pct": {
                key: (
                    (baseline[key] - gbt[key]) / baseline[key] * 100
                    if baseline.get(key) not in (None, 0) and gbt.get(key) is not None
                    else None
                )
                for key in ("rmse", "mae", "mape")
            },
            "output_dir": str(output_dir),
            "exports": sorted(
                [
                    *exports.keys(),
                    "predictions_zone_hour.csv",
                    "evaluation_metrics.csv",
                    "daily_zone_hour.parquet",
                    "gbt_model",
                ]
            ),
            "notes": [
                "The clean Hive source was scanned once to build daily zone/hour aggregates.",
                "Monthly fare/duration and heatmap outputs are derived from the same daily aggregates.",
                "Predictions CSV is aggregated over the held-out test period by zone and hour; metrics use every test row.",
            ],
        }
        (output_dir / "evaluation_manifest.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        status_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info(f"[DONE] Saved full-history metrics and Power BI exports to {output_dir}.")
        return result
    except Exception as error:
        failure = {
            **status,
            "status": "failed",
            "failed_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "error": str(error),
            "traceback": traceback.format_exc(),
        }
        status_path.write_text(json.dumps(failure, ensure_ascii=False, indent=2), encoding="utf-8")
        raise
    finally:
        if predictions is not None:
            predictions.unpersist()
        if test_with_baseline is not None:
            test_with_baseline.unpersist()
        if daily is not None:
            daily.unpersist()
        spark.stop()


def parse_args() -> argparse.Namespace:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-host", default="192.168.100.17")
    parser.add_argument("--clean-table", default="default.clean_nyc_taxi_tlc")
    parser.add_argument("--raw-table", default="default.raw_nyc_taxi")
    parser.add_argument("--train-ratio", type=float, default=0.8)
    parser.add_argument("--max-iter", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--master", default="local[6]")
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--aggregate-checkpoint",
        type=Path,
        help="Resume exports and ML evaluation from an existing daily_zone_hour.parquet checkpoint.",
    )
    args = parser.parse_args()
    if not 0 < args.train_ratio < 1:
        parser.error("--train-ratio must be between 0 and 1")
    if args.max_iter < 1:
        parser.error("--max-iter must be at least 1")
    if args.output_dir is not None and args.output_dir.exists():
        parser.error(f"--output-dir already exists: {args.output_dir}")
    return args


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    run(parse_args())
