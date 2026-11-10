"""Spark SQL aggregates and forecast generation for the dashboard."""
from __future__ import annotations
import logging

import os
import time

from .context import PROJECT_ROOT
from .warehouse_metadata import (
    _month_sort_key,
    _parse_partition,
    _quote_table_name,
    _show_partition_months,
)


def _query_warehouse_impl(
    warehouse_host: str,
    partition: str,
    clean_table: str,
    raw_table: str,
) -> dict:
    """Query aggregates and train the displayed model from Hive SQL table rows."""
    logging.info(f"\n[WAREHOUSE] Kết nối Hive SQL tại {warehouse_host}:9083...")
    started = time.time()
    selected_partition = _parse_partition(partition)
    clean_source = _quote_table_name(clean_table)
    raw_source = _quote_table_name(raw_table)
    if selected_partition is None:
        # Resolve the full range from Hive partitions after the session starts.
        # This keeps cold starts current as the monthly warehouse grows.
        where_partition = None
        period = None
        model_partition = None
        where_model = None
        first_month = last_month = None
    else:
        year, month = selected_partition
        where_partition = f"year = {year} AND month = {month}"
        period = f"{year:04d}-{month:02d}"
        model_partition = partition
        where_model = where_partition
        first_month = last_month = (year, month)

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    hdfs_uri = f"hdfs://{warehouse_host}:8020"
    hosts_candidates = [
        os.path.join(os.environ["HADOOP_HOME"], "custom_hosts") if os.environ.get("HADOOP_HOME") else "",
        os.path.join(PROJECT_ROOT, "custom_hosts"),
    ]
    # _JAVA_OPTIONS is whitespace-tokenized by the JVM launcher, so a hosts file
    # under a path with spaces kills the driver JVM; fall back to real DNS (sslip.io).
    hosts_file = next(
        (path for path in hosts_candidates
         if path.strip(os.sep) and os.path.isfile(path) and " " not in path),
        "",
    )

    # Sử dụng ổ D (nơi có > 500GB trống) làm thư mục tạm cho Spark shuffle spill, tránh tràn ổ C
    spark_tmp_dir = os.path.join(PROJECT_ROOT, ".spark_tmp").replace("\\", "/")
    os.makedirs(spark_tmp_dir, exist_ok=True)

    builder = (
        SparkSession.builder.appName("NYC_Taxi_BI_Live_Server")
        .master("local[8]")
        .config("spark.driver.memory", "6g")
        .config("spark.local.dir", spark_tmp_dir)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.parquet.aggregatePushdown", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
        .config("spark.hadoop.io.file.buffer.size", "131072")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{warehouse_host}:9083")
        .config("spark.hadoop.fs.defaultFS", hdfs_uri)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.ipc.client.connect.timeout", "10000")
        .config("spark.hadoop.ipc.client.connect.max.retries", "1")
        .config("spark.hadoop.ipc.client.connect.retry.interval", "1000")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.warehouse.dir", f"{hdfs_uri}/opt/hive/data/warehouse")
    )
    if os.path.exists(hosts_file):
        hosts_option = f"-Djdk.net.hosts.file={hosts_file}"
        java_options = os.environ.get("_JAVA_OPTIONS", "")
        if hosts_option not in java_options.split():
            os.environ["_JAVA_OPTIONS"] = f"{java_options} {hosts_option}".strip()
        builder = (
            builder.config("spark.executor.extraJavaOptions", hosts_option)
        )
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if selected_partition is None:
        clean_db, separator, clean_name = clean_table.rpartition(".")
        physical_clean_table = clean_table
        if separator and clean_name.endswith("_tlc"):
            physical_clean_table = f"{clean_db}.{clean_name[:-4]}"

        # Repair metadata before choosing min/max so a newly ingested month is
        # visible on the very first dashboard load as well as to the watcher.
        for table_name in (physical_clean_table, raw_table):
            try:
                spark.sql(f"MSCK REPAIR TABLE {_quote_table_name(table_name)}")
            except Exception as error:
                logging.info(f"[PARTITIONS] Không repair được {table_name}: {error}")

        clean_months = _show_partition_months(spark, physical_clean_table)
        raw_months = _show_partition_months(spark, raw_table)
        available_months = sorted(clean_months & raw_months, key=_month_sort_key)
        if not available_months:
            raise RuntimeError(
                "Không tìm thấy partition tháng dùng chung giữa bảng clean và raw."
            )

        first_label, last_label = available_months[0], available_months[-1]
        first_month = _month_sort_key(first_label)
        last_month = _month_sort_key(last_label)
        start_year, start_month = first_month
        end_year, end_month = last_month
        where_partition = (
            f"(year > {start_year} OR (year = {start_year} AND month >= {start_month})) "
            f"AND (year < {end_year} OR (year = {end_year} AND month <= {end_month}))"
        )
        period = f"{first_label} → {last_label}"
        model_partition = f"year={end_year}/month={end_month}"
        where_model = f"year = {end_year} AND month = {end_month}"
        logging.info(
            f"[PARTITIONS] Phạm vi all: {period} ({len(available_months)} tháng chung); "
            f"mô hình dùng partition mới nhất {model_partition}."
        )

    # Tự động truy vấn dung lượng thư mục thực tế từ HDFS NameNode
    clean_hdfs_dir = f"{hdfs_uri}/data/nyc_taxi/clean/trips"
    if selected_partition:
        clean_hdfs_dir = f"{clean_hdfs_dir}/{selected_partition}"

    storage_size_str = "HDFS Parquet"
    storage_bytes = None
    storage_file_count = None
    try:
        sc = spark.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        path = sc._gateway.jvm.org.apache.hadoop.fs.Path(clean_hdfs_dir)
        fs = path.getFileSystem(conf)
        summary = fs.getContentSummary(path)
        hdfs_bytes = int(summary.getLength())
        file_cnt = int(summary.getFileCount())
        storage_bytes = hdfs_bytes
        storage_file_count = file_cnt
        def _fmt_size(b: float) -> str:
            for u in ["B", "KiB", "MiB", "GiB", "TiB"]:
                if b < 1024.0:
                    return f"{b:.2f} {u}"
                b /= 1024.0
            return f"{b:.2f} PiB"
        storage_size_str = f"{_fmt_size(hdfs_bytes)} Parquet ({file_cnt:,} files)"
    except Exception:
        storage_size_str = f"HDFS Parquet ({partition})"

    logging.info(f"[STORAGE] Dung lượng kho HDFS ghi nhận : {storage_size_str}")
    logging.info(f"[RECORDS] Đang truy vấn tổng hợp từ Hive Metastore ({partition})...")
    logging.info(f"[1/4] Truy vấn {clean_table} ({partition}) qua Spark SQL...")

    clean_aggregates = spark.sql(
        f"""
        SELECT GROUPING(year) AS g_year,
               GROUPING(pickup_borough) AS g_borough,
               GROUPING(PULocationID) AS g_zone,
               GROUPING(HOUR(tpep_pickup_datetime)) AS g_hour,
               GROUPING(DAYOFWEEK(tpep_pickup_datetime)) AS g_day,
               year,
               month,
               pickup_borough,
               PULocationID,
               MAX(pickup_zone) AS pickup_zone,
               HOUR(tpep_pickup_datetime) AS pickup_hour,
               DAYOFWEEK(tpep_pickup_datetime) AS day_of_week,
               COUNT(*) AS trip_count,
               AVG(fare_amount) AS avg_fare,
               AVG(trip_duration_min) AS avg_duration,
               SUM(fare_amount) AS total_revenue
        FROM {clean_source}
        WHERE {where_partition}
        GROUP BY GROUPING SETS (
            (year, month),
            (pickup_borough),
            (PULocationID),
            (HOUR(tpep_pickup_datetime)),
            (DAYOFWEEK(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)),
            ()
        )
        """
    ).collect()
    monthly_counts: dict[str, int] = {}
    borough_rows = []
    zone_rows = []
    hourly_rows = []
    heatmap_rows = []
    overall = None
    for row in clean_aggregates:
        if not row["g_year"]:
            monthly_counts[f"{int(row['year']):04d}-{int(row['month']):02d}"] = int(row["trip_count"])
        elif not row["g_borough"]:
            borough_rows.append(row)
        elif not row["g_zone"]:
            zone_rows.append(row)
        elif not row["g_hour"]:
            if not row["g_day"]:
                heatmap_rows.append(row)
            else:
                hourly_rows.append(row)
        else:
            overall = row

    month_labels = []
    current_year, current_month = first_month
    while (current_year, current_month) <= last_month:
        month_labels.append(f"{current_year:04d}-{current_month:02d}")
        if current_month == 12:
            current_year, current_month = current_year + 1, 1
        else:
            current_month += 1
    monthly_data = {
        "labels": month_labels,
        "counts": [monthly_counts.get(label) for label in month_labels],
        "loaded_months": len(monthly_counts),
        "expected_months": len(month_labels),
        "missing_months": [label for label in month_labels if label not in monthly_counts],
    }

    borough_rows.sort(key=lambda row: int(row["trip_count"]), reverse=True)
    borough_total = sum(int(row["trip_count"]) for row in borough_rows)
    borough_data = {
        "labels": [row["pickup_borough"] or "Unknown" for row in borough_rows],
        "counts": [int(row["trip_count"]) for row in borough_rows],
        "percentages": [
            round(int(row["trip_count"]) / borough_total * 100, 2) if borough_total else 0
            for row in borough_rows
        ],
    }

    zone_rows.sort(key=lambda row: int(row["trip_count"]), reverse=True)
    zone_names = {
        int(row["PULocationID"]): row["pickup_zone"]
        for row in zone_rows
        if row["PULocationID"] is not None
    }
    top_zones_data = {
        "labels": [row["pickup_zone"] or "Unknown" for row in zone_rows[:8]],
        "counts": [int(row["trip_count"]) for row in zone_rows[:8]],
        "all_labels": [row["pickup_zone"] or "Unknown" for row in zone_rows],
        "all_counts": [int(row["trip_count"]) for row in zone_rows],
        "incremental_candidates": False,
    }

    hourly_rows.sort(key=lambda row: int(row["pickup_hour"]))
    hourly_data = {
        "hours": [f"{int(row['pickup_hour'])}h" for row in hourly_rows],
        "avg_fare": [float(row["avg_fare"] or 0) for row in hourly_rows],
        "avg_duration": [float(row["avg_duration"] or 0) for row in hourly_rows],
    }

    heatmap = [[0] * 24 for _ in range(7)]
    for row in heatmap_rows:
        day_index = int(row["day_of_week"]) - 1
        hour = int(row["pickup_hour"])
        if 0 <= day_index < 7 and 0 <= hour < 24:
            heatmap[day_index][hour] = int(row["trip_count"])

    if overall is None:
        raise ValueError(f"Không có dữ liệu clean trong khoảng {period}.")
    total_records = int(overall["trip_count"])
    avg_fare = float(overall["avg_fare"] or 0)
    avg_duration = float(overall["avg_duration"] or 0)
    total_revenue = float(overall["total_revenue"] or 0)

    logging.info(f"[2/4] Truy vấn {raw_table} ({partition})")
    raw_aggregates = spark.sql(
        f"""
        SELECT GROUPING(HOUR(tpep_pickup_datetime)) AS g_hour,
               HOUR(tpep_pickup_datetime) AS pickup_hour,
               COUNT(*) AS total_raw,
               SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS total_cancelled,
               SUM(CASE WHEN fare_amount < 0 THEN ABS(fare_amount) ELSE 0 END) AS negative_fare_total,
               SUM(
                   CASE
                       WHEN fare_amount < 0 AND tpep_pickup_datetime IS NOT NULL THEN 1
                       ELSE 0
                   END
               ) AS cancelled_with_hour
        FROM {raw_source}
        WHERE {where_partition}
        GROUP BY GROUPING SETS (
            (),
            (HOUR(tpep_pickup_datetime))
        )
        """
    ).collect()
    raw_total_row = next((row for row in raw_aggregates if row["g_hour"]), None)
    if raw_total_row is None:
        raise ValueError(f"Không có dữ liệu raw trong khoảng {period}.")
    total_raw = int(raw_total_row["total_raw"])
    total_cancelled = int(raw_total_row["total_cancelled"] or 0)
    negative_fare_total = float(raw_total_row["negative_fare_total"] or 0)
    cancelled_hourly = [0] * 24
    for row in raw_aggregates:
        if row["g_hour"] or row["pickup_hour"] is None:
            continue
        hour = int(row["pickup_hour"])
        if 0 <= hour < 24:
            cancelled_hourly[hour] = int(row["cancelled_with_hour"] or 0)
    cancelled_data = {
        "total_cancelled": total_cancelled,
        "total_raw": total_raw,
        "cancellation_rate": round(total_cancelled / total_raw * 100, 2) if total_raw else 0,
        "negative_fare_total": round(negative_fare_total, 2),
        "hourly": cancelled_hourly,
    }

    logging.info("[3/4] Huấn luyện Spark ML từ dữ liệu Hive SQL...")
    from modules.baseline_model import add_baseline_predictions
    from modules.features import build_feature_data
    from modules.gbt_model import add_gbt_predictions, train_gbt
    from modules.metrics_eval import compute_metrics
    from modules.splitter import chronological_split

    model_df = spark.sql(f"SELECT * FROM {clean_source} WHERE {where_model}")
    feature_data = build_feature_data(model_df).cache()
    train_data, test_data = chronological_split(feature_data, train_ratio=0.8)
    train_count, test_count = train_data.count(), test_data.count()
    baseline_data = add_baseline_predictions(test_data, train_data)
    model = train_gbt(train_data, max_iter=15)
    predictions = add_gbt_predictions(model, baseline_data).cache()
    baseline_metrics = compute_metrics(
        predictions, prediction_col="baseline_prediction", model_name="zone_hour_mean_baseline"
    )
    gbt_metrics = compute_metrics(
        predictions, prediction_col="gbt_prediction", model_name="gbt_regressor"
    )

    selected_zone = (
        predictions.groupBy("PULocationID")
        .count()
        .orderBy(F.desc("count"), F.asc("PULocationID"))
        .first()
    )
    if selected_zone is None:
        raise ValueError(f"Không có bản ghi dự báo trong Hive partition {partition}.")
    zone_id = int(selected_zone["PULocationID"])
    chart_rows = (
        predictions.filter(F.col("PULocationID") == zone_id)
        .orderBy("date", "pickup_hour")
        .limit(16)
        .collect()
    )
    zone_name = zone_names.get(zone_id)
    location = f"{zone_name} (khu vực {zone_id})" if zone_name else f"Khu vực {zone_id}"
    ml_data = {
        "metrics": {"baseline": baseline_metrics, "gbt": gbt_metrics},
        "model_partition": model_partition,
        "train_count": train_count,
        "test_count": test_count,
        "chart_data": {
            "time_labels": [
                f"{row['date'].isoformat()} {int(row['pickup_hour'])}h" for row in chart_rows
            ],
            "actual": [float(row["label"]) for row in chart_rows],
            "gbt": [round(float(row["gbt_prediction"]), 1) for row in chart_rows],
            "baseline": [round(float(row["baseline_prediction"]), 1) for row in chart_rows],
            "location": location,
        },
    }
    predictions.unpersist()
    feature_data.unpersist()

    result = {
        "kpis": {
            "total_records": total_records,
            "avg_fare": round(avg_fare, 2),
            "avg_duration": round(avg_duration, 2),
            "total_revenue": round(total_revenue, 2),
            "partition": partition,
            "period": period,
            "months_loaded": monthly_data["loaded_months"],
            "months_expected": monthly_data["expected_months"],
            "storage_size": storage_size_str,
            "storage_bytes": storage_bytes,
            "file_count": storage_file_count,
            "queried_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        },
        "monthly": monthly_data,
        "borough": borough_data,
        "top_zones": top_zones_data,
        "hourly": hourly_data,
        "heatmap": heatmap,
        "cancelled": cancelled_data,
        "ml": ml_data,
    }
    logging.info(f"[4/4] SQL query hoàn tất trong {time.time() - started:.2f}s.")
    return result



def query_warehouse(
    warehouse_host: str,
    partition: str,
    clean_table: str,
    raw_table: str,
) -> dict:
    """Run the warehouse query and release its Spark session on success or failure."""
    try:
        return _query_warehouse_impl(warehouse_host, partition, clean_table, raw_table)
    finally:
        try:
            from pyspark.sql import SparkSession

            active_session = SparkSession.getActiveSession()
            if active_session is not None:
                active_session.stop()
        except Exception as cleanup_error:
            logging.info(f"[SPARK] Không thể dừng Spark session: {cleanup_error}")
