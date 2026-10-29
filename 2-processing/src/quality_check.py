"""Thống kê schema và các bất thường của dữ liệu Yellow Taxi thô."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger("quality_check")
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "total_amount",
}


def load_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """Đọc cấu hình YAML của dự án."""
    with config_path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def set_runtime_environment(config: dict[str, Any]) -> None:
    """Đưa Java và vùng tạm của Spark về các đường dẫn đã cấu hình trên ổ D."""
    temp_dir = os.path.normpath("D:/tmp")
    spark_local_dirs = os.path.normpath(config["paths"]["spark_local_dir"])
    os.environ["TMP"] = temp_dir
    os.environ["TEMP"] = temp_dir
    os.environ["SPARK_LOCAL_DIRS"] = spark_local_dirs
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    hadoop_home = config.get("hadoop_home")
    if hadoop_home:
        hadoop_home = str(Path(hadoop_home))
        hadoop_bin = str(Path(hadoop_home) / "bin")
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        java_options = os.environ.get("JAVA_TOOL_OPTIONS", "")
        java_options = " ".join(
            option
            for option in java_options.split()
            if not option.startswith("-Dhadoop.home.dir=")
        )
        os.environ["JAVA_TOOL_OPTIONS"] = (
            f"{java_options} -Dhadoop.home.dir={hadoop_home}".strip()
        )
    java_home = config.get("runtime", {}).get("java_home")
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        java_bin = str(Path(java_home) / "bin")
        path_entries = [
            entry
            for entry in os.environ["PATH"].split(os.pathsep)
            if entry and entry.rstrip("\\/").casefold() != java_bin.rstrip("\\/").casefold()
        ]
        os.environ["PATH"] = os.pathsep.join([java_bin, *path_entries])
    if hadoop_home:
        path_entries = [
            entry
            for entry in os.environ["PATH"].split(os.pathsep)
            if entry and entry.rstrip("\\/").casefold() != hadoop_bin.rstrip("\\/").casefold()
        ]
        os.environ["PATH"] = os.pathsep.join([hadoop_bin, *path_entries])


def _percent(count: int, total: int) -> str:
    return f"{count / total * 100:.4f}%" if total else "0.0000%"


def build_report(
    month: str,
    total_rows: int,
    schema_text: str,
    null_counts: dict[str, int],
    issue_counts: dict[str, int],
    source_path: Path,
) -> str:
    """Tạo cùng một nội dung Markdown để in console và ghi file."""
    lines = [
        f"# Báo cáo chất lượng Yellow Taxi {month}",
        "",
        f"- Thời điểm kiểm tra: {datetime.now().astimezone().isoformat(timespec='seconds')}",
        f"- File: `{source_path}`",
        f"- Tổng số dòng raw: **{total_rows:,}**",
        "",
        "## Schema Parquet",
        "",
        "```text",
        schema_text.rstrip(),
        "```",
        "",
        "## Null theo cột",
        "",
        "| Cột | Null | Tỷ lệ trên raw |",
        "|---|---:|---:|",
    ]
    lines.extend(
        f"| `{column}` | {count:,} | {_percent(count, total_rows)} |"
        for column, count in null_counts.items()
    )
    lines.extend(
        [
            "",
            "## Bất thường và các mốc chẩn đoán",
            "",
            "Các tỷ lệ bên dưới dùng tổng raw làm mẫu số. Một dòng có thể vi phạm nhiều quy tắc; không cộng các hàng để suy ra số dòng duy nhất bị ảnh hưởng.",
            "",
            "| Quy tắc / mốc | Số dòng | Tỷ lệ trên raw |",
            "|---|---:|---:|",
        ]
    )
    lines.extend(
        f"| {name} | {count:,} | {_percent(count, total_rows)} |"
        for name, count in issue_counts.items()
    )
    lines.extend(
        [
            "",
            "## Ghi chú",
            "",
            "Các mốc thời lượng và quãng đường là phép đo phân bố để thảo luận với người dùng, chưa phải ngưỡng làm sạch. Báo cáo không loại hoặc sửa bất kỳ dòng nào.",
            "",
        ]
    )
    return "\n".join(lines)


def run_quality_check(month: str, config: dict[str, Any]) -> Path:
    """Đọc một tháng, đếm null và bất thường, rồi ghi báo cáo Markdown."""
    set_runtime_environment(config)

    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.functions import broadcast

    paths = config["paths"]
    source_path = Path(paths["raw_dir"]) / f"yellow_tripdata_{month}.parquet"
    zones_path = Path(paths["raw_dir"]) / config["source"]["zones_filename"]
    if not source_path.is_file():
        raise FileNotFoundError(f"Không thấy file Parquet: {source_path}")
    if not zones_path.is_file():
        raise FileNotFoundError(f"Không thấy file zone lookup: {zones_path}")

    spark = (
        SparkSession.builder.master(config["runtime"]["spark_master"])
        .appName(f"nyc-taxi-quality-{month}")
        .config("spark.local.dir", paths["spark_local_dir"])
        .config("spark.driver.memory", str(config["runtime"]["driver_memory"]))
        .config("spark.sql.shuffle.partitions", str(config["runtime"]["shuffle_partitions"]))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        trips = spark.read.parquet(str(source_path))
        total_rows = trips.count()
        missing = REQUIRED_COLUMNS.difference(trips.columns)
        if missing:
            raise ValueError(f"Thiếu cột bắt buộc trong Parquet: {sorted(missing)}")

        pickup = F.col("tpep_pickup_datetime").cast("timestamp")
        dropoff = F.col("tpep_dropoff_datetime").cast("timestamp")
        duration_seconds = dropoff.cast("long") - pickup.cast("long")
        month_start = F.to_timestamp(F.lit(f"{month}-01 00:00:00"))
        next_month = F.add_months(month_start, 1)

        expressions = [
            F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(f"null_{index}")
            for index, column in enumerate(trips.columns)
        ]
        issue_names = [
            "trip_distance <= 0",
            "fare_amount < 0",
            "total_amount < 0",
            "passenger_count = 0",
            "dropoff trước pickup",
            f"pickup ngoài tháng {month}",
        ]
        issue_conditions = [
            F.col("trip_distance") <= 0,
            F.col("fare_amount") < 0,
            F.col("total_amount") < 0,
            F.col("passenger_count") == 0,
            dropoff < pickup,
            (pickup < month_start) | (pickup >= next_month),
        ]

        quality_cfg = config.get("quality_check", {})
        duration_levels = quality_cfg.get("duration_diagnostic_hours", [4, 6, 12, 24])
        distance_levels = quality_cfg.get("distance_diagnostic_miles", [50, 100, 200, 500])
        for hours in duration_levels:
            issue_names.append(f"thời lượng > {hours} giờ")
            issue_conditions.append(duration_seconds > int(hours) * 3600)
        for miles in distance_levels:
            issue_names.append(f"quãng đường > {miles} dặm")
            issue_conditions.append(F.col("trip_distance") > float(miles))

        expressions.extend(
            F.sum(F.when(condition, 1).otherwise(0)).alias(f"issue_{index}")
            for index, condition in enumerate(issue_conditions)
        )
        aggregates = trips.agg(*expressions).first().asDict()
        null_counts = {
            column: int(aggregates.get(f"null_{index}") or 0)
            for index, column in enumerate(trips.columns)
        }
        issue_counts = {
            name: int(aggregates.get(f"issue_{index}") or 0)
            for index, name in enumerate(issue_names)
        }

        zones = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(zones_path))
        )
        zone_column = "LocationID" if "LocationID" in zones.columns else "location_id"
        known_zones = broadcast(
            zones.select(F.col(zone_column).cast("int").alias("zone_id")).distinct()
        )
        for label, trip_column in (
            ("PULocationID không có trong zone lookup", "PULocationID"),
            ("DOLocationID không có trong zone lookup", "DOLocationID"),
        ):
            unknown_count = (
                trips.filter(F.col(trip_column).isNotNull())
                .join(
                    known_zones,
                    F.col(trip_column).cast("int") == F.col("zone_id"),
                    "left_anti",
                )
                .count()
            )
            issue_counts[label] = unknown_count

        schema_text = trips._jdf.schema().treeString()
        report = build_report(
            month, total_rows, schema_text, null_counts, issue_counts, source_path
        )
        report_path = Path(paths["reports_dir"]) / f"quality_{month}.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        print(report)
        LOGGER.info("Đã ghi báo cáo: %s", report_path)
        return report_path
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Kiểm tra chất lượng dữ liệu NYC TLC")
    parser.add_argument("--months", nargs="+", help="Danh sách tháng YYYY-MM")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = load_config(args.config)
    months = args.months or config.get("months", [])
    if not months:
        parser.error("Cần khai báo tháng trong config.yaml hoặc truyền --months")
    for month in months:
        run_quality_check(month, config)


if __name__ == "__main__":
    main()
