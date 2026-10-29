"""Chạy pipeline NYC TLC theo tháng, dừng an toàn nếu tỷ lệ loại vượt giới hạn."""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession, functions as F

from clean import clean, effective_max_drop_rate
from quality_check import set_runtime_environment
from output_validation import read_local_month_partition, verify_clean_partition
from raw_schema import PIPELINE_RAW_COLUMNS, raw_columns_ignored, standardize_raw_schema
from transform import transform
from writer import write

LOGGER = logging.getLogger("pipeline")
CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"


class DropRateExceeded(RuntimeError):
    """Vượt ngưỡng làm sạch: pipeline cố tình không ghi output, batch chỉ đánh dấu tháng đó."""

    def __init__(self, month: str, drop_rate: float, limit: float):
        super().__init__(
            f"Tỷ lệ loại {drop_rate:.2%} vượt giới hạn {limit:.2%}; dừng trước khi ghi output."
        )
        self.month = month
        self.drop_rate = drop_rate
        self.limit = limit


def load_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    with path.open(encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def create_spark(config: dict[str, Any]) -> SparkSession:
    """Tạo Spark local; Hive support chỉ bật khi sink được cấu hình là hive."""
    set_runtime_environment(config)
    builder = (
        SparkSession.builder.master(config["runtime"]["spark_master"])
        .appName("nyc-yellow-taxi-processing")
        .config("spark.local.dir", config["paths"]["spark_local_dir"])
        .config("spark.driver.memory", str(config["runtime"]["driver_memory"]))
        .config("spark.hadoop.hadoop.home.dir", config["hadoop_home"])
        .config("spark.sql.shuffle.partitions", str(config["runtime"]["shuffle_partitions"]))
        .config("spark.sql.session.timeZone", config["runtime"].get("timezone", "America/New_York"))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if config["sink"]["type"].lower() == "hive":
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_free_disk_space(month: str, config: dict[str, Any]) -> float:
    """Fail clearly when the Spark/output drive has less than configured headroom."""
    spark_dir = Path(config["paths"]["spark_local_dir"])
    drive = spark_dir.anchor or str(spark_dir)
    free_bytes = shutil.disk_usage(drive).free
    free_gb = free_bytes / (1024**3)
    required_gb = float(config["runtime"].get("min_free_disk_gb", 8))
    if free_gb < required_gb:
        raise RuntimeError(
            f"Tháng {month}: ổ đĩa {drive} còn {free_gb:.2f} GB trống; "
            f"cần tối thiểu {required_gb:.2f} GB (kiểm tra Spark temp: {spark_dir})."
        )
    return free_gb


def get_free_disk_gb(config: dict[str, Any]) -> float:
    spark_dir = Path(config["paths"]["spark_local_dir"])
    drive = spark_dir.anchor or str(spark_dir)
    return shutil.disk_usage(drive).free / (1024**3)


def write_schema_doc(df, config: dict[str, Any]) -> None:
    """Sinh tài liệu từ schema thật của DataFrame đầu ra."""
    meanings = {
        "pickup_datetime": "Thời điểm đón theo giờ địa phương của chuyến.",
        "dropoff_datetime": "Thời điểm trả khách theo giờ địa phương của chuyến.",
        "year": "Năm pickup; partition năm.",
        "month": "Tháng pickup; partition tháng.",
        "day": "Ngày trong tháng theo pickup.",
        "hour": "Giờ trong ngày theo pickup (0-23).",
        "day_of_week": "Thứ theo Spark dayofweek: 1=Chủ nhật, 7=Thứ bảy.",
        "pickup_location_id": "Mã TLC taxi zone tại điểm đón.",
        "pickup_borough": "Quận tại điểm đón từ Taxi Zone Lookup.",
        "pickup_zone": "Tên taxi zone tại điểm đón.",
        "dropoff_location_id": "Mã TLC taxi zone tại điểm trả.",
        "dropoff_borough": "Quận tại điểm trả từ Taxi Zone Lookup.",
        "dropoff_zone": "Tên taxi zone tại điểm trả.",
        "trip_distance": "Quãng đường khai báo, đơn vị dặm; giá trị 0 được chuyển thành null.",
        "is_distance_missing": "True khi trip_distance gốc bằng 0 trước chuẩn hóa.",
        "is_short_trip": "True khi thời lượng chuyến dưới ngưỡng short_trip_max_minutes; dòng vẫn được giữ.",
        "is_unknown_zone": "True khi một LocationID không có trong lookup, hoặc lookup ghi Borough 'Unknown' / Zone 'N/A' cho zone đó.",
        "trip_duration_min": "Thời lượng từ pickup đến dropoff, đơn vị phút.",
        "passenger_count": "Số hành khách khai báo; 0 được đổi thành null; chỉ mô tả, không dùng làm feature.",
        "fare_amount": "Giá cước tính theo đồng USD.",
        "total_amount": "Tổng tiền chuyến tính theo đồng USD.",
        "tip_amount": "Tiền tip tính theo đồng USD.",
        "tolls_amount": "Phí cầu đường tính theo đồng USD.",
    }
    lines = [
        "# Schema dữ liệu sạch",
        "",
        "Schema dưới đây được sinh từ DataFrame đầu ra thực tế của pipeline.",
        "",
        "| Cột | Kiểu Spark | Ý nghĩa |",
        "|---|---|---|",
    ]
    for field in df.schema.fields:
        description = meanings.get(field.name, "")
        lines.append(f"| `{field.name}` | `{field.dataType.simpleString()}` | {description} |")
    path = Path(config["paths"]["schema_doc"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_pipeline(months: list[str], config: dict[str, Any]) -> list[dict[str, Any]]:
    """Làm sạch, transform, ghi dữ liệu cho danh sách tháng."""
    for month in months:
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
            raise ValueError(f"Tháng không hợp lệ: {month}; dùng YYYY-MM")

    results = []
    for month in months:
        check_free_disk_space(month, config)
        source_path = Path(config["paths"]["raw_dir"]) / f"yellow_tripdata_{month}.parquet"
        zones_path = Path(config["paths"]["raw_dir"]) / config["source"]["zones_filename"]
        if not zones_path.is_file():
            raise FileNotFoundError(f"Không thấy Taxi Zone Lookup: {zones_path}")
        if not source_path.is_file():
            raise FileNotFoundError(f"Không thấy dữ liệu tháng {month}: {source_path}")

        base_spark_dir = Path(config["paths"]["spark_local_dir"])
        base_spark_dir.mkdir(parents=True, exist_ok=True)
        month_spark_dir = Path(tempfile.mkdtemp(prefix=f"{month}-", dir=base_spark_dir))
        month_config = dict(config)
        month_config["paths"] = {**config["paths"], "spark_local_dir": str(month_spark_dir)}
        month_config["processing_month"] = month
        spark = None
        started = time.perf_counter()
        try:
            spark = create_spark(month_config)
            zones = (
                spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(zones_path))
            )
            raw_source = spark.read.parquet(str(source_path))
            ignored_columns = raw_columns_ignored(raw_source)
            raw_standardized = standardize_raw_schema(raw_source, context=month)
            LOGGER.info(
                "month=%s raw_columns_ignored=%s standardized_but_unused=%s",
                month,
                ignored_columns,
                ["airport_fee"],
            )
            # Keep the cleaner's input and duplicate semantics limited to fields
            # consumed by the existing cleaning/transform rules.
            raw = raw_standardized.select(*PIPELINE_RAW_COLUMNS)
            cleaned, stats = clean(raw, month_config)

            drop_limit = effective_max_drop_rate(config["cleaning"], month)
            if stats["drop_rate"] > drop_limit:
                raise DropRateExceeded(month, stats["drop_rate"], drop_limit)
            if stats["clean_rows"] == 0:
                raise RuntimeError(f"Không còn dòng sau làm sạch cho tháng {month}.")

            transformed = transform(cleaned, zones, config)
            write_schema_doc(transformed, config)
            check_free_disk_space(month, month_config)
            destination = write(transformed, config, spark)
            if config["sink"]["type"].lower() == "local":
                read_back = read_local_month_partition(spark, destination, month)
            else:
                read_back = spark.table(destination).filter(
                    (F.col("year") == int(month[:4])) & (F.col("month") == int(month[5:]))
                )
            report = verify_clean_partition(
                read_back, config, month, stats["clean_rows"], transformed
            )
            if report["rows"] == 0:
                raise RuntimeError(f"Output transform rỗng cho tháng {month}.")

            stats["output_rows"] = report["rows"]
            stats["destination"] = destination
            # Count cờ đã đo từ partition đọc lại trên đĩa ngay tại đây, batch không cần Spark lần hai.
            stats["invalid"] = report["invalid"]
            stats["missing"] = report["missing"]
            stats["is_distance_missing"] = report["is_distance_missing"]
            stats["is_unknown_zone"] = report["is_unknown_zone"]
            stats["is_short_trip"] = report["is_short_trip"]
            LOGGER.info(
                "month=%s raw=%s clean=%s removed=%s drop_rate=%.4f%% destination=%s",
                month,
                stats["raw_rows"],
                stats["clean_rows"],
                stats["total_removed"],
                stats["drop_rate"] * 100,
                destination,
            )
            for rule in stats["rules"]:
                rule_rate = rule["removed"] / stats["raw_rows"] if stats["raw_rows"] else 0
                LOGGER.info(
                    "summary month=%s rule=%s removed=%s distance_set_null=%s raw_rate=%.4f%%",
                    month,
                    rule["rule"],
                    rule["removed"],
                    rule["distance_set_null"],
                    rule_rate * 100,
                )
        finally:
            if spark is not None:
                spark.stop()
            if month_spark_dir.exists():
                shutil.rmtree(month_spark_dir)

        stats["elapsed_seconds"] = time.perf_counter() - started
        stats["disk_free_gb_after"] = get_free_disk_gb(config)
        results.append(stats)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi Data Processing")
    parser.add_argument("--months", nargs="+", help="Danh sách tháng YYYY-MM")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config(args.config)
    months = args.months or config.get("months", [])
    if not months:
        parser.error("Cần khai báo tháng trong config.yaml hoặc truyền --months")
    try:
        run_pipeline(months, config)
    except Exception:
        LOGGER.exception("Pipeline thất bại")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
