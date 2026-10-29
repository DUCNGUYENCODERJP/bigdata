"""Batch runner cho NYC TLC Yellow Taxi: tháng mới trước, lỗi được ghi log và bỏ qua."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shutil
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import requests

# Pipeline modules use sibling-module imports, so make the source directory importable
# whether this file is run as a script or imported from the test package.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pipeline import CONFIG_PATH, DropRateExceeded, create_spark, load_config, run_pipeline
from output_validation import read_local_month_partition, verify_clean_partition
from clean import effective_max_drop_rate

LOGGER = logging.getLogger("batch_runner")
RUN_LOG_FIELDS = [
    "month", "timestamp", "raw", "clean", "drop_rate", "rule_removed",
    "distance_set_null", "is_distance_missing", "is_unknown_zone", "is_short_trip",
    "seconds", "invalid", "missing", "raw_bytes", "output_bytes", "status", "error",
]
NETWORK_ERROR_CODES = {403, 429}
_LAST_HTTP_REQUEST_AT: float | None = None


def month_range(start: str, end: str) -> list[str]:
    for value in (start, end):
        if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
            raise ValueError(f"Invalid month {value!r}; expected YYYY-MM")
    start_y, start_m = map(int, start.split("-"))
    end_y, end_m = map(int, end.split("-"))
    if (start_y, start_m) > (end_y, end_m):
        raise ValueError(f"--from {start} must not be after --to {end}")
    result = []
    year, month = start_y, start_m
    while (year, month) <= (end_y, end_m):
        result.append(f"{year:04d}-{month:02d}")
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return result


def batch_months(start: str, end: str, oldest_first: bool = False) -> list[str]:
    """Danh sách tháng theo thứ tự batch: mặc định mới nhất trước."""
    months = month_range(start, end)
    return months if oldest_first else months[::-1]


def load_manifest(path: Path) -> dict[str, dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as manifest_file:
        rows = csv.DictReader(manifest_file)
        result = {}
        for row in rows:
            month = row.get("month", "")
            if re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
                result[month] = row
    return result


def file_tree_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def partition_path(clean_dir: Path, month: str) -> Path:
    year, month_number = (int(part) for part in month.split("-"))
    return clean_dir / "trips" / f"year={year}" / f"month={month_number}"


def output_bytes(path: Path) -> int:
    return file_tree_bytes(path)


def append_run_log(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as log_file:
        writer = csv.DictWriter(log_file, fieldnames=RUN_LOG_FIELDS, extrasaction="ignore")
        if needs_header:
            writer.writeheader()
        writer.writerow({field: record.get(field, "") for field in RUN_LOG_FIELDS})


def latest_done_record(path: Path, month: str) -> dict[str, str] | None:
    if not path.is_file():
        return None
    with path.open(newline="", encoding="utf-8-sig") as log_file:
        matches = [
            row for row in csv.DictReader(log_file)
            if row.get("month") == month and row.get("status") == "done"
        ]
    return matches[-1] if matches else None


def _exact_http_error(response: requests.Response) -> str:
    body = response.content[:300].decode(response.encoding or "utf-8", errors="replace")
    return (
        f"HTTP {response.status_code}; headers={dict(response.headers)!r}; "
        f"body_first_300={body!r}"
    )


def download_with_policy(url: str, destination: Path, expected_size: int) -> int:
    """Sequential requests download with the project-approved retry/backoff policy."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    backoffs = (30, 120, 600)
    attempts = len(backoffs) + 1
    global _LAST_HTTP_REQUEST_AT
    for attempt in range(attempts):
        if _LAST_HTTP_REQUEST_AT is not None:
            # Enforce at least two seconds between requests; retry backoff is longer.
            delay = max(2, backoffs[attempt - 1]) if attempt else 2
            elapsed = time.monotonic() - _LAST_HTTP_REQUEST_AT
            if elapsed < delay:
                time.sleep(delay - elapsed)
        try:
            _LAST_HTTP_REQUEST_AT = time.monotonic()
            response = requests.get(url, headers={}, stream=True, timeout=(30, 180))
            if response.status_code in NETWORK_ERROR_CODES or response.status_code >= 500:
                detail = _exact_http_error(response)
                LOGGER.error("Request failure for %s: %s", url, detail)
                response.close()
                if attempt == attempts - 1:
                    raise RuntimeError(detail)
                continue
            response.raise_for_status()
            with partial.open("wb") as output:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        output.write(chunk)
            response.close()
            actual_size = partial.stat().st_size
            if actual_size != expected_size:
                partial.unlink(missing_ok=True)
                destination.unlink(missing_ok=True)
                raise IOError(
                    f"Downloaded size mismatch for {destination.name}: "
                    f"expected {expected_size}, got {actual_size}"
                )
            partial.replace(destination)
            return actual_size
        except (requests.RequestException, OSError) as error:
            partial.unlink(missing_ok=True)
            if isinstance(error, OSError) and str(error).startswith("Downloaded size mismatch"):
                raise
            if attempt == attempts - 1:
                raise
            LOGGER.warning("Download attempt %d/%d failed for %s: %s", attempt + 1, attempts, url, error)
            # Ordinary transport errors use the same bounded backoff.
    raise RuntimeError(f"Download failed after {attempts} attempts: {url}")


def validate_existing_partition(month: str, config: dict[str, Any]) -> dict[str, Any]:
    """Read and postcheck one partition without changing it."""
    clean_dir = Path(config["paths"]["clean_dir"])
    destination = clean_dir / "trips"
    part = partition_path(clean_dir, month)
    if not part.is_dir():
        raise FileNotFoundError(f"Output partition not found: {part}")
    spark_base = Path(config["paths"]["spark_local_dir"])
    spark_base.mkdir(parents=True, exist_ok=True)
    validation_spark_dir = Path(tempfile.mkdtemp(prefix=f"validate-{month}-", dir=spark_base))
    validation_config = dict(config)
    validation_config["paths"] = {**config["paths"], "spark_local_dir": str(validation_spark_dir)}
    spark = None
    try:
        spark = create_spark(validation_config)
        frame = read_local_month_partition(spark, str(destination), month)
        # Tháng chuẩn schema lấy từ config (validation.schema_baseline_month), không hardcode.
        baseline_month = str(config.get("validation", {}).get("schema_baseline_month", month))
        if month == baseline_month:
            expected_schema = frame
        else:
            baseline_path = partition_path(clean_dir, baseline_month)
            if not baseline_path.is_dir():
                raise RuntimeError(
                    "Cannot validate output schema: baseline partition "
                    f"{baseline_month} (validation.schema_baseline_month) is missing"
                )
            expected_schema = read_local_month_partition(spark, str(destination), baseline_month)
        rows = frame.count()
        report = verify_clean_partition(frame, config, month, rows, expected_schema)
        return {
            "clean": report["rows"],
            "invalid": report["invalid"],
            "missing": report["missing"],
            "is_distance_missing": report["is_distance_missing"],
            "is_unknown_zone": report["is_unknown_zone"],
            "is_short_trip": report["is_short_trip"],
            "output_bytes": output_bytes(part),
        }
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(validation_spark_dir, ignore_errors=True)


def _disk_report(config: dict[str, Any], disk_usage_fn: Callable = shutil.disk_usage) -> dict[str, Any]:
    spark_path = Path(config["paths"]["spark_local_dir"])
    result = {}
    for drive in ("C:\\", "D:\\"):
        usage = disk_usage_fn(drive)
        result[drive[0]] = usage.free
    result["spark_temp_bytes"] = file_tree_bytes(spark_path)
    return result


def _check_capacity(month: str, config: dict[str, Any], disk_usage_fn: Callable) -> dict[str, Any]:
    report = _disk_report(config, disk_usage_fn)
    required = float(config["runtime"].get("min_free_disk_gb", 8)) * 1024**3
    for drive in ("C", "D"):
        if report[drive] < required:
            free_gb = report[drive] / 1024**3
            threshold = required / 1024**3
            raise RuntimeError(
                f"Month {month}: drive {drive}:\\ has {free_gb:.2f} GB free; "
                f"minimum configured is {threshold:.2f} GB. "
                f"Spark temp {config['paths']['spark_local_dir']} uses "
                f"{report['spark_temp_bytes']} bytes."
            )
    return report


def _record_base(month: str, now: Callable[[], datetime]) -> dict[str, Any]:
    return {"month": month, "timestamp": now().astimezone().isoformat(timespec="seconds")}


def _skip_record(
    month: str,
    manifest_row: dict[str, Any],
    metrics: dict[str, Any],
    log_path: Path,
    raw_path: Path,
    now: Callable[[], datetime],
) -> dict[str, Any]:
    previous = latest_done_record(log_path, month) or {}
    raw_bytes = raw_path.stat().st_size if raw_path.is_file() else int(manifest_row["size_bytes"])
    return {
        **_record_base(month, now),
        "raw": previous.get("raw") or manifest_row.get("row_count", ""),
        "clean": metrics["clean"],
        "drop_rate": previous.get("drop_rate", ""),
        "rule_removed": previous.get("rule_removed", ""),
        "distance_set_null": previous.get("distance_set_null", ""),
        "is_distance_missing": metrics["is_distance_missing"],
        "is_unknown_zone": metrics["is_unknown_zone"],
        "is_short_trip": metrics["is_short_trip"],
        "seconds": "",
        "invalid": metrics["invalid"],
        "missing": metrics["missing"],
        "raw_bytes": raw_bytes,
        "output_bytes": metrics["output_bytes"],
        "status": "skipped",
        "error": "",
    }


def _normalise_pipeline_stats(stats: dict[str, Any], month: str, config: dict[str, Any], now) -> dict[str, Any]:
    rules = {row["rule"]: int(row.get("removed", 0)) for row in stats.get("rules", [])}
    distance_set_null = sum(int(row.get("distance_set_null", 0)) for row in stats.get("rules", []))
    raw_path = Path(config["paths"]["raw_dir"]) / f"yellow_tripdata_{month}.parquet"
    part = partition_path(Path(config["paths"]["clean_dir"]), month)
    return {
        **_record_base(month, now),
        "raw": stats.get("raw_rows", ""),
        "clean": stats.get("output_rows", stats.get("clean_rows", "")),
        "drop_rate": stats.get("drop_rate", ""),
        "rule_removed": json.dumps(rules, separators=(",", ":"), sort_keys=True),
        "distance_set_null": distance_set_null,
        "is_distance_missing": stats.get("is_distance_missing", ""),
        "is_unknown_zone": stats.get("is_unknown_zone", ""),
        "is_short_trip": stats.get("is_short_trip", ""),
        "seconds": stats.get("elapsed_seconds", ""),
        "invalid": stats.get("invalid", 0),
        "missing": stats.get("missing", 0),
        "raw_bytes": raw_path.stat().st_size if raw_path.is_file() else "",
        "output_bytes": output_bytes(part),
        "status": "done",
        "error": "",
    }


def dry_run_plan(
    months: list[str],
    config: dict[str, Any],
    manifest: dict[str, dict[str, Any]],
    *,
    force: bool = False,
    validate_fn: Callable[[str, dict[str, Any]], dict[str, Any]] = validate_existing_partition,
) -> dict[str, Any]:
    """Estimate the batch without Spark or network: dry-run phải xong trong vài giây."""
    clean_dir = Path(config["paths"]["clean_dir"])
    raw_dir = Path(config["paths"]["raw_dir"])
    estimates = config.get("batch", {})
    fixed_seconds = float(estimates.get("fixed_seconds_per_month", 25.6))
    seconds_per_million = float(estimates.get("seconds_per_million_raw_rows", 13.97))
    bytes_per_row = float(estimates.get("output_bytes_per_row", 22.3))
    download_rate = float(estimates.get("download_bytes_per_second", 1_810_000))
    plan = []
    for month in months:
        if month not in manifest:
            raise KeyError(f"Month {month} is absent from source_manifest.csv")
        part = partition_path(clean_dir, month)
        row = manifest[month]
        expected_bytes = int(row["size_bytes"])
        raw_path = raw_dir / f"yellow_tripdata_{month}.parquet"
        needs_download = not raw_path.is_file() or raw_path.stat().st_size != expected_bytes
        if not part.is_dir():
            status = "run"
        elif force:
            # --force luôn ghi lại; dry-run không mở Spark để kiểm tra schema từng tháng.
            status = "rerun"
        else:
            # Partition đã có: chỉ chắc chắn bỏ qua nếu chạy thật vượt postcheck,
            # nên đánh dấu "check" thay vì mở một phiên Spark cho mỗi tháng.
            status = "check"
        row_count = int(row.get("row_count") or 0)
        plan.append({
            "month": month,
            "status": status,
            "raw_bytes_to_download": expected_bytes if needs_download else 0,
            "raw_bytes": expected_bytes,
            "row_count": row_count,
            "estimated_output_bytes": int(row_count * bytes_per_row),
        })
    active = [row for row in plan if row["status"] in {"run", "rerun"}]
    download_bytes = sum(row["raw_bytes_to_download"] for row in active)
    return {
        "months": plan,
        "raw_bytes_to_download": download_bytes,
        "estimated_output_bytes": sum(row["estimated_output_bytes"] for row in active),
        "estimated_seconds": sum(
            fixed_seconds + seconds_per_million * row["row_count"] / 1_000_000 for row in active
        ),
        "estimated_download_seconds": download_bytes / download_rate if download_rate else 0.0,
        "run_count": len(active),
        "skip_count": len(plan) - len(active),
        "counts_by_status": {
            status: sum(1 for entry in plan if entry["status"] == status)
            for status in ("run", "rerun", "check")
        },
    }


def run_months(
    months: list[str],
    config: dict[str, Any],
    manifest: dict[str, dict[str, Any]],
    *,
    force: bool = False,
    dry_run: bool = False,
    log_path: Path | None = None,
    pipeline_fn: Callable[[list[str], dict[str, Any]], list[dict[str, Any]]] = run_pipeline,
    validate_fn: Callable[[str, dict[str, Any]], dict[str, Any]] = validate_existing_partition,
    download_fn: Callable[[str, Path, int], int] = download_with_policy,
    disk_usage_fn: Callable = shutil.disk_usage,
    now: Callable[[], datetime] = datetime.now,
) -> dict[str, Any] | list[dict[str, Any]]:
    if dry_run:
        return dry_run_plan(months, config, manifest, force=force, validate_fn=validate_fn)
    log_path = log_path or Path(config["paths"]["reports_dir"]) / "run_log.csv"
    raw_dir = Path(config["paths"]["raw_dir"])
    records: list[dict[str, Any]] = []
    for month in months:
        if month not in manifest:
            raise KeyError(f"Month {month} is absent from source_manifest.csv")
        row = manifest[month]
        raw_path = raw_dir / f"yellow_tripdata_{month}.parquet"
        part = partition_path(Path(config["paths"]["clean_dir"]), month)
        try:
            if part.is_dir() and not force:
                try:
                    metrics = validate_fn(month, config)
                except Exception as error:
                    # Partition cũ không khớp code hiện tại (thiếu cột, sai schema, hỏng dữ liệu):
                    # coi như chưa xong và ghi lại tháng đó, không dừng cả batch.
                    LOGGER.warning("%s: postcheck that bai (%s); ghi lại partition", month, error)
                else:
                    record = _skip_record(month, row, metrics, log_path, raw_path, now)
                    append_run_log(log_path, record)
                    records.append(record)
                    LOGGER.info("%s skipped: postcheck passed", month)
                    continue
            if part.is_dir() and force:
                LOGGER.info("%s: --force requested; rerunning completed partition", month)

            _check_capacity(month, config, disk_usage_fn)
            expected_size = int(row["size_bytes"])
            if not raw_path.is_file() or raw_path.stat().st_size != expected_size:
                url = row.get("url") or config["source"]["trip_url_template"].format(month=month)
                for attempt in range(2):
                    if attempt:
                        raw_path.unlink(missing_ok=True)
                        raw_path.with_name(raw_path.name + ".part").unlink(missing_ok=True)
                        time.sleep(2)
                    try:
                        download_fn(url, raw_path, expected_size)
                    except OSError as error:
                        is_size_mismatch = "size mismatch" in str(error).lower()
                        if is_size_mismatch and attempt == 0:
                            raw_path.unlink(missing_ok=True)
                            raw_path.with_name(raw_path.name + ".part").unlink(missing_ok=True)
                            continue
                        raise
                    actual_size = raw_path.stat().st_size if raw_path.is_file() else -1
                    if actual_size == expected_size:
                        break
                    raw_path.unlink(missing_ok=True)
                    raw_path.with_name(raw_path.name + ".part").unlink(missing_ok=True)
                    if attempt == 1:
                        raise IOError(
                            f"Month {month}: raw size does not match manifest after one redownload; "
                            f"expected {expected_size}, got {actual_size}"
                        )
                else:
                    raise IOError(
                        f"Month {month}: raw size does not match manifest after one redownload; "
                        f"expected {expected_size}, got "
                        f"{raw_path.stat().st_size if raw_path.exists() else 'missing'}"
                    )

            results = pipeline_fn([month], config)
            if len(results) != 1:
                raise RuntimeError(f"Pipeline returned {len(results)} result rows for {month}; expected 1")
            stats = results[0]
            # pipeline đã verify partition đọc lại từ đĩa và trả count cờ trong stats,
            # nên batch không mở thêm một phiên Spark thứ hai cho tháng vừa ghi.
            record = _normalise_pipeline_stats(stats, month, config, now)
            drop_limit = effective_max_drop_rate(config["cleaning"], month)
            if float(stats.get("drop_rate", 0)) > drop_limit:
                # run_pipeline luôn ném DropRateExceeded trước khi ghi output, nên không
                # tự sửa ngưỡng ở đây; nếu stats vẫn vượt ngưỡng thì đó là tín hiệu phải báo.
                record["error"] = (
                    f"Vượt ngưỡng làm sạch: tỷ lệ loại {float(stats['drop_rate']):.2%} "
                    f"lớn hơn giới hạn {drop_limit:.2%}."
                )
                record["status"] = "flagged"
            append_run_log(log_path, record)
            records.append(record)
            LOGGER.info("%s %s: clean=%s, drop_rate=%.4f", month, record["status"], record["clean"], record["drop_rate"])
        except Exception as error:
            # Một tháng lỗi không được xóa quyền xử lý 187 tháng còn lại; mọi thứ ghi vào run_log.csv.
            status = "failed"
            if isinstance(error, DropRateExceeded):
                # Vượt ngưỡng làm sạch là tín hiệu cần người xem, không phải sự cố kỹ thuật.
                status = "flagged"
            flagged = {
                **_record_base(month, now),
                "raw": row.get("row_count", ""),
                "raw_bytes": raw_path.stat().st_size if raw_path.is_file() else "",
                "status": status,
                "error": str(error),
            }
            append_run_log(log_path, flagged)
            records.append(flagged)
            LOGGER.error("%s %s: %s", month, status, error)
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run NYC TLC Yellow Taxi pipeline by month")
    parser.add_argument("--from", dest="start", default="2011-01", help="First month YYYY-MM")
    parser.add_argument("--to", dest="end", default="2026-08", help="Last month YYYY-MM")
    parser.add_argument("--force", action="store_true", help="Rerun completed partitions")
    parser.add_argument("--dry-run", action="store_true", help="Plan only; do not download or run pipeline")
    parser.add_argument(
        "--oldest-first", action="store_true",
        help="Do not reverse the range; default is newest month first",
    )
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config(args.config)
    manifest_path = args.manifest or Path(config["paths"]["reports_dir"]) / "source_manifest.csv"
    months = batch_months(args.start, args.end, oldest_first=args.oldest_first)
    try:
        plan = run_months(
            months, config, load_manifest(manifest_path),
            force=args.force, dry_run=args.dry_run,
        )
        if args.dry_run:
            for row in plan["months"]:
                print(f"{row['month']}: {row['status']}")
            print(f"run={plan['run_count']} skip={plan['skip_count']} by_status={plan['counts_by_status']}")
            print(f"raw download bytes={plan['raw_bytes_to_download']:,} ({plan['raw_bytes_to_download'] / 2**30:.1f} GiB)")
            print(f"estimated output bytes={plan['estimated_output_bytes']:,} ({plan['estimated_output_bytes'] / 2**30:.1f} GiB)")
            print(f"estimated pipeline seconds={plan['estimated_seconds']:.0f} ({plan['estimated_seconds'] / 3600:.1f} h)")
            print(f"estimated download seconds={plan['estimated_download_seconds']:.0f} ({plan['estimated_download_seconds'] / 3600:.1f} h)")
            return 0
        statuses = [record["status"] for record in plan]
        summary = {status: statuses.count(status) for status in sorted(set(statuses))}
        for status in ("failed", "flagged"):
            months_in_status = [record["month"] for record in plan if record["status"] == status]
            if months_in_status:
                print(f"{status} months ({len(months_in_status)}): {', '.join(months_in_status)}")
        print(f"batch finished: {summary}")
        # Batch không dừng vì một tháng lỗi, nhưng exit code vẫn báo cho wrapper PowerShell.
        return 1 if statuses.count("failed") else 0
    except Exception:
        LOGGER.exception("Batch run stopped")
        return 1


if __name__ == "__main__":
    sys.exit(main())
