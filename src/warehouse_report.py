"""Báo cáo mốc 6: xác minh toàn cây warehouse, bảng theo tháng, danh sách lỗi, sinh docs/schema.md."""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from batch_runner import load_manifest, month_range, partition_path
from pipeline import create_spark, load_config
from output_validation import read_local_month_partition

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.yaml"
PARTITION_COLUMNS = {"year", "month"}
MONTH_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def schema_signature(fields: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Signature so sánh được giữa các partition: tên + kiểu, không phụ thuộc thứ tự partition column."""
    physical = [(name, dtype) for name, dtype in fields if name not in PARTITION_COLUMNS]
    return sorted(physical)


def read_partitions(spark, clean_dir: Path, months: list[str]) -> dict[str, dict[str, Any]]:
    """Đọc schema và số dòng của từng partition; count dùng pushdown nên chỉ đọc footer Parquet."""
    result: dict[str, dict[str, Any]] = {}
    for month in months:
        part = partition_path(clean_dir, month)
        if not part.is_dir():
            continue
        frame = read_local_month_partition(spark, str(clean_dir / "trips"), month)
        fields = [(field.name, field.dataType.simpleString()) for field in frame.schema.fields]
        files = [item for item in part.rglob("*") if item.is_file() and item.suffix == ".parquet"]
        result[month] = {
            "columns": fields,
            "signature": schema_signature(fields),
            "rows": int(frame.count()),
            "data_bytes": sum(item.stat().st_size for item in files),
            "tree_bytes": sum(item.stat().st_size for item in part.rglob("*") if item.is_file()),
            "files": len(files),
        }
    return result


def load_run_log(path: Path) -> dict[str, dict[str, Any]]:
    """Thẻ ghi cuối cùng của mỗi tháng, kèm thẻ done/skipped gần nhất cho số liệu."""
    if not path.is_file():
        return {}
    latest: dict[str, dict[str, Any]] = {}
    metrics: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8-sig") as log_file:
        for row in csv.DictReader(log_file):
            month = row.get("month", "")
            if not MONTH_RE.fullmatch(month):
                continue
            latest[month] = row
            if row.get("status") in {"done", "skipped"}:
                metrics[month] = row
    return {
        month: {"latest": latest[month], "metrics": metrics.get(month)}
        for month in latest
    }


def removed_from_log(row: dict[str, str] | None) -> int | None:
    """Tổng số dòng bị loại theo thẻ ghi; thiếu dữ liệu thì trả None thay vì đoán 0."""
    if not row or not row.get("rule_removed"):
        return None
    try:
        rules = json.loads(row["rule_removed"])
    except json.JSONDecodeError:
        return None
    return sum(int(value) for value in rules.values())


def build_report(
    months: list[str],
    manifest: dict[str, dict[str, Any]],
    disk: dict[str, dict[str, Any]],
    run_log: dict[str, dict[str, Any]],
    baseline_month: str,
) -> dict[str, Any]:
    baseline = disk.get(baseline_month)
    if baseline is None:
        raise RuntimeError(f"Baseline partition {baseline_month} chưa tồn tại trên đĩa")

    rows_out: list[dict[str, Any]] = []
    missing_partitions: list[str] = []
    schema_mismatch: dict[str, list[tuple[str, str]]] = {}
    arithmetic_fail: list[str] = []
    total_rows = total_bytes = total_raw = total_removed = 0
    requested_raw_total = 0
    months_with_removed = 0
    for month in months:
        entry = disk.get(month)
        log = run_log.get(month, {})
        metrics = log.get("metrics")
        latest = log.get("latest", {})
        if entry is None:
            missing_partitions.append(month)
            requested_raw_total += int(manifest[month]["row_count"])
            # Trạng thái lấy từ run_log: "done" mà không có partition là dấu hiệu dữ liệu bị mất.
            rows_out.append({
                "month": month, "status": f"missing_partition({latest.get('status', 'no_log')})",
                "raw": manifest[month]["row_count"], "removed": "", "expected": "",
                "disk_rows": "", "drop_rate": "", "distance_missing": "", "unknown_zone": "",
                "short_trip": "", "bytes": 0,
            })
            continue
        if entry["signature"] != baseline["signature"]:
            schema_mismatch[month] = [
                item for item in entry["signature"] if item not in baseline["signature"]
            ] + [
                item for item in baseline["signature"] if item not in entry["signature"]
            ]
        raw_rows = int(manifest[month]["row_count"])
        requested_raw_total += raw_rows
        removed = removed_from_log(metrics)
        if removed is not None:
            months_with_removed += 1
        expected = raw_rows - removed if removed is not None else None
        status = latest.get("status", "")
        if expected is not None and expected != entry["rows"]:
            arithmetic_fail.append(month)
        total_rows += entry["rows"]
        total_bytes += entry["tree_bytes"]
        total_raw += raw_rows
        total_removed += removed or 0
        rows_out.append({
            "month": month,
            "status": status or "partition_only",
            "raw": raw_rows,
            "removed": removed if removed is not None else "",
            "expected": expected if expected is not None else "",
            "disk_rows": entry["rows"],
            "drop_rate": metrics.get("drop_rate", "") if metrics else "",
            "distance_missing": metrics.get("is_distance_missing", "") if metrics else "",
            "unknown_zone": metrics.get("is_unknown_zone", "") if metrics else "",
            "short_trip": metrics.get("is_short_trip", "") if metrics else "",
            "bytes": entry["tree_bytes"],
        })
    failed = sorted(m for m, v in run_log.items() if v["latest"].get("status") == "failed")
    flagged = sorted(m for m, v in run_log.items() if v["latest"].get("status") == "flagged")
    return {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "baseline_month": baseline_month,
        "baseline_columns": baseline["columns"],
        "months_checked": len([m for m in months if m in disk]),
        "months_requested": len(months),
        "missing_partitions": missing_partitions,
        "schema_mismatch": schema_mismatch,
        "arithmetic_fail": arithmetic_fail,
        "failed_months": failed,
        "flagged_months": flagged,
        "total_rows": total_rows,
        "total_raw_rows": total_raw,
        "requested_raw_total": requested_raw_total,
        "months_with_removed": months_with_removed,
        "total_removed": total_removed,
        "total_bytes": total_bytes,
        "months": rows_out,
    }


def render_markdown(report: dict[str, Any]) -> str:
    gib = 1024**3
    lines = [
        "# Tổng kết warehouse theo tháng",
        "",
        f"- Sinh lúc: {report['generated_at']}",
        f"- Phạm vi kiểm tra: {report['months_requested']} tháng, có partition trên đĩa: {report['months_checked']}",
        f"- Chuẩn schema: `{report['baseline_month']}` ({len(report['baseline_columns'])} cột đọc từ partition)",
        f"- Tổng dòng trên đĩa: {report['total_rows']:,}",
        f"- Raw theo manifest của {report['months_checked']} partition có mặt: {report['total_raw_rows']:,}; "
        f"removed ghi trong run_log: {report['total_removed']:,} (chỉ {report['months_with_removed']} tháng có thẻ metrics)",
        f"- Raw theo manifest cho toàn {report['months_requested']} tháng yêu cầu: {report['requested_raw_total']:,}",
        f"- Dung lượng output: {report['total_bytes']:,} B ({report['total_bytes'] / gib:.2f} GiB)",
        "",
        "## Tính nhất quán schema",
        "",
    ]
    if report["schema_mismatch"]:
        lines.append(f"{len(report['schema_mismatch'])} partition lệch schema so với chuẩn:")
        for month, diff in sorted(report["schema_mismatch"].items()):
            lines.append(f"- `{month}`: lệch {diff}")
    else:
        lines.append("Tất cả partition có mặt trên đĩa đều trùng schema với chuẩn.")
    lines += ["", "## Arithmetic raw - removed = dòng trên đĩa", ""]
    if report["arithmetic_fail"]:
        lines.append(f"{len(report['arithmetic_fail'])} tháng không khớp: {', '.join(report['arithmetic_fail'])}")
    else:
        lines.append("Mọi tháng có đủ dữ liệu run_log đều khớp; thiếu số liệu thì để trống, không đoán.")
    lines += ["", "## Tháng lỗi và tháng bị đánh dấu", ""]
    lines.append(f"- `failed` ({len(report['failed_months'])}): {', '.join(report['failed_months']) or 'không có'}")
    lines.append(f"- `flagged` ({len(report['flagged_months'])}): {', '.join(report['flagged_months']) or 'không có'}")
    lines += [
        "",
        f"- Thiếu partition so với manifest ({len(report['missing_partitions'])}): "
        f"{', '.join(report['missing_partitions']) or 'không có'}",
        "",
        "## Bảng theo tháng",
        "",
        "| Tháng | Trạng thái | Raw | Removed | Raw-Removed | Dòng trên đĩa | Drop rate | is_distance_missing | is_unknown_zone | is_short_trip | Output bytes |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in report["months"]:
        lines.append(
            f"| {row['month']} | {row['status']} | {row['raw']} | {row['removed']} | "
            f"{row['expected']} | {row['disk_rows']} | {row['drop_rate']} | "
            f"{row['distance_missing']} | {row['unknown_zone']} | {row['short_trip']} | {row['bytes']} |"
        )
    return "\n".join(lines) + "\n"


COLUMN_MEANINGS = {
    "pickup_datetime": "Thời điểm đón theo giờ địa phương NYC.",
    "pickup_date": "Ngày pickup, suy ra từ pickup_datetime.",
    "pickup_hour_ts": "pickup_datetime làm tròn xuống đầu giờ.",
    "dropoff_datetime": "Thời điểm trả khách theo giờ địa phương NYC.",
    "year": "Năm pickup; cột phân vùng.",
    "month": "Tháng pickup; cột phân vùng.",
    "day": "Ngày trong tháng theo pickup.",
    "hour": "Giờ pickup trong ngày, 0-23.",
    "day_of_week": "Thứ theo Spark: 1=Chủ nhật, 7=Thứ bảy.",
    "pickup_location_id": "Mã TLC taxi zone tại điểm đón.",
    "pickup_borough": "Borough tại điểm đón từ Taxi Zone Lookup.",
    "pickup_zone": "Tên zone tại điểm đón.",
    "dropoff_location_id": "Mã TLC taxi zone tại điểm trả.",
    "dropoff_borough": "Borough tại điểm trả từ Taxi Zone Lookup.",
    "dropoff_zone": "Tên zone tại điểm trả.",
    "trip_distance": "Quãng đường dặm; giá trị 0 được chuyển thành null.",
    "is_distance_missing": "True khi trip_distance gốc bằng 0 trước chuẩn hóa.",
    "trip_duration_min": "Thời lượng chuyến theo phút.",
    "is_short_trip": "True khi thời lượng dưới short_trip_max_minutes; dòng vẫn giữ.",
    "is_unknown_zone": "True khi một LocationID không có trong lookup, hoặc lookup ghi Borough 'Unknown' / Zone 'N/A'.",
    "passenger_count": "Số hành khách khai báo; 0 đổi thành null; chỉ mô tả, không làm feature.",
    "fare_amount": "Cước USD.",
    "total_amount": "Tổng tiền USD.",
    "tip_amount": "Tip USD.",
    "tolls_amount": "Phí cầu đường USD.",
}


def render_schema_doc(report: dict[str, Any], config: dict[str, Any]) -> str:
    """docs/schema.md sinh từ schema thật đọc trên đĩa, không chép tay."""
    cleaning = config["cleaning"]
    overrides = cleaning.get("max_drop_rate_by_year") or {}
    threshold_note = ", ".join(
        f"nạp theo năm {year} = {float(value):.0%}" for year, value in sorted(overrides.items())
    )
    physical = [item for item in report["baseline_columns"] if item[0] not in PARTITION_COLUMNS]
    partition_columns = [item for item in report["baseline_columns"] if item[0] in PARTITION_COLUMNS]
    lines = [
        "# Schema dữ liệu Yellow Taxi đã làm sạch",
        "",
        "Sinh tự động từ schema thật của partition trên đĩa, không viết tay.",
        "",
        f"- Sinh lúc: {report['generated_at']}",
        f"- Nguồn: NYC TLC Yellow Taxi, phạm vi manifest {report['months_requested']} tháng",
        f"- Output: `{Path(config['paths']['clean_dir']) / 'trips'}`, Parquet Snappy, phân vùng `year=/month=`",
        f"- Partition đối chiếu: {report['months_checked']}; partition lệch schema: {len(report['schema_mismatch'])}",
        f"- Tổng dòng trên đĩa: {report['total_rows']:,}",
        f"- Ngưỡng làm sạch: duration trong ({cleaning['trip_duration_min_minutes']}, {cleaning['trip_duration_max_minutes']}] phút, "
        f"trip_distance trong [{cleaning['trip_distance_min_miles']}, {cleaning['trip_distance_max_miles']}] dặm, "
        f"average_speed <= {cleaning['average_speed_max_mph']} mph cho chuyến >= {cleaning['short_trip_max_minutes']} phút, "
        f"max_drop_rate = {cleaning['max_drop_rate']:.0%}"
        + (f", {threshold_note}" if threshold_note else ""),
        "",
        f"## Cột vật lý trong file ({len(physical)})",
        "",
        "| Tên cột | Kiểu Spark | Đơn vị / miền giá trị | Ý nghĩa |",
        "|---|---|---|---|",
    ]
    units = {
        "trip_distance": "dặm", "trip_duration_min": "phút", "fare_amount": "USD",
        "total_amount": "USD", "tip_amount": "USD", "tolls_amount": "USD",
        "day": "1-31", "hour": "0-23", "day_of_week": "1=CN .. 7=T7",
        "pickup_location_id": "LocationID TLC", "dropoff_location_id": "LocationID TLC",
    }
    for name, dtype in physical:
        lines.append(f"| `{name}` | `{dtype}` | {units.get(name, '—')} | {COLUMN_MEANINGS.get(name, '')} |")
    lines += [
        "",
        f"## Cột phân vùng ({len(partition_columns)})",
        "",
        "`year` và `month` là cột phân vùng Hive-style; Spark nối chúng khi đọc nên DataFrame đầu ra có "
        f"{len(report['baseline_columns'])} cột.",
        "",
        "| Tên cột | Kiểu Spark | Ý nghĩa |",
        "|---|---|---|",
    ]
    for name, dtype in partition_columns:
        lines.append(f"| `{name}` | `{dtype}` | {COLUMN_MEANINGS.get(name, '')} |")
    lines += [
        "",
        "## Cờ chất lượng",
        "",
        "- `is_distance_missing`: trip_distance gốc bằng 0 đã được set null, dòng vẫn giữ.",
        "- `is_short_trip`: chuyến dưới ngưỡng vẫn giữ, chỉ gắn cờ để lọc khi làm feature.",
        "- `is_unknown_zone`: id không có trong lookup HOẶC lookup gán Borough 'Unknown' / Zone 'N/A'.",
        "",
        "## Feature cho mô hình",
        "",
        "- `passenger_count` chỉ dùng mô tả, tuyệt đối không đưa vào features dự báo.",
    ]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Tổng kết warehouse và sinh docs/schema.md")
    parser.add_argument("--from", dest="start", default="2011-01")
    parser.add_argument("--to", dest="end", default="2026-08")
    parser.add_argument("--config", type=Path, default=CONFIG_PATH)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--run-log", type=Path)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--schema-doc", type=Path, help="Để trống nếu chưa muốn ghi docs/schema.md")
    args = parser.parse_args(argv)

    config = load_config(args.config)
    reports_dir = Path(config["paths"]["reports_dir"])
    manifest_path = args.manifest or reports_dir / "source_manifest.csv"
    run_log_path = args.run_log or reports_dir / "run_log.csv"
    summary_path = args.summary or reports_dir / "warehouse_summary.md"
    months = month_range(args.start, args.end)
    manifest = load_manifest(manifest_path)
    missing = [month for month in months if month not in manifest]
    if missing:
        raise KeyError(f"Tháng không có trong manifest: {missing}")

    spark = create_spark(config)
    try:
        disk = read_partitions(spark, Path(config["paths"]["clean_dir"]), months)
    finally:
        spark.stop()

    baseline_month = str(config.get("validation", {}).get("schema_baseline_month", months[-1]))
    report = build_report(months, manifest, disk, load_run_log(run_log_path), baseline_month)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(render_markdown(report), encoding="utf-8")
    print(f"warehouse_summary: {summary_path}")
    print(
        f"partition_kiem_tra={report['months_checked']}/{report['months_requested']} "
        f"lech_schema={len(report['schema_mismatch'])} thieu_partition={len(report['missing_partitions'])} "
        f"arithmetic_fail={len(report['arithmetic_fail'])}"
    )
    print(f"dong_tren_dia={report['total_rows']:,} output_bytes={report['total_bytes']:,}")
    print(f"failed={report['failed_months']} flagged={report['flagged_months']}")
    if args.schema_doc:
        args.schema_doc.parent.mkdir(parents=True, exist_ok=True)
        args.schema_doc.write_text(render_schema_doc(report, config), encoding="utf-8")
        print(f"schema_doc: {args.schema_doc}")
    return 1 if report["schema_mismatch"] or report["arithmetic_fail"] else 0


if __name__ == "__main__":
    sys.exit(main())