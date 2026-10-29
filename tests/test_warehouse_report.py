from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import warehouse_report as wr


def full_columns(with_short_trip: bool = True):
    names = [
        "pickup_datetime", "pickup_date", "pickup_hour_ts", "dropoff_datetime",
        "year", "month", "day", "hour", "day_of_week", "pickup_location_id",
        "is_unknown_zone", "pickup_borough", "pickup_zone", "dropoff_location_id",
        "dropoff_borough", "dropoff_zone", "trip_distance", "is_distance_missing",
        "trip_duration_min", "passenger_count", "fare_amount", "total_amount",
        "tip_amount", "tolls_amount",
    ]
    if with_short_trip:
        names.insert(names.index("trip_duration_min") + 1, "is_short_trip")
    return [(name, "string") for name in names]


def disk_entry(rows: int, with_short_trip: bool = True):
    columns = full_columns(with_short_trip)
    return {
        "columns": columns,
        "signature": wr.schema_signature(columns),
        "rows": rows,
        "data_bytes": rows * 20,
        "tree_bytes": rows * 21,
        "files": 1,
    }


def manifest(months, raw: dict[str, int]):
    return {
        month: {"month": month, "row_count": str(raw.get(month, 500)), "size_bytes": "1"}
        for month in months
    }


def done_record(month: str, removed: int, **metrics):
    record = {"status": "done", "drop_rate": "0.02", "rule_removed": f'{{"duplicate":{removed}}}'}
    record.update(metrics)
    return {"latest": {"month": month, **record}, "metrics": record}


def test_report_flags_stale_partition_and_keeps_arithmetic_separate():
    months = ["2024-01", "2023-02"]
    disk = {"2024-01": disk_entry(400), "2023-02": disk_entry(300, with_short_trip=False)}
    run_log = {
        "2024-01": done_record(
            "2024-01", 100, is_short_trip="7", is_distance_missing="3", is_unknown_zone="9"
        ),
        "2023-02": {"latest": {"month": "2023-02", "status": "failed"}, "metrics": None},
    }
    report = wr.build_report(months, manifest(months, {"2024-01": 500}), disk, run_log, "2024-01")

    assert report["months_checked"] == 2
    assert list(report["schema_mismatch"]) == ["2023-02"]
    assert ("is_short_trip", "string") in report["schema_mismatch"]["2023-02"]
    # raw 500 - removed 100 = 400 dòng đọc được từ footer partition chuẩn.
    assert report["arithmetic_fail"] == []
    assert report["total_rows"] == 700
    assert report["failed_months"] == ["2023-02"]
    january = report["months"][0]
    assert (january["removed"], january["expected"], january["disk_rows"]) == (100, 400, 400)
    assert january["short_trip"] == "7"
    # Tháng failed không có metrics thì để trống, không được đoán 0.
    february = report["months"][1]
    assert february["removed"] == "" and february["expected"] == "" and february["drop_rate"] == ""


def test_report_separates_missing_partition_and_arithmetic_failure():
    months = ["2024-01", "2022-06", "2021-05"]
    disk = {"2024-01": disk_entry(400), "2021-05": disk_entry(40)}
    run_log = {month: done_record(month, 90) for month in months}
    raw = {"2024-01": 490, "2022-06": 500, "2021-05": 500}
    report = wr.build_report(months, manifest(months, raw), disk, run_log, "2024-01")

    assert report["missing_partitions"] == ["2022-06"]
    assert report["months"][1]["status"] == "missing_partition(done)"
    # 2021-05: raw 500 - removed 90 = 410 nhưng đĩa chỉ có 40 dòng.
    assert report["arithmetic_fail"] == ["2021-05"]
    # raw toàn phạm vi = 490 + 500 + 500; chỉ 2 tháng có partition mới tính vào removed.
    assert report["requested_raw_total"] == 1490
    assert report["total_raw_rows"] == 990
    assert report["months_with_removed"] == 2
    markdown = wr.render_markdown(report)
    assert "không khớp: 2021-05" in markdown
    assert "| 2022-06 | missing_partition(done) |" in markdown
    assert "Tất cả partition có mặt trên đĩa đều trùng schema" in markdown


def test_schema_doc_splits_physical_and_partition_columns():
    report = wr.build_report(
        ["2024-01"], manifest(["2024-01"], {"2024-01": 500}),
        {"2024-01": disk_entry(400)}, {}, "2024-01",
    )
    config = {
        "cleaning": {
            "trip_duration_min_minutes": 0, "trip_duration_max_minutes": 180,
            "trip_distance_min_miles": 0, "trip_distance_max_miles": 100,
            "average_speed_max_mph": 100, "short_trip_max_minutes": 1, "max_drop_rate": 0.08,
        },
        "paths": {"clean_dir": "D:/BIGDATA/2-processing/data/clean"},
    }
    doc = wr.render_schema_doc(report, config)

    assert "## Cột vật lý trong file (23)" in doc
    assert "## Cột phân vùng (2)" in doc
    assert "không có trong lookup, hoặc lookup ghi Borough 'Unknown' / Zone 'N/A'" in doc
    assert "max_drop_rate = 8%" in doc
    assert "phạm vi manifest 1 tháng" in doc
