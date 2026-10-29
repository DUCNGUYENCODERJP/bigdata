from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.batch_runner import (
    RUN_LOG_FIELDS,
    DropRateExceeded,
    batch_months,
    dry_run_plan,
    partition_path,
    run_months,
)


class FakeUsage:
    def __init__(self, free: int = 20 * 1024**3):
        self.total = 30 * 1024**3
        self.used = self.total - free
        self.free = free


def fixture_paths(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    clean_dir = tmp_path / "clean"
    spark_dir = tmp_path / "spark"
    reports_dir = tmp_path / "reports"
    for path in (raw_dir, clean_dir, spark_dir, reports_dir):
        path.mkdir(parents=True, exist_ok=True)
    return {
        "paths": {
            "raw_dir": str(raw_dir),
            "clean_dir": str(clean_dir),
            "spark_local_dir": str(spark_dir),
            "reports_dir": str(reports_dir),
        },
        "runtime": {"min_free_disk_gb": 8},
        "cleaning": {"max_drop_rate": 0.08},
        "source": {"trip_url_template": "https://example.test/{month}"},
        # Số liệu ước lượng cố định để dry-run so sánh được chính xác.
        "validation": {"schema_baseline_month": "2024-01"},
        "batch": {
            "fixed_seconds_per_month": 20.0,
            "seconds_per_million_raw_rows": 10.0,
            "output_bytes_per_row": 2.0,
            "download_bytes_per_second": 1000.0,
        },
    }


def make_manifest(config, months=("2024-01", "2024-02")):
    manifest = {}
    for month in months:
        raw = Path(config["paths"]["raw_dir"]) / f"yellow_tripdata_{month}.parquet"
        raw.write_bytes(b"raw")
        manifest[month] = {
            "month": month,
            "url": f"https://example.test/{month}",
            "size_bytes": "3",
            "row_count": "100",
        }
    return manifest


def fake_metrics(**overrides):
    return {
        "clean": 96,
        "invalid": 0,
        "missing": 0,
        "is_distance_missing": 2,
        "is_unknown_zone": 3,
        "is_short_trip": 5,
        "output_bytes": 1234,
        **overrides,
    }


def fake_stats(month: str, **overrides):
    return {
        "month": month,
        "raw_rows": 100,
        "clean_rows": 96,
        "output_rows": 96,
        "drop_rate": 0.04,
        "elapsed_seconds": 1.25,
        # pipeline trả luôn count cờ đo từ partition đọc lại, batch không mở Spark lần hai.
        "invalid": 0,
        "missing": 0,
        "is_distance_missing": 2,
        "is_unknown_zone": 3,
        "is_short_trip": 5,
        "rules": [
            {"rule": "duplicate", "removed": 1, "distance_set_null": 0},
            {"rule": "trip_distance", "removed": 2, "distance_set_null": 4},
        ],
        **overrides,
    }


def read_log(path: Path):
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_batch_months_defaults_to_newest_first(tmp_path):
    assert batch_months("2024-11", "2025-02") == ["2025-02", "2025-01", "2024-12", "2024-11"]
    assert batch_months("2024-11", "2025-02", oldest_first=True) == ["2024-11", "2024-12", "2025-01", "2025-02"]


def test_completed_partition_is_postchecked_and_skipped(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    part = partition_path(Path(config["paths"]["clean_dir"]), "2024-01")
    part.mkdir(parents=True)
    calls = {"pipeline": 0, "validate": 0}

    def pipeline(*args):
        calls["pipeline"] += 1
        raise AssertionError("pipeline must not rewrite a valid completed month")

    def validate(month, cfg):
        calls["validate"] += 1
        return fake_metrics()

    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    run_months(
        ["2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=validate,
        disk_usage_fn=lambda _: FakeUsage(),
        now=lambda: datetime(2026, 10, 24, tzinfo=timezone.utc),
    )
    rows = read_log(log)
    assert calls == {"pipeline": 0, "validate": 1}
    assert rows[0]["status"] == "skipped"
    assert rows[0]["clean"] == "96"
    assert rows[0]["is_distance_missing"] == "2"
    assert rows[0]["is_unknown_zone"] == "3"
    assert rows[0]["is_short_trip"] == "5"
    assert set(rows[0]) == set(RUN_LOG_FIELDS)


def test_failed_month_is_logged_and_the_batch_continues(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config)
    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    calls = []

    def pipeline(months, cfg):
        month = months[0]
        calls.append(month)
        if month == "2024-02":
            raise RuntimeError("synthetic failure")
        partition_path(Path(cfg["paths"]["clean_dir"]), month).mkdir(parents=True, exist_ok=True)
        return [fake_stats(month)]

    records = run_months(
        ["2024-02", "2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    # Tháng lỗi đứng đầu vẫn không được chặn các tháng phía sau.
    assert calls == ["2024-02", "2024-01"]
    rows = read_log(log)
    assert [(row["month"], row["status"], row["error"]) for row in rows] == [
        ("2024-02", "failed", "synthetic failure"),
        ("2024-01", "done", ""),
    ]
    assert [record["status"] for record in records] == ["failed", "done"]


def test_stale_partition_that_fails_postcheck_is_rewritten(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    part = partition_path(Path(config["paths"]["clean_dir"]), "2024-01")
    part.mkdir(parents=True)
    calls = []

    def validate(month, cfg):
        # Partition cũ 24 cột: postcheck báo thiếu cột, batch phải ghi lại chứ không dừng.
        raise RuntimeError("schema_matches=False")

    def pipeline(months, cfg):
        calls.extend(months)
        return [fake_stats(months[0])]

    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    run_months(
        ["2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=validate,
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert calls == ["2024-01"]
    rows = read_log(log)
    assert [(row["month"], row["status"]) for row in rows] == [("2024-01", "done")]


def test_written_month_is_not_revalidated_with_a_second_spark_session(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    validate_calls = []

    def pipeline(months, cfg):
        partition_path(Path(cfg["paths"]["clean_dir"]), months[0]).mkdir(parents=True, exist_ok=True)
        return [fake_stats(months[0])]

    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    records = run_months(
        ["2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline,
        validate_fn=lambda month, cfg: validate_calls.append(month) or fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    # verify_clean_partition trong pipeline đã đọc partition từ đĩa; batch không cần phiên Spark thứ hai.
    assert validate_calls == []
    assert records[0]["status"] == "done"
    assert records[0]["is_short_trip"] == 5
    assert records[0]["is_unknown_zone"] == 3
    assert records[0]["is_distance_missing"] == 2


def test_drop_rate_over_limit_is_flagged_and_batch_continues(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config)
    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    calls = []

    def pipeline(months, cfg):
        month = months[0]
        calls.append(month)
        if month == "2024-02":
            # Pipeline thật dừng trước khi ghi output và báo bằng exception riêng.
            raise DropRateExceeded(month, 0.12, 0.08)
        partition_path(Path(cfg["paths"]["clean_dir"]), month).mkdir(parents=True, exist_ok=True)
        return [fake_stats(month)]

    run_months(
        ["2024-02", "2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert calls == ["2024-02", "2024-01"]
    rows = read_log(log)
    assert [(row["month"], row["status"]) for row in rows] == [
        ("2024-02", "flagged"),
        ("2024-01", "done"),
    ]
    assert "12.00%" in rows[0]["error"] and "8.00%" in rows[0]["error"]


def test_stats_over_limit_from_pipeline_fn_are_flagged(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"

    def pipeline(months, cfg):
        partition_path(Path(cfg["paths"]["clean_dir"]), months[0]).mkdir(parents=True, exist_ok=True)
        return [fake_stats(months[0], drop_rate=0.09)]

    records = run_months(
        ["2024-01"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert records[0]["status"] == "flagged"
    assert read_log(log)[0]["status"] == "flagged"


def test_year_override_lets_2025_pass_at_the_same_drop_rate(tmp_path):
    """Cùng 9%: 2025-05 có ngưỡng riêng nên vẫn ghi, 2024-05 vẫn bị flag."""
    config = fixture_paths(tmp_path)
    config["cleaning"]["max_drop_rate_by_year"] = {"2025": 0.12}
    manifest = make_manifest(config, ("2025-05", "2024-05"))
    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"

    def pipeline(months, cfg):
        partition_path(Path(cfg["paths"]["clean_dir"]), months[0]).mkdir(parents=True, exist_ok=True)
        return [fake_stats(months[0], drop_rate=0.09)]

    records = run_months(
        ["2025-05", "2024-05"], config, manifest, log_path=log,
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert [(row["month"], row["status"]) for row in records] == [
        ("2025-05", "done"),
        ("2024-05", "flagged"),
    ]
    assert "8.00%" in records[1]["error"]
    assert records[0]["error"] == ""


def test_resume_skips_successful_month_and_retries_failed_month(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config)
    log = Path(config["paths"]["reports_dir"]) / "run_log.csv"
    calls = []
    fail_second = {"value": True}

    def validate(month, cfg):
        assert partition_path(Path(cfg["paths"]["clean_dir"]), month).is_dir()
        return fake_metrics()

    def pipeline(months, cfg):
        month = months[0]
        calls.append(month)
        if month == "2024-02" and fail_second["value"]:
            raise RuntimeError("stop at February")
        partition_path(Path(cfg["paths"]["clean_dir"]), month).mkdir(parents=True, exist_ok=True)
        return [fake_stats(month)]

    kwargs = dict(
        log_path=log, pipeline_fn=pipeline, validate_fn=validate,
        disk_usage_fn=lambda _: FakeUsage(),
    )
    run_months(["2024-01", "2024-02"], config, manifest, **kwargs)
    fail_second["value"] = False
    run_months(["2024-01", "2024-02"], config, manifest, **kwargs)
    assert calls == ["2024-01", "2024-02", "2024-02"]
    rows = read_log(log)
    assert [(r["month"], r["status"]) for r in rows] == [
        ("2024-01", "done"),
        ("2024-02", "failed"),
        ("2024-01", "skipped"),
        ("2024-02", "done"),
    ]
    assert rows[-1]["rule_removed"] == '{"duplicate":1,"trip_distance":2}'
    assert rows[-1]["distance_set_null"] == "4"
    assert rows[-1]["is_short_trip"] == "5"
    assert rows[-1]["seconds"] == "1.25"
    assert rows[-1]["raw_bytes"] == "3"


def test_force_reruns_completed_partition(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    part = partition_path(Path(config["paths"]["clean_dir"]), "2024-01")
    part.mkdir(parents=True)
    calls = []

    def pipeline(months, cfg):
        calls.extend(months)
        return [fake_stats(months[0])]

    run_months(
        ["2024-01"], config, manifest, force=True,
        log_path=Path(config["paths"]["reports_dir"]) / "run_log.csv",
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(),
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert calls == ["2024-01"]


def test_raw_size_mismatch_is_deleted_and_downloaded_once_more(tmp_path):
    config = fixture_paths(tmp_path)
    raw_path = Path(config["paths"]["raw_dir"]) / "yellow_tripdata_2024-01.parquet"
    manifest = {
        "2024-01": {
            "month": "2024-01", "url": "https://example.test/2024-01",
            "size_bytes": "4", "row_count": "10",
        }
    }
    calls = []

    def download(url, destination, expected):
        calls.append(url)
        destination.write_bytes(b"bad" if len(calls) == 1 else b"good")
        return destination.stat().st_size

    def pipeline(months, cfg):
        partition_path(Path(cfg["paths"]["clean_dir"]), months[0]).mkdir(parents=True)
        return [fake_stats(months[0])]

    run_months(
        ["2024-01"], config, manifest,
        log_path=Path(config["paths"]["reports_dir"]) / "run_log.csv",
        pipeline_fn=pipeline, validate_fn=lambda *_: fake_metrics(), download_fn=download,
        disk_usage_fn=lambda _: FakeUsage(),
    )
    assert len(calls) == 2
    assert raw_path.read_bytes() == b"good"


def test_dry_run_estimates_only_months_that_will_run(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config)
    completed = partition_path(Path(config["paths"]["clean_dir"]), "2024-01")
    completed.mkdir(parents=True)
    plan = dry_run_plan(["2024-01", "2024-02"], config, manifest)
    # Không force: partition đã có chỉ được đánh dấu "check" vì chưa mở Spark xác minh schema.
    assert [row["status"] for row in plan["months"]] == ["check", "run"]
    assert plan["counts_by_status"] == {"run": 1, "rerun": 0, "check": 1}
    assert plan["run_count"] == 1 and plan["skip_count"] == 1
    assert plan["raw_bytes_to_download"] == 0  # February raw file already matches manifest.
    assert plan["estimated_output_bytes"] == int(100 * 2.0)
    assert plan["estimated_seconds"] == 20.0 + 10.0 * 100 / 1_000_000


def test_dry_run_with_force_counts_every_month_as_rerun(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config)
    completed = partition_path(Path(config["paths"]["clean_dir"]), "2024-01")
    completed.mkdir(parents=True)
    plan = dry_run_plan(["2024-01", "2024-02"], config, manifest, force=True)
    # Tháng đã có partition -> "rerun"; tháng chưa có -> vẫn là "run".
    assert [row["status"] for row in plan["months"]] == ["rerun", "run"]
    assert plan["counts_by_status"] == {"run": 1, "rerun": 1, "check": 0}
    assert plan["run_count"] == 2 and plan["skip_count"] == 0
    assert plan["estimated_output_bytes"] == 2 * int(100 * 2.0)
    assert plan["estimated_seconds"] == 2 * (20.0 + 10.0 * 100 / 1_000_000)


def test_dry_run_counts_missing_raw_bytes_for_download_estimate(tmp_path):
    config = fixture_paths(tmp_path)
    manifest = make_manifest(config, ("2024-01",))
    Path(config["paths"]["raw_dir"], "yellow_tripdata_2024-01.parquet").unlink()
    plan = dry_run_plan(["2024-01"], config, manifest)
    assert plan["raw_bytes_to_download"] == 3
    assert plan["estimated_download_seconds"] == 3 / 1000.0
