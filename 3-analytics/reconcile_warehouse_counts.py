"""Compare raw Hive Parquet row counts with the TLC source manifest.

Counts are read from Parquet footers through WebHDFS range reads, so the job
does not scan trip rows or depend on the DataNode's internal Docker address.
"""

from __future__ import annotations
import logging

import argparse
import concurrent.futures
import csv
import datetime as dt
import io
import json
import re
import sys
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")


PARTITION_PATTERN = re.compile(r"year=(\d{4})/month=(\d{1,2})")
MONTH_PATTERN = re.compile(r"\d{4}-\d{2}")


class _WarehouseRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Route the single Docker DataNode through its published warehouse host."""

    def __init__(self, warehouse_host: str):
        super().__init__()
        self.warehouse_host = warehouse_host

    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        parsed = urlsplit(new_url)
        if parsed.hostname and parsed.hostname != self.warehouse_host:
            netloc = f"{self.warehouse_host}:{parsed.port}" if parsed.port else self.warehouse_host
            new_url = urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))
        return super().redirect_request(
            request, file_pointer, code, message, headers, new_url
        )


class _WebHdfsRangeFile(io.RawIOBase):
    """Seekable file-like adapter that fetches only requested WebHDFS ranges."""

    def __init__(self, warehouse_host: str, path: str, size: int):
        self.warehouse_host = warehouse_host
        self.path = path
        self.size = size
        self.position = 0
        self.opener = urllib.request.build_opener(
            _WarehouseRedirectHandler(warehouse_host)
        )

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self.position + offset
        elif whence == io.SEEK_END:
            position = self.size + offset
        else:
            raise ValueError(f"Invalid seek mode: {whence}")
        if position < 0:
            raise ValueError("Cannot seek before the start of a Parquet file.")
        self.position = position
        return position

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = self.size - self.position
        size = min(size, self.size - self.position)
        if size <= 0:
            return b""

        query = urllib.parse.urlencode(
            {
                "op": "OPEN",
                "user.name": "hadoop",
                "offset": self.position,
                "length": size,
            }
        )
        url = (
            f"http://{self.warehouse_host}:9870/webhdfs/v1"
            f"{self.path}?{query}"
        )
        request = urllib.request.Request(url, headers={"Accept": "application/octet-stream"})
        with self.opener.open(request, timeout=20) as response:
            data = response.read()
        self.position += len(data)
        if len(data) != size:
            raise IOError(
                f"WebHDFS returned {len(data)} of {size} requested bytes for {self.path}."
            )
        return data

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)


def _request_json(warehouse_host: str, path: str, operation: str) -> dict:
    query = urllib.parse.urlencode({"op": operation, "user.name": "hadoop"})
    url = f"http://{warehouse_host}:9870/webhdfs/v1{path}?{query}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def _list_parquet_files(warehouse_host: str, root: str) -> list[dict]:
    frontier = [root.rstrip("/")]
    parquet_files = []
    while frontier:
        next_frontier = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = executor.map(
                lambda directory: (directory, _request_json(warehouse_host, directory, "LISTSTATUS")),
                frontier,
            )
            for directory, payload in results:
                statuses = payload.get("FileStatuses", {}).get("FileStatus", [])
                for status in statuses:
                    child = f"{directory}/{status['pathSuffix']}"
                    if status.get("type") == "DIRECTORY":
                        next_frontier.append(child)
                    elif child.lower().endswith(".parquet"):
                        match = PARTITION_PATTERN.search(child)
                        if not match:
                            raise ValueError(f"Parquet path has no year/month partition: {child}")
                        year, month = map(int, match.groups())
                        parquet_files.append(
                            {
                                "path": child,
                                "size": int(status["length"]),
                                "month": f"{year:04d}-{month:02d}",
                            }
                        )
        frontier = next_frontier
    return sorted(parquet_files, key=lambda item: (item["month"], item["path"]))


def _parquet_row_count(warehouse_host: str, parquet_file: dict) -> int:
    remote_file = _WebHdfsRangeFile(
        warehouse_host,
        parquet_file["path"],
        parquet_file["size"],
    )
    try:
        with pa.PythonFile(remote_file, mode="r") as source:
            metadata = pq.read_metadata(source)
        return int(metadata.num_rows)
    finally:
        remote_file.close()


def _load_manifest(path: Path) -> dict[str, int]:
    with path.open(encoding="utf-8-sig", newline="") as manifest_file:
        rows = csv.DictReader(manifest_file)
        counts = {}
        for row in rows:
            month = row["month"]
            if not MONTH_PATTERN.fullmatch(month) or month < "2011-01":
                continue
            if month in counts:
                raise ValueError(f"Duplicate month {month} in source manifest: {path}")
            counts[month] = int(row["row_count"])
    return counts


def _cached_raw_count(path: Path) -> int | None:
    if not path.exists():
        return None
    cached = json.loads(path.read_text(encoding="utf-8"))
    value = cached.get("data", {}).get("cancelled", {}).get("total_raw")
    return int(value) if value is not None else None


def reconcile(
    warehouse_host: str,
    raw_path: str,
    manifest_path: Path,
    cache_path: Path,
    output_dir: Path,
) -> dict:
    manifest_counts = _load_manifest(manifest_path)
    parquet_files = _list_parquet_files(warehouse_host, raw_path)
    counts_by_month: dict[str, int] = defaultdict(int)
    files_by_month: dict[str, list[str]] = defaultdict(list)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_parquet_row_count, warehouse_host, item): item
            for item in parquet_files
        }
        for completed, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            item = futures[future]
            counts_by_month[item["month"]] += future.result()
            files_by_month[item["month"]].append(Path(item["path"]).name)
            if completed % 16 == 0 or completed == len(futures):
                logging.info(f"[AUDIT] Đọc footer {completed}/{len(futures)} file raw.")

    hive_months = sorted(counts_by_month)
    compared_months = sorted(set(hive_months) & set(manifest_counts))
    missing_manifest_months = sorted(set(hive_months) - set(manifest_counts))
    missing_hive_months = sorted(set(manifest_counts) - set(hive_months))
    rows = []
    for month in compared_months:
        hive_rows = counts_by_month[month]
        manifest_rows = manifest_counts[month]
        rows.append(
            {
                "month": month,
                "manifest_rows": manifest_rows,
                "hive_raw_rows": hive_rows,
                "delta_hive_minus_manifest": hive_rows - manifest_rows,
                "raw_parquet_files": len(files_by_month[month]),
                "raw_files": ";".join(sorted(files_by_month[month])),
            }
        )

    manifest_total = sum(manifest_counts[month] for month in compared_months)
    hive_total = sum(counts_by_month[month] for month in compared_months)
    previous_cache_count = _cached_raw_count(cache_path)
    result = {
        "created_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        "warehouse_host": warehouse_host,
        "raw_hdfs_path": raw_path,
        "manifest_path": str(manifest_path),
        "count_method": "Parquet footer row-group metadata read through WebHDFS OPEN byte ranges; no trip rows scanned.",
        "compared_months": len(compared_months),
        "first_month": compared_months[0] if compared_months else None,
        "last_month": compared_months[-1] if compared_months else None,
        "manifest_total_rows": manifest_total,
        "hive_raw_total_rows": hive_total,
        "delta_hive_minus_manifest": hive_total - manifest_total,
        "previous_dashboard_cache_raw_rows": previous_cache_count,
        "delta_hive_minus_previous_cache": (
            hive_total - previous_cache_count if previous_cache_count is not None else None
        ),
        "missing_manifest_months": missing_manifest_months,
        "missing_hive_months": missing_hive_months,
        "unmatched_hive_month_rows": sum(counts_by_month[m] for m in missing_manifest_months),
        "monthly": rows,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    date_stamp = dt.datetime.now().strftime("%Y%m%d")
    json_path = output_dir / f"warehouse_reconciliation_{date_stamp}.json"
    csv_path = output_dir / f"warehouse_reconciliation_{date_stamp}.csv"
    markdown_path = output_dir / f"warehouse_reconciliation_{date_stamp}.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(rows[0]) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    markdown = [
        "# Đối soát raw Hive và manifest TLC",
        "",
        f"- Thời điểm: {result['created_at']}",
        f"- Khoảng tháng: {result['first_month']} đến {result['last_month']} ({result['compared_months']} tháng)",
        f"- Manifest: {manifest_total:,} chuyến",
        f"- Raw Hive: {hive_total:,} chuyến",
        f"- Chênh lệch Hive − manifest: {hive_total - manifest_total:+,} chuyến",
        f"- Snapshot trước đó: {previous_cache_count:,} chuyến raw"
        if previous_cache_count is not None
        else "- Snapshot trước đó: không có tổng raw",
        f"- Chênh lệch Hive − snapshot: {result['delta_hive_minus_previous_cache']:+,} chuyến"
        if result["delta_hive_minus_previous_cache"] is not None
        else "- Chênh lệch Hive − snapshot: không xác định",
        "- Cách đếm: đọc footer Parquet qua WebHDFS; không đọc các dòng chuyến.",
        f"- Tháng Hive không có trong manifest: {', '.join(missing_manifest_months) or 'không có'}",
        f"- Tháng manifest chưa có trong Hive: {', '.join(missing_hive_months) or 'không có'}",
        "",
        "| Tháng | Manifest | Raw Hive | Hive − manifest | File Parquet |",
        "|---|---:|---:|---:|---:|",
    ]
    markdown.extend(
        f"| {row['month']} | {row['manifest_rows']:,} | {row['hive_raw_rows']:,} | "
        f"{row['delta_hive_minus_manifest']:+,} | {row['raw_parquet_files']} |"
        for row in rows
    )
    markdown_path.write_text("\n".join(markdown) + "\n", encoding="utf-8")

    logging.info(f"[AUDIT] Manifest: {manifest_total:,}; raw Hive: {hive_total:,}.")
    logging.info(f"[AUDIT] Chênh lệch so manifest: {hive_total - manifest_total:+,}.")
    logging.info(f"[AUDIT] CSV: {csv_path}")
    logging.info(f"[AUDIT] Report: {markdown_path}")
    return result


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-host", default="192.168.100.17")
    parser.add_argument("--raw-path", default="/data/nyc_taxi/raw/trips")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=project_root / "2-processing" / "reports" / "source_manifest.csv",
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=project_root / "4-bi" / "cache" / "dashboard_hive_cache.json",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "3-analytics" / "artifacts",
    )
    args = parser.parse_args()
    reconcile(
        args.warehouse_host,
        args.raw_path,
        args.manifest,
        args.cache,
        args.output_dir,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
