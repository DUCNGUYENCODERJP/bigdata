"""Poll HDFS metadata and load only unseen monthly partitions."""
from __future__ import annotations
import logging

import json
import time
import urllib.parse
import urllib.request

from .cache import _save_persistent_cache
from .cache_merge import _apply_storage_metadata, _merge_partition_delta
from .context import DATABASE_CACHE, DATABASE_LOCK, INCREMENTAL_POLL_SECONDS, INCREMENTAL_SYNC_LOCK
from .warehouse import _format_size, _month_sort_key, discover_warehouse_state, query_warehouse


def _webhdfs_metadata_fingerprint(warehouse_host: str) -> dict:
    """Read tiny HDFS metadata summaries without starting Spark or reading Parquet rows."""
    summaries = {}
    for name, path in (
        ("clean", "/data/nyc_taxi/clean/trips"),
        ("raw", "/data/nyc_taxi/raw/trips"),
    ):
        query = urllib.parse.urlencode({"op": "GETCONTENTSUMMARY", "user.name": "hadoop"})
        url = f"http://{warehouse_host}:9870/webhdfs/v1{path}?{query}"
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=4) as response:
            payload = json.load(response)
        content = payload.get("ContentSummary") or {}
        if not content:
            remote_error = payload.get("RemoteException", {}).get("message", "empty ContentSummary")
            raise RuntimeError(f"WebHDFS {name}: {remote_error}")
        summaries[name] = {
            "bytes": int(content.get("length", 0)),
            "files": int(content.get("fileCount", 0)),
            "directories": int(content.get("directoryCount", 0)),
        }
    return {
        "fingerprint": tuple(
            value
            for name in ("clean", "raw")
            for value in (
                summaries[name]["bytes"],
                summaries[name]["files"],
                summaries[name]["directories"],
            )
        ),
        "storage_bytes": summaries["clean"]["bytes"],
        "storage_files": summaries["clean"]["files"],
    }


def _friendly_incremental_error(error: Exception) -> str:
    """Turn common warehouse-offline failures into a useful status message."""
    detail = str(error)
    lower_detail = detail.lower()
    if "sessionhivemetastoreclient" in lower_detail or "9083" in lower_detail:
        return (
            "Không kết nối được Hive Metastore trên máy kho (có thể máy đang tắt "
            "hoặc service metastore chưa chạy). Bật máy kho và chạy `docker compose up -d` "
            "trong thư mục 1-warehouse; cache hiện tại vẫn dùng được và hệ thống sẽ tự thử lại."
        )
    if "webhdfs" in lower_detail or "9870" in lower_detail or "timed out" in lower_detail:
        return (
            "Không kết nối được HDFS/WebHDFS trên máy kho. Kiểm tra máy kho và Docker; "
            "cache hiện tại vẫn dùng được và hệ thống sẽ tự thử lại khi dịch vụ hoạt động."
        )
    return detail


def _run_incremental_sync() -> bool:
    """Auto-detect new HDFS data; query only Hive months absent from the durable cache."""
    if not INCREMENTAL_SYNC_LOCK.acquire(blocking=False):
        return False

    try:
        with DATABASE_LOCK:
            if DATABASE_CACHE["data"] is None or DATABASE_CACHE["partition"] != "all":
                return False
            DATABASE_CACHE["is_querying"] = True
            host = DATABASE_CACHE["warehouse_host"]
            clean_table = DATABASE_CACHE["clean_table"]
            raw_table = DATABASE_CACHE["raw_table"]

        try:
            metadata = _webhdfs_metadata_fingerprint(host)
            fingerprint = metadata["fingerprint"]
            now = time.time()
            with DATABASE_LOCK:
                baseline = DATABASE_CACHE["warehouse_fingerprint"]
                DATABASE_CACHE["last_incremental_check"] = now
                DATABASE_CACHE["storage_bytes"] = metadata["storage_bytes"]
                DATABASE_CACHE["storage_files"] = metadata["storage_files"]
                file_suffix = f" ({metadata['storage_files']:,} files)"
                DATABASE_CACHE["storage_size"] = (
                    f"{_format_size(metadata['storage_bytes'])} Parquet{file_suffix}"
                )
                if baseline is not None and fingerprint == baseline:
                    DATABASE_CACHE["pending_fingerprint"] = None
                    DATABASE_CACHE["pending_fingerprint_samples"] = 0
                    DATABASE_CACHE["last_incremental_error"] = None
                    return True

                if baseline is not None:
                    if DATABASE_CACHE["pending_fingerprint"] == fingerprint:
                        DATABASE_CACHE["pending_fingerprint_samples"] += 1
                    else:
                        DATABASE_CACHE["pending_fingerprint"] = fingerprint
                        DATABASE_CACHE["pending_fingerprint_samples"] = 1
                    if DATABASE_CACHE["pending_fingerprint_samples"] < 4:
                        return True

            state = discover_warehouse_state(host, clean_table, raw_table)
            state.update(
                storage_bytes=metadata["storage_bytes"],
                storage_files=metadata["storage_files"],
            )
            discovered = set(state["partitions"])
            with DATABASE_LOCK:
                covered = set(DATABASE_CACHE["covered_partitions"])
                current = DATABASE_CACHE["data"]
                DATABASE_CACHE["last_incremental_check"] = time.time()
                DATABASE_CACHE["last_incremental_error"] = None
                missing = sorted(discovered - covered, key=_month_sort_key)

            if not missing:
                with DATABASE_LOCK:
                    DATABASE_CACHE["warehouse_fingerprint"] = fingerprint
                    DATABASE_CACHE["pending_fingerprint"] = None
                    DATABASE_CACHE["pending_fingerprint_samples"] = 0
                    if baseline is not None and fingerprint != baseline:
                        DATABASE_CACHE["last_incremental_error"] = (
                            "HDFS có thay đổi nhưng không có partition tháng mới; dữ liệu trong tháng đã cache được giữ nguyên."
                        )
                return True

            logging.info(
                f"[CACHE] Phát hiện {len(missing)} partition mới; chỉ đọc các tháng: {', '.join(missing)}."
            )
            for month in missing:
                year, month_number = _month_sort_key(month)
                partition = f"year={year}/month={month_number}"
                delta = query_warehouse(host, partition, clean_table, raw_table)
                if int(delta.get("kpis", {}).get("total_records") or 0) <= 0:
                    raise RuntimeError(f"Partition {partition} không trả về dữ liệu clean hợp lệ.")

                with DATABASE_LOCK:
                    current = DATABASE_CACHE["data"]
                    merged = _merge_partition_delta(current, delta, month)
                    _apply_storage_metadata(merged, state)
                    covered.add(month)
                    saved_at = time.time()
                    DATABASE_CACHE["data"] = merged
                    DATABASE_CACHE["covered_partitions"] = sorted(covered)
                    DATABASE_CACHE["last_queried"] = saved_at
                    DATABASE_CACHE["cache_source"] = "incremental"
                    DATABASE_CACHE["last_error"] = None
                    _save_persistent_cache(merged, saved_at, sorted(covered))
                logging.info(f"[CACHE] Đã gộp và lưu partition {month}; tổng đã cache {len(covered)} tháng.")
            with DATABASE_LOCK:
                DATABASE_CACHE["warehouse_fingerprint"] = fingerprint
                DATABASE_CACHE["pending_fingerprint"] = None
                DATABASE_CACHE["pending_fingerprint_samples"] = 0
            return True
        except Exception as error:
            friendly_error = _friendly_incremental_error(error)
            with DATABASE_LOCK:
                DATABASE_CACHE["last_incremental_check"] = time.time()
                DATABASE_CACHE["last_incremental_error"] = friendly_error
                DATABASE_CACHE["pending_fingerprint_samples"] = 0
            logging.info(
                f"[CACHE] Đồng bộ tăng dần thất bại; cache cũ vẫn được giữ: "
                f"{friendly_error} Chi tiết: {error}"
            )
            return False
        finally:
            with DATABASE_LOCK:
                DATABASE_CACHE["is_querying"] = False
    finally:
        INCREMENTAL_SYNC_LOCK.release()


def _incremental_watcher() -> None:
    while True:
        _run_incremental_sync()
        time.sleep(INCREMENTAL_POLL_SECONDS)
