"""Durable dashboard snapshot storage and validation."""
from __future__ import annotations
import logging

import hashlib
import json
import os
import tempfile
import time

from .context import CACHE_VERSION, CURRENT_DIR, DATABASE_CACHE, PROJECT_CACHE_IDENTITY


def _project_cache_path() -> str:
    return os.path.join(CURRENT_DIR, "cache", "dashboard_hive_cache.json")


def _cache_identity() -> dict[str, str]:
    return {
        "warehouse_host": DATABASE_CACHE["warehouse_host"],
        "partition": DATABASE_CACHE["partition"],
        "clean_table": DATABASE_CACHE["clean_table"],
        "raw_table": DATABASE_CACHE["raw_table"],
    }


def _cache_path() -> str:
    identity = json.dumps(_cache_identity(), sort_keys=True).encode("utf-8")
    cache_id = hashlib.sha256(identity).hexdigest()
    cache_root = os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.expanduser("~")),
        "NYCTaxiDashboard",
        "cache",
    )
    return os.path.join(cache_root, f"dashboard-{cache_id}.json")


def _covered_partitions_from_data(data: dict) -> list[str]:
    monthly = data.get("monthly", {})
    labels = monthly.get("labels", [])
    counts = monthly.get("counts", [])
    return sorted(
        label for label, count in zip(labels, counts)
        if isinstance(label, str) and count is not None
    )


def _validated_cache_entry(
    cached: object,
    identity: dict[str, str],
) -> tuple[dict, float, list[str]] | None:
    if not isinstance(cached, dict) or cached.get("version") != CACHE_VERSION:
        return None
    saved_at = cached.get("saved_at")
    data = cached.get("data")
    if cached.get("identity") != identity or not isinstance(saved_at, (int, float)):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("kpis"), dict):
        return None
    if data["kpis"].get("partition") != identity["partition"]:
        return None
    if not all(key in data for key in ("monthly", "borough", "top_zones", "hourly", "heatmap", "cancelled", "ml")):
        return None
    # Cache snapshots are durable; age alone must never trigger a full warehouse scan.
    if time.time() - float(saved_at) < 0:
        return None
    covered = cached.get("covered_partitions")
    if (
        not isinstance(covered, list)
        or any(not isinstance(month, str) for month in covered)
        or not covered
    ):
        covered = _covered_partitions_from_data(data)
    return data, float(saved_at), sorted(set(covered))


def _write_json_atomically(path: str, payload: dict, *, compact: bool) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(prefix=".dashboard-cache-", suffix=".tmp", dir=directory)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as cache_file:
            json.dump(
                payload,
                cache_file,
                ensure_ascii=False,
                separators=(",", ":") if compact else None,
                indent=None if compact else 2,
            )
            cache_file.flush()
            os.fsync(cache_file.fileno())
        os.replace(temporary_path, path)
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


def _load_persistent_cache() -> tuple[dict, float, list[str]] | None:
    identity = _cache_identity()
    # Project snapshot chỉ chứa phạm vi mặc định; không được áp dụng cho partition/table khác.
    if identity == PROJECT_CACHE_IDENTITY:
        proj_path = _project_cache_path()
        if os.path.exists(proj_path):
            try:
                with open(proj_path, encoding="utf-8") as f:
                    cached = json.load(f)
                entry = _validated_cache_entry(cached, identity)
                if entry is not None:
                    logging.info(f"[CACHE] Đã nạp snapshot Hive SQL từ dự án ({proj_path}).")
                    return entry
                logging.info(f"[CACHE] Project snapshot sai identity hoặc schema: {proj_path}.")
            except (OSError, json.JSONDecodeError) as ex:
                logging.info(f"[CACHE] Không đọc được project snapshot {proj_path}: {ex}")

    # Cache riêng theo host, partition và tên bảng tại AppData.
    path = _cache_path()
    try:
        with open(path, encoding="utf-8") as cache_file:
            cached = json.load(cache_file)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as ex:
        logging.info(f"[CACHE] Không đọc được cache {path}: {ex}; sẽ truy vấn Hive.")
        return None

    entry = _validated_cache_entry(cached, identity)
    if entry is None:
        logging.info(f"[CACHE] Cache sai identity/schema: {path}; sẽ truy vấn Hive.")
        return None
    return entry


def _save_persistent_cache(
    data: dict,
    saved_at: float,
    covered_partitions: list[str] | None = None,
) -> bool:
    covered = _covered_partitions_from_data(data) if covered_partitions is None else covered_partitions
    identity = _cache_identity()
    payload = {
        "version": CACHE_VERSION,
        "identity": identity,
        "saved_at": saved_at,
        "covered_partitions": sorted(set(covered)),
        "data": data,
    }

    # Chỉ cập nhật snapshot dùng chung cho phạm vi mặc định all; cache khác nằm ở AppData.
    saved = False
    if identity == PROJECT_CACHE_IDENTITY:
        try:
            proj_path = _project_cache_path()
            _write_json_atomically(proj_path, payload, compact=False)
            saved = True
            logging.info(f"[CACHE] Đã cập nhật snapshot dự án tại {proj_path}.")
        except Exception as ex:
            logging.info(f"[CACHE] Không lưu được project cache: {ex}")

    path = _cache_path()
    try:
        _write_json_atomically(path, payload, compact=True)
        saved = True
    except Exception as ex:
        logging.info(f"[CACHE] Không ghi được cache bền vững tại {path}: {ex}")
    return saved
