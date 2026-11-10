"""HTTP routes for dashboard assets, data, and runtime status."""
from __future__ import annotations
import logging

import gzip
import http.server
import json
import time
import urllib.parse

from .cache import _covered_partitions_from_data, _save_persistent_cache
from .context import CURRENT_DIR, DATABASE_CACHE, DATABASE_LOCK
from .warehouse import query_warehouse


class DashboardRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Serve the dashboard and Hive SQL API."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=CURRENT_DIR, **kwargs)

    def _send_json(self, status_code: int, data: dict) -> None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        accept_encoding = self.headers.get("Accept-Encoding", "")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")

        if "gzip" in accept_encoding and len(payload) > 512:
            compressed = gzip.compress(payload, compresslevel=6)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(compressed)))
            self.end_headers()
            self.wfile.write(compressed)
        else:
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/data":
            if DATABASE_CACHE["data"] is None:
                request_started_at = time.monotonic()
                query_was_running = DATABASE_CACHE["is_querying"]
                try:
                    with DATABASE_LOCK:
                        previous_query_started = DATABASE_CACHE["last_query_started"]
                        coalesced_with_running_query = query_was_running or (
                            previous_query_started is not None
                            and previous_query_started >= request_started_at
                        )
                        should_query = DATABASE_CACHE["data"] is None and not coalesced_with_running_query
                        if should_query:
                            DATABASE_CACHE["is_querying"] = True
                            DATABASE_CACHE["last_query_started"] = time.monotonic()
                            result = query_warehouse(
                                DATABASE_CACHE["warehouse_host"],
                                DATABASE_CACHE["partition"],
                                DATABASE_CACHE["clean_table"],
                                DATABASE_CACHE["raw_table"],
                            )
                            DATABASE_CACHE["data"] = result
                            DATABASE_CACHE["covered_partitions"] = _covered_partitions_from_data(result)
                            DATABASE_CACHE["last_queried"] = time.time()
                            DATABASE_CACHE["cache_source"] = "hive"
                            DATABASE_CACHE["last_error"] = None
                            _save_persistent_cache(
                                DATABASE_CACHE["data"],
                                DATABASE_CACHE["last_queried"],
                                DATABASE_CACHE["covered_partitions"],
                            )
                except Exception as ex:
                    DATABASE_CACHE["last_error"] = str(ex)
                    logging.error(f"[ERROR] Truy vấn Hive SQL thất bại: {ex}")
                    self._send_json(
                        502,
                        {"error": "Không thể truy vấn dữ liệu Hive SQL.", "details": str(ex)},
                    )
                    return
                finally:
                    DATABASE_CACHE["is_querying"] = False
            if DATABASE_CACHE["data"] is None:
                self._send_json(
                    502,
                    {
                        "error": "Không thể truy vấn dữ liệu Hive SQL.",
                        "details": DATABASE_CACHE["last_error"] or "Truy vấn Hive trước đó đã thất bại.",
                    },
                )
                return
            response_data = dict(DATABASE_CACHE["data"])
            response_data["_cache_updated_at"] = DATABASE_CACHE["last_queried"]
            self._send_json(200, response_data)
            return

        if parsed.path == "/api/status":
            self._send_json(
                200,
                {
                    "warehouse_host": DATABASE_CACHE["warehouse_host"],
                    "partition": DATABASE_CACHE["partition"],
                    "clean_table": DATABASE_CACHE["clean_table"],
                    "raw_table": DATABASE_CACHE["raw_table"],
                    "last_queried": DATABASE_CACHE["last_queried"],
                    "is_cached": DATABASE_CACHE["data"] is not None,
                    "cache_source": DATABASE_CACHE["cache_source"],
                    "cache_age_seconds": (
                        max(0, int(time.time() - DATABASE_CACHE["last_queried"]))
                        if DATABASE_CACHE["last_queried"] is not None
                        else None
                    ),
                    "cache_persistent": True,
                    "is_querying": DATABASE_CACHE["is_querying"],
                    "covered_partition_count": len(DATABASE_CACHE["covered_partitions"]),
                    "last_incremental_check": DATABASE_CACHE["last_incremental_check"],
                    "last_incremental_error": DATABASE_CACHE["last_incremental_error"],
                    "storage_bytes": DATABASE_CACHE["storage_bytes"],
                    "storage_files": DATABASE_CACHE["storage_files"],
                    "storage_size": DATABASE_CACHE["storage_size"],
                    "last_error": DATABASE_CACHE["last_error"],
                },
            )
            return

        if parsed.path in ("/", ""):
            self.path = "/dashboard.html"
        super().do_GET()

    def log_message(self, format, *args):
        if args and isinstance(args[0], str) and "/api/data" in args[0]:
            logging.info(f"[HTTP] {args[0]} -> {args[1]}")
