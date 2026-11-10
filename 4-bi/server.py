"""CLI entry point for the NYC taxi dashboard server."""
from __future__ import annotations
import logging

import argparse
import http.server
import json
import threading
import time
import webbrowser

from dashboard_backend.cache import (
    _covered_partitions_from_data,
    _load_persistent_cache,
    _save_persistent_cache,
)
from dashboard_backend.context import DATABASE_CACHE, DATABASE_LOCK
from dashboard_backend.http_handler import DashboardRequestHandler
from dashboard_backend.sync import _incremental_watcher
from dashboard_backend.warehouse import _parse_partition, _quote_table_name, query_warehouse


def start_server(
    port: int = 8080,
    host: str = "192.168.100.17",
    partition: str = "all",
    clean_table: str = "default.clean_nyc_taxi_tlc",
    raw_table: str = "default.raw_nyc_taxi",
    auto_open: bool = True,
):
    _parse_partition(partition)
    _quote_table_name(clean_table)
    _quote_table_name(raw_table)
    DATABASE_CACHE.update(
        warehouse_host=host,
        partition=partition,
        clean_table=clean_table,
        raw_table=raw_table,
    )

    server_address = ("127.0.0.1", port)
    try:
        httpd = http.server.ThreadingHTTPServer(server_address, DashboardRequestHandler)
    except OSError:
        port = 8050
        server_address = ("127.0.0.1", port)
        httpd = http.server.ThreadingHTTPServer(server_address, DashboardRequestHandler)

    url = f"http://localhost:{port}/dashboard.html"
    logging.info("=" * 70)
    logging.info("NYC TLC YELLOW TAXI — BI DASHBOARD")
    logging.info(f"Hive Metastore : thrift://{host}:9083")
    logging.info(f"HDFS Warehouse : hdfs://{host}:8020/data/nyc_taxi/clean/trips")
    logging.info(f"SQL tables     : {clean_table}, {raw_table}")
    logging.info(f"SQL partition  : {partition}")
    logging.info(f"Dashboard URL  : {url}")
    logging.info("=" * 70)

    cached = _load_persistent_cache()
    if cached is not None:
        (
            DATABASE_CACHE["data"],
            DATABASE_CACHE["last_queried"],
            DATABASE_CACHE["covered_partitions"],
        ) = cached
        DATABASE_CACHE["cache_source"] = "snapshot"
        snapshot_kpis = DATABASE_CACHE["data"].get("kpis", {})
        DATABASE_CACHE["storage_size"] = snapshot_kpis.get("storage_size")
        DATABASE_CACHE["storage_bytes"] = snapshot_kpis.get("storage_bytes")
        DATABASE_CACHE["storage_files"] = snapshot_kpis.get("file_count")
        size_kb = len(json.dumps(DATABASE_CACHE["data"], ensure_ascii=False).encode("utf-8")) / 1024
        kpis = DATABASE_CACHE["data"].get("kpis", {})
        storage_size_str = kpis.get("storage_size", "HDFS Parquet")
        total_records = kpis.get("total_records", 0)
        months_loaded = kpis.get("months_loaded", 0)
        months_expected = kpis.get("months_expected", 0)
        period = kpis.get("period", "")

        logging.info(f"Dung lượng kho : {storage_size_str} ({months_loaded}/{months_expected} tháng • {total_records:,} chuyến)")
        logging.info(f"[STORAGE] Dung lượng kho HDFS đang nạp : {storage_size_str} ({period})")
        logging.info(f"[RECORDS] Quy mô bản ghi đang nạp    : {total_records:,} chuyến xe sạch ({months_loaded} tháng)")
        logging.info(f"[CACHE]   Đã nạp snapshot đã xác minh ({size_kb:.1f} KB) vào RAM.")
        logging.info(f"[CACHE]   Đã đánh dấu {len(DATABASE_CACHE['covered_partitions'])} tháng là đã đọc.")
        logging.info(f"[SERVER]  Máy chủ đang lắng nghe tại  : {url}\n")
    else:
        def preload():
            try:
                with DATABASE_LOCK:
                    DATABASE_CACHE["is_querying"] = True
                    DATABASE_CACHE["last_query_started"] = time.monotonic()
                    DATABASE_CACHE["data"] = query_warehouse(host, partition, clean_table, raw_table)
                    DATABASE_CACHE["covered_partitions"] = _covered_partitions_from_data(DATABASE_CACHE["data"])
                    DATABASE_CACHE["last_queried"] = time.time()
                    DATABASE_CACHE["cache_source"] = "hive"
                    DATABASE_CACHE["last_error"] = None
                    _save_persistent_cache(
                        DATABASE_CACHE["data"],
                        DATABASE_CACHE["last_queried"],
                        DATABASE_CACHE["covered_partitions"],
                    )
                    DATABASE_CACHE["storage_size"] = DATABASE_CACHE["data"]["kpis"].get("storage_size")
                    DATABASE_CACHE["storage_bytes"] = DATABASE_CACHE["data"]["kpis"].get("storage_bytes")
                    DATABASE_CACHE["storage_files"] = DATABASE_CACHE["data"]["kpis"].get("file_count")
            except Exception as ex:
                DATABASE_CACHE["last_error"] = str(ex)
                logging.error(f"[ERROR] Không thể nạp dữ liệu Hive SQL: {ex}")
            finally:
                DATABASE_CACHE["is_querying"] = False

        threading.Thread(target=preload, daemon=True).start()

    threading.Thread(target=_incremental_watcher, daemon=True).start()

    if auto_open:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logging.info("\n[SERVER] Đã dừng máy chủ BI Dashboard.")
        httpd.server_close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Khởi động dashboard dùng Hive SQL.")
    parser.add_argument("--port", type=int, default=8080, help="Cổng HTTP.")
    parser.add_argument("--host", default="192.168.100.17", help="IP warehouse.")
    parser.add_argument("--partition", default="all", help="'all' (tháng cũ nhất đến mới nhất có ở cả bảng clean/raw) hoặc year=YYYY/month=M.")
    parser.add_argument("--clean-table", default="default.clean_nyc_taxi_tlc", help="Bảng Hive dữ liệu sạch.")
    parser.add_argument("--raw-table", default="default.raw_nyc_taxi", help="Bảng Hive dữ liệu raw.")
    parser.add_argument("--no-browser", action="store_true", help="Không tự động mở trình duyệt.")
    args = parser.parse_args()
    start_server(
        port=args.port,
        host=args.host,
        partition=args.partition,
        clean_table=args.clean_table,
        raw_table=args.raw_table,
        auto_open=not args.no_browser,
    )
