"""Shared paths, process configuration, and dashboard state."""
from __future__ import annotations

import os
import sys
import threading

if sys.stdout.encoding != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

CURRENT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
ANALYTICS_DIR = os.path.join(PROJECT_ROOT, "3-analytics")
if ANALYTICS_DIR not in sys.path:
    sys.path.insert(0, ANALYTICS_DIR)

java_candidates = [
    r"C:\Program Files\Java\jdk-17",
    r"C:\Program Files\Eclipse Adoptium\jdk-17.0.15.6-hotspot",
    os.environ.get("JAVA_HOME"),
]


def _is_java_17(path: str) -> bool:
    if not os.path.isfile(os.path.join(path, "bin", "java.exe")):
        return False
    try:
        with open(os.path.join(path, "release"), encoding="utf-8") as release_file:
            version_line = next(
                (line for line in release_file if line.startswith("JAVA_VERSION=")),
                "",
            )
        version = version_line.partition("=")[2].strip().strip('"')
        return version.split(".", 1)[0] == "17"
    except OSError:
        return os.path.basename(path).lower().startswith("jdk-17")


java_home = next(
    (path for path in java_candidates if path and _is_java_17(path)),
    None,
)
if java_home:
    os.environ["JAVA_HOME"] = java_home

hadoop_candidates = [os.environ.get("HADOOP_HOME"), r"C:\hadoop", r"D:\hadoop"]
hadoop_home = next(
    (path for path in hadoop_candidates if path and os.path.isfile(os.path.join(path, "bin", "winutils.exe"))),
    None,
)
if hadoop_home:
    os.environ["HADOOP_HOME"] = hadoop_home
path_prefix = [
    os.path.join(java_home, "bin") if java_home else None,
    os.path.join(hadoop_home, "bin") if hadoop_home else None,
]
path_prefix = [path for path in path_prefix if path]
if path_prefix:
    os.environ["PATH"] = os.pathsep.join([*path_prefix, os.environ.get("PATH", "")])

DATABASE_CACHE: dict = {
    "data": None,
    "last_queried": None,
    "cache_source": None,
    "is_querying": False,
    "warehouse_host": "192.168.100.17",
    "partition": "all",
    "clean_table": "default.clean_nyc_taxi_tlc",
    "raw_table": "default.raw_nyc_taxi",
    "last_query_started": None,
    "covered_partitions": [],
    "last_incremental_check": None,
    "last_incremental_error": None,
    "warehouse_fingerprint": None,
    "pending_fingerprint": None,
    "pending_fingerprint_samples": 0,
    "storage_bytes": None,
    "storage_files": None,
    "storage_size": None,
    "last_error": None,
}
DATABASE_LOCK = threading.Lock()
INCREMENTAL_SYNC_LOCK = threading.Lock()
CACHE_VERSION = 1
INCREMENTAL_POLL_SECONDS = max(3, int(os.environ.get("NYC_TAXI_CACHE_POLL_SECONDS", "5")))
PROJECT_CACHE_IDENTITY = {
    "warehouse_host": "192.168.100.17",
    "partition": "all",
    "clean_table": "default.clean_nyc_taxi_tlc",
    "raw_table": "default.raw_nyc_taxi",
}
