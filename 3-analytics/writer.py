"""
writer.py
---------
Output writers for analytics results and model artifacts (Parquet + CSV).
Re-exported from modules/ for modularity and pipeline integration.
"""

from modules.writers import write_metrics_csv, write_parquet

__all__ = [
    "write_parquet",
    "write_metrics_csv",
]
