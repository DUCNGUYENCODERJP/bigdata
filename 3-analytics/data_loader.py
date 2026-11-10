"""
data_loader.py
--------------
Data loading utilities from Hive Warehouse.
Re-exported from modules/ for modularity and pipeline integration.
"""

from modules.loader import load_clean_trips, load_raw_trips

__all__ = [
    "load_clean_trips",
    "load_raw_trips",
]
