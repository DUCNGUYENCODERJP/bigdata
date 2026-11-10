"""
feature_engineering.py
----------------------
Feature engineering and chronological train/test split.
Re-exported from modules/ for modularity and pipeline integration.
"""

from modules.features import build_feature_data
from modules.splitter import chronological_split

__all__ = [
    "build_feature_data",
    "chronological_split",
]
