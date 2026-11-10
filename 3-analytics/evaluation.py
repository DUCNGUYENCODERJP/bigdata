"""
evaluation.py
-------------
Model evaluation metrics (RMSE, MAE, MAPE) and reporting.
Re-exported from modules/ for modularity and pipeline integration.
"""

from modules.metrics_eval import compute_metrics
from modules.reporter import log_comparison_table

__all__ = [
    "compute_metrics",
    "log_comparison_table",
]
