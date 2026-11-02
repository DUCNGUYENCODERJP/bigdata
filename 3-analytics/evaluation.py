"""
evaluation.py
-------------
Model evaluation metrics (RMSE, MAE, MAPE) and formatted reporting.

Public functions
~~~~~~~~~~~~~~~~
compute_metrics(predictions, prediction_col, model_name)
    → Dict with 'model', 'rmse', 'mae', 'mape'

print_comparison_table(metrics_list)
    → Prints a formatted table comparing all evaluated models side-by-side.

Usage example
~~~~~~~~~~~~~
    from evaluation import compute_metrics, print_comparison_table

    baseline_metrics = compute_metrics(results, "baseline_prediction", "Baseline")
    gbt_metrics      = compute_metrics(results, "gbt_prediction",      "GBTRegressor")

    print_comparison_table([baseline_metrics, gbt_metrics])
"""

from typing import Dict, List, Optional

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ── Public API ─────────────────────────────────────────────────────────────────

def compute_metrics(
    predictions: DataFrame,
    prediction_col: str,
    model_name: str,
) -> Dict[str, Optional[float]]:
    """Compute RMSE, MAE, and MAPE for one prediction column.

    Parameters
    ----------
    predictions:
        DataFrame containing both a ``label`` column (actual trip counts)
        and the *prediction_col* column (predicted values).
    prediction_col:
        Name of the column holding model predictions.
    model_name:
        Human-readable label for the model (used in report tables).

    Returns
    -------
    dict
        {'model': str, 'rmse': float, 'mae': float, 'mape': float | None}

    Notes on MAPE
    -------------
    MAPE is computed only on rows where label > 0 to avoid division by
    zero.  If no positive-label rows exist, 'mape' is returned as None.
    """
    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol=prediction_col,
    )

    rmse = float(evaluator.setMetricName("rmse").evaluate(predictions))
    mae  = float(evaluator.setMetricName("mae").evaluate(predictions))

    # MAPE — filter to positive actuals only
    pos = predictions.filter(F.col("label") > 0)
    mape_row = pos.agg(
        (
            F.avg(
                F.abs(F.col("label") - F.col(prediction_col)) / F.col("label")
            ) * F.lit(100.0)
        ).alias("mape")
    ).first()

    mape: Optional[float] = (
        float(mape_row["mape"]) if mape_row["mape"] is not None else None
    )

    return {"model": model_name, "rmse": rmse, "mae": mae, "mape": mape}


def print_comparison_table(metrics_list: List[Dict[str, Optional[float]]]) -> None:
    """Print a side-by-side comparison table of model metrics.

    Example output for two models:
    ─────────────────────────────────────────────────────────────
    Metric     │ zone_hour_mean_baseline │ gbt_regressor
    ───────────┼─────────────────────────┼───────────────
    RMSE       │ 42.1234                 │ 28.5678
    MAE        │ 31.2345                 │ 19.8765
    MAPE (%)   │ 24.56%                  │ 15.23%
    ─────────────────────────────────────────────────────────────

    Parameters
    ----------
    metrics_list:
        One or more metric dicts produced by :func:`compute_metrics`.
    """
    if not metrics_list:
        print("No metrics to display.")
        return

    # ── Column widths ──────────────────────────────────────────────────────────
    METRIC_COL_W = 12
    model_names = [m["model"] for m in metrics_list]
    value_widths = [max(len(n), 12) for n in model_names]

    total_w = METRIC_COL_W + sum(w + 4 for w in value_widths)

    # ── Header ─────────────────────────────────────────────────────────────────
    header = f"{'Metric':<{METRIC_COL_W}}"
    for name, w in zip(model_names, value_widths):
        header += f" │ {name:<{w}}"

    sep_line = "─" * METRIC_COL_W
    for w in value_widths:
        sep_line += "─┼─" + "─" * w

    print()
    print("─" * total_w)
    print(header)
    print(sep_line)

    # ── Data rows ──────────────────────────────────────────────────────────────
    rows = [
        ("RMSE",     [f"{m['rmse']:.4f}"          for m in metrics_list]),
        ("MAE",      [f"{m['mae']:.4f}"            for m in metrics_list]),
        ("MAPE (%)", [_fmt_mape(m["mape"])         for m in metrics_list]),
    ]
    for metric_name, values in rows:
        row = f"{metric_name:<{METRIC_COL_W}}"
        for v, w in zip(values, value_widths):
            row += f" │ {v:<{w}}"
        print(row)

    print("─" * total_w)
    print()

    # ── Improvement summary (only when exactly two models are provided) ────────
    if len(metrics_list) == 2:
        base, model = metrics_list[0], metrics_list[1]
        _print_improvement(base, model)


# ── Internal helpers ───────────────────────────────────────────────────────────

def _fmt_mape(mape: Optional[float]) -> str:
    return f"{mape:.2f}%" if mape is not None else "N/A"


def _print_improvement(
    base: Dict[str, Optional[float]],
    model: Dict[str, Optional[float]],
) -> None:
    """Print relative improvement of *model* over *base*."""
    def pct_imp(b: Optional[float], m: Optional[float]) -> str:
        if b is None or m is None or b == 0:
            return "N/A"
        return f"{(b - m) / b * 100:+.1f}%"

    print(
        f"  Improvement over baseline →"
        f"  RMSE: {pct_imp(base['rmse'], model['rmse'])}"
        f"  |  MAE: {pct_imp(base['mae'], model['mae'])}"
        f"  |  MAPE: {pct_imp(base['mape'], model['mape'])}"
    )
    print()
