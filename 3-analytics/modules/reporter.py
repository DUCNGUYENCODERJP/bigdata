"""
modules/reporter.py
-------------------
Định dạng và ghi bảng so sánh các chỉ số mô hình vào log.
"""
import logging

from typing import Dict, List, Optional


def _fmt_mape(mape: Optional[float]) -> str:
    return f"{mape:.2f}%" if mape is not None else "N/A"


def _log_improvement(
    base: Dict[str, Optional[float]],
    model: Dict[str, Optional[float]],
) -> None:
    """In phần trăm cải thiện tương đối của model so với baseline."""
    def pct_imp(b: Optional[float], m: Optional[float]) -> str:
        if b is None or m is None or b == 0:
            return "N/A"
        return f"{(b - m) / b * 100:+.1f}%"

    logging.info(
        f"  Improvement over baseline →"
        f"  RMSE: {pct_imp(base['rmse'], model['rmse'])}"
        f"  |  MAE: {pct_imp(base['mae'], model['mae'])}"
        f"  |  MAPE: {pct_imp(base['mape'], model['mape'])}"
    )
    logging.info("")


def log_comparison_table(metrics_list: List[Dict[str, Optional[float]]]) -> None:
    """Ghi bảng so sánh song song giữa Baseline và GBTRegressor."""
    if not metrics_list:
        logging.info("No metrics to display.")
        return

    METRIC_COL_W = 12
    model_names = [m["model"] for m in metrics_list]
    value_widths = [max(len(n), 12) for n in model_names]

    total_w = METRIC_COL_W + sum(w + 4 for w in value_widths)

    header = f"{'Metric':<{METRIC_COL_W}}"
    for name, w in zip(model_names, value_widths):
        header += f" │ {name:<{w}}"

    sep_line = "─" * METRIC_COL_W
    for w in value_widths:
        sep_line += "─┼─" + "─" * w

    logging.info("")
    logging.info("─" * total_w)
    logging.info(header)
    logging.info(sep_line)

    rows = [
        ("RMSE",     [f"{m['rmse']:.4f}"          for m in metrics_list]),
        ("MAE",      [f"{m['mae']:.4f}"            for m in metrics_list]),
        ("MAPE (%)", [_fmt_mape(m["mape"])         for m in metrics_list]),
    ]
    for metric_name, values in rows:
        row = f"{metric_name:<{METRIC_COL_W}}"
        for v, w in zip(values, value_widths):
            row += f" │ {v:<{w}}"
        logging.info(row)

    logging.info("─" * total_w)
    logging.info("")

    if len(metrics_list) == 2:
        _log_improvement(metrics_list[0], metrics_list[1])
