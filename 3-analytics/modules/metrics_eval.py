"""
modules/metrics_eval.py
-----------------------
Tính toán các chỉ số đánh giá độ chính xác mô hình: RMSE, MAE và MAPE.
"""

from typing import Dict, Optional
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_metrics(
    predictions: DataFrame,
    prediction_col: str,
    model_name: str,
) -> Dict[str, Optional[float]]:
    """Tính toán RMSE, MAE và MAPE cho một cột dự báo.
    Lưu ý: MAPE chỉ tính trên các dòng có label > 0 để tránh chia cho 0.
    """
    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol=prediction_col,
    )

    rmse = float(evaluator.setMetricName("rmse").evaluate(predictions))
    mae = float(evaluator.setMetricName("mae").evaluate(predictions))

    # MAPE — lọc chỉ các dòng thực tế > 0
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
