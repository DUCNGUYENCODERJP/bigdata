"""
modules/splitter.py
-------------------
Chia tập dữ liệu thành Train và Test theo thứ tự thời gian (Chronological Split)
nhằm tránh rò rỉ dữ liệu tương lai (No future leakage) trong bài toán chuỗi thời gian.
"""

from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def chronological_split(
    data: DataFrame,
    train_ratio: float = 0.8,
) -> Tuple[DataFrame, DataFrame]:
    """Chia data thành (train_df, test_df) theo phân vị thời gian trên date_index."""
    if not (0.0 < train_ratio < 1.0):
        raise ValueError(f"train_ratio phải nằm trong khoảng (0, 1); nhận được: {train_ratio}")

    cutoffs = data.approxQuantile("date_index", [train_ratio], 0.0)
    if not cutoffs:
        raise ValueError("Không thể chia tập dữ liệu: không có bản ghi nào.")
    cutoff = cutoffs[0]

    train = data.filter(F.col("date_index") <= F.lit(cutoff))
    test = data.filter(F.col("date_index") > F.lit(cutoff))

    if train.limit(1).count() == 0 or test.limit(1).count() == 0:
        raise ValueError(
            "Phân tách thời gian tạo ra phân vùng rỗng. "
            "Hãy đảm bảo dữ liệu trải dài qua ít nhất 2 ngày khác nhau."
        )

    return train, test
