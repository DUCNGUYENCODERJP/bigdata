"""
modules/gbt_model.py
--------------------
Định nghĩa, huấn luyện và suy luận mô hình Spark ML GBTRegressor cho bài toán dự báo nhu cầu taxi.
"""

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_gbt_pipeline(max_iter: int = 50, seed: int = 42) -> Pipeline:
    """Xây dựng Pipeline Spark ML cho bài toán hồi quy GBT:
    1. StringIndexer: PULocationID -> zone_index
    2. OneHotEncoder: zone_index -> zone_vector (dropLast=False giữ đủ các zone)
    3. VectorAssembler: zone_vector + pickup_hour + day_of_week + date_index -> features
    4. GBTRegressor: Gradient-Boosted Trees Regression
    """
    zone_indexer = StringIndexer(
        inputCol="PULocationID",
        outputCol="zone_index",
        handleInvalid="keep",
    )
    zone_encoder = OneHotEncoder(
        inputCol="zone_index",
        outputCol="zone_vector",
        dropLast=False,
    )
    assembler = VectorAssembler(
        inputCols=["zone_vector", "pickup_hour", "day_of_week", "date_index"],
        outputCol="features",
        handleInvalid="skip",
    )
    regressor = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        maxIter=max_iter,
        seed=seed,
    )
    return Pipeline(stages=[zone_indexer, zone_encoder, assembler, regressor])


def train_gbt(
    train_data: DataFrame,
    max_iter: int = 50,
    seed: int = 42,
) -> PipelineModel:
    """Huấn luyện mô hình GBT Pipeline trên tập dữ liệu train."""
    return build_gbt_pipeline(max_iter=max_iter, seed=seed).fit(train_data)


def add_gbt_predictions(model: PipelineModel, data: DataFrame) -> DataFrame:
    """Dự báo bằng mô hình GBT đã huấn luyện và clip các giá trị âm về 0."""
    return (
        model.transform(data)
        .withColumn("gbt_prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
        .drop("prediction", "features", "zone_index", "zone_vector")
    )
