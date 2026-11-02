"""
model.py
--------
GBTRegressor ML pipeline and baseline model for taxi demand forecasting.

Two models are defined here:

1. **GBT pipeline** (``build_gbt_pipeline`` / ``train_gbt`` / ``add_gbt_predictions``)
   A Spark ML Pipeline that encodes the pickup zone with OneHotEncoder,
   assembles all features into a single vector, then trains a
   GBTRegressor.  Negative predictions are clipped to 0 at inference
   time (demand cannot be negative).

2. **Baseline model** (``add_baseline_predictions``)
   Historical mean trip count per (PULocationID × pickup_hour).
   Zone / hour combinations unseen during training fall back to the
   global mean so that the baseline always produces a valid prediction.
"""

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ── GBT Pipeline ──────────────────────────────────────────────────────────────

def build_gbt_pipeline(max_iter: int = 50, seed: int = 42) -> Pipeline:
    """Construct the full Spark ML Pipeline for demand forecasting.

    Stages
    ------
    1. StringIndexer   — encode PULocationID (string/long) → numeric index.
    2. OneHotEncoder   — convert zone index → sparse binary vector.
                         ``dropLast=False`` preserves all zone categories.
    3. VectorAssembler — combine zone_vector + pickup_hour + day_of_week
                         + date_index into a single features column.
    4. GBTRegressor    — gradient-boosted trees regression.

    Features used (in order)
    ------------------------
    zone_vector  : OneHotEncoded PULocationID  (categorical)
    pickup_hour  : 0–23                         (ordinal / cyclic)
    day_of_week  : 1–7                          (ordinal / cyclic)
    date_index   : days since 1970-01-01        (long-term trend)

    Intentionally excluded
    ----------------------
    passenger_count — unreliable in raw data; adds noise to the model.

    Parameters
    ----------
    max_iter:
        Maximum boosting rounds.  Default: 50.
    seed:
        Random seed for reproducibility.

    Returns
    -------
    Pipeline
        An unfitted Spark ML Pipeline.
    """
    zone_indexer = StringIndexer(
        inputCol="PULocationID",
        outputCol="zone_index",
        handleInvalid="keep",   # keeps unseen zones as a separate category
    )
    zone_encoder = OneHotEncoder(
        inputCol="zone_index",
        outputCol="zone_vector",
        dropLast=False,          # retain all zone categories
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
    """Fit the GBT Pipeline on *train_data*.

    Parameters
    ----------
    train_data:
        Output of :func:`feature_engineering.build_feature_data` (training split).
    max_iter:
        GBT boosting rounds.
    seed:
        Random seed.

    Returns
    -------
    PipelineModel
        Fitted pipeline ready for :func:`add_gbt_predictions`.
    """
    return build_gbt_pipeline(max_iter=max_iter, seed=seed).fit(train_data)


def add_gbt_predictions(model: PipelineModel, data: DataFrame) -> DataFrame:
    """Apply a fitted GBT model to *data* and add a ``gbt_prediction`` column.

    The raw ``prediction`` column is clipped to 0 so that no negative
    demand values are emitted.  The raw column is dropped afterwards.

    Parameters
    ----------
    model:
        Fitted PipelineModel from :func:`train_gbt`.
    data:
        DataFrame to score (must contain the same feature columns used
        during training).

    Returns
    -------
    DataFrame
        Input DataFrame enriched with a ``gbt_prediction`` DOUBLE column.
    """
    return (
        model.transform(data)
        .withColumn("gbt_prediction", F.greatest(F.col("prediction"), F.lit(0.0)))
        .drop("prediction", "features", "zone_index", "zone_vector")
    )


# ── Baseline Model ─────────────────────────────────────────────────────────────

def add_baseline_predictions(test: DataFrame, train: DataFrame) -> DataFrame:
    """Add historical-mean baseline predictions to the test DataFrame.

    Strategy
    --------
    For each (PULocationID, pickup_hour) pair, compute the mean trip count
    seen in *train_data* and use that as the baseline prediction.

    Fallback
    --------
    Zone / hour combinations not present in training (e.g. new zones) fall
    back to the global mean trip count across all training records,
    ensuring every test row gets a non-NULL prediction.

    Parameters
    ----------
    test:
        Test split DataFrame (output of chronological_split).
    train:
        Training split DataFrame used to compute historical means.

    Returns
    -------
    DataFrame
        *test* enriched with a ``baseline_prediction`` DOUBLE column.
    """
    # Per-(zone × hour) historical mean
    zone_hour_means = train.groupBy("PULocationID", "pickup_hour").agg(
        F.avg("label").alias("_zone_hour_mean")
    )

    # Global fallback mean (for unseen zone/hour combos)
    global_mean_row = train.agg(F.avg("label").alias("gm")).first()
    global_mean: float = float(global_mean_row["gm"]) if global_mean_row["gm"] is not None else 0.0

    return (
        test
        .join(zone_hour_means, on=["PULocationID", "pickup_hour"], how="left")
        .withColumn(
            "baseline_prediction",
            F.coalesce(F.col("_zone_hour_mean"), F.lit(global_mean)),
        )
        .drop("_zone_hour_mean")
    )
