"""
ML Model Training — Popularity Prediction (GBTRegressor).

Reads audio features from ml.feature_store, trains a Gradient Boosted
Tree regressor to predict track popularity (0-100), logs metrics and
model to MLflow, and registers the model as 'popularity_predictor'.
"""

import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("train_popularity_model")

POSTGRES_USER = os.environ.get("POSTGRES_USER", "spotify_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "spotify_pass")
JDBC_URL = os.environ.get(
    "SPARK_POSTGRES_JDBC_URL",
    "jdbc:postgresql://postgres:5432/spotify_platform",
)
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.environ.get("MLFLOW_EXPERIMENT_NAME", "spotify_platform")

FEATURE_COLS = [
    "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence",
    "tempo", "rolling_30d_plays",
]
TARGET_COL = "popularity_label"


def create_spark_session():
    """
    Create a SparkSession with JDBC and MLflow support.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("SpotifyML_PopularityPredictor")
        .getOrCreate()
    )


def main():
    """
    Main entry point: trains a GBTRegressor on audio features to predict popularity.

    Steps:
        1. Read ml.feature_store from Postgres
        2. Assemble features into a vector
        3. Split 80/20 train/test
        4. Train GBTRegressor
        5. Evaluate (RMSE, MAE, R²)
        6. Log metrics and model to MLflow

    Returns:
        None
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("ML Training — Popularity Prediction — Starting")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        import mlflow
        import mlflow.spark

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment(MLFLOW_EXPERIMENT)

        # Read feature store
        logger.info("Reading ml.feature_store...")
        df = spark.read.jdbc(
            url=JDBC_URL,
            table="ml.feature_store",
            properties=JDBC_PROPERTIES,
        )
        logger.info("Feature store rows: %d", df.count())

        # Drop rows with nulls in feature or target columns
        df_clean = df.dropna(subset=FEATURE_COLS + [TARGET_COL])
        logger.info("Rows after null removal: %d", df_clean.count())

        # Assemble features
        assembler = VectorAssembler(
            inputCols=FEATURE_COLS,
            outputCol="features",
            handleInvalid="skip",
        )

        # GBT Regressor
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol=TARGET_COL,
            maxIter=50,
            maxDepth=5,
            seed=42,
        )

        pipeline = Pipeline(stages=[assembler, gbt])

        # Train/test split
        train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
        logger.info("Train: %d rows, Test: %d rows", train_df.count(), test_df.count())

        # Train with MLflow tracking
        with mlflow.start_run(run_name="gbt_popularity_v1"):
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)

            # Evaluate
            rmse_eval = RegressionEvaluator(
                labelCol=TARGET_COL, predictionCol="prediction", metricName="rmse"
            )
            mae_eval = RegressionEvaluator(
                labelCol=TARGET_COL, predictionCol="prediction", metricName="mae"
            )
            r2_eval = RegressionEvaluator(
                labelCol=TARGET_COL, predictionCol="prediction", metricName="r2"
            )

            rmse = rmse_eval.evaluate(predictions)
            mae = mae_eval.evaluate(predictions)
            r2 = r2_eval.evaluate(predictions)

            logger.info("RMSE: %.4f", rmse)
            logger.info("MAE:  %.4f", mae)
            logger.info("R²:   %.4f", r2)

            # Log to MLflow
            mlflow.log_param("algorithm", "GBTRegressor")
            mlflow.log_param("max_iter", 50)
            mlflow.log_param("max_depth", 5)
            mlflow.log_param("features", FEATURE_COLS)
            mlflow.log_param("train_rows", train_df.count())
            mlflow.log_param("test_rows", test_df.count())
            mlflow.log_metric("rmse", rmse)
            mlflow.log_metric("mae", mae)
            mlflow.log_metric("r2", r2)

            # Log model
            mlflow.spark.log_model(
                model,
                artifact_path="popularity_predictor",
                registered_model_name="popularity_predictor",
            )

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Training complete in %.1f seconds", elapsed)
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
