"""
ML Model Training — Genre Classification (RandomForest).

Reads audio features from ml.feature_store, filters for tracks
with genre labels, trains a Random Forest multi-class classifier,
logs metrics (accuracy, F1 macro) and model to MLflow.
"""

import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StringIndexer, IndexToString
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("train_genre_classifier")

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


def create_spark_session():
    """
    Create a SparkSession with JDBC and MLflow support.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("SpotifyML_GenreClassifier")
        .getOrCreate()
    )


def main():
    """
    Main entry point: trains a RandomForestClassifier for genre prediction.

    Steps:
        1. Read ml.feature_store, filter for non-null genres
        2. StringIndex the genre labels
        3. Assemble feature vector
        4. Split 80/20 train/test
        5. Train RandomForestClassifier
        6. Evaluate (accuracy, F1 macro)
        7. Log metrics and model to MLflow

    Returns:
        None
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("ML Training — Genre Classification — Starting")
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

        # Filter for rows with genre labels
        df_labeled = df.filter(df.genre.isNotNull() & (df.genre != ""))
        logger.info("Rows with genre labels: %d", df_labeled.count())

        if df_labeled.count() < 50:
            logger.warning("Not enough labeled data for training. Exiting.")
            return

        # Drop nulls in feature columns
        df_clean = df_labeled.dropna(subset=FEATURE_COLS)

        # Pipeline stages
        genre_indexer = StringIndexer(
            inputCol="genre",
            outputCol="genre_index",
            handleInvalid="skip",
        )

        assembler = VectorAssembler(
            inputCols=FEATURE_COLS,
            outputCol="features",
            handleInvalid="skip",
        )

        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="genre_index",
            numTrees=100,
            maxDepth=10,
            seed=42,
        )

        label_converter = IndexToString(
            inputCol="prediction",
            outputCol="predicted_genre",
            labels=genre_indexer.fit(df_clean).labels,
        )

        pipeline = Pipeline(stages=[genre_indexer, assembler, rf])

        # Train/test split
        train_df, test_df = df_clean.randomSplit([0.8, 0.2], seed=42)
        logger.info("Train: %d rows, Test: %d rows", train_df.count(), test_df.count())

        # Train with MLflow tracking
        with mlflow.start_run(run_name="rf_genre_classifier_v1"):
            model = pipeline.fit(train_df)
            predictions = model.transform(test_df)

            # Evaluate
            accuracy_eval = MulticlassClassificationEvaluator(
                labelCol="genre_index",
                predictionCol="prediction",
                metricName="accuracy",
            )
            f1_eval = MulticlassClassificationEvaluator(
                labelCol="genre_index",
                predictionCol="prediction",
                metricName="f1",
            )

            accuracy = accuracy_eval.evaluate(predictions)
            f1_macro = f1_eval.evaluate(predictions)

            logger.info("Accuracy: %.4f", accuracy)
            logger.info("F1 Macro: %.4f", f1_macro)

            # Log to MLflow
            mlflow.log_param("algorithm", "RandomForestClassifier")
            mlflow.log_param("num_trees", 100)
            mlflow.log_param("max_depth", 10)
            mlflow.log_param("features", FEATURE_COLS)
            mlflow.log_param("train_rows", train_df.count())
            mlflow.log_param("test_rows", test_df.count())
            mlflow.log_metric("accuracy", accuracy)
            mlflow.log_metric("f1_macro", f1_macro)

            # Log model
            mlflow.spark.log_model(
                model,
                artifact_path="genre_classifier",
                registered_model_name="genre_classifier",
            )

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Training complete in %.1f seconds", elapsed)
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
