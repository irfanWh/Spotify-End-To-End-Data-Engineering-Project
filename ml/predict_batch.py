"""
ML Batch Scoring — Score all tracks in the feature store.

Loads the latest production models (popularity_predictor and
genre_classifier) from the MLflow model registry, scores all
tracks in ml.feature_store, and writes predictions to ml.predictions.
"""

import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("predict_batch")

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

FEATURE_COLS = [
    "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence",
    "tempo", "rolling_30d_plays",
]


def create_spark_session():
    """
    Create a SparkSession with JDBC support.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("SpotifyML_BatchScoring")
        .config("spark.jars", "/opt/spark-jobs/jars/postgresql-42.7.1.jar")
        .getOrCreate()
    )


def main():
    """
    Main entry point: loads MLflow models and scores all tracks.

    Steps:
        1. Read ml.feature_store
        2. Load popularity_predictor from MLflow
        3. Load genre_classifier from MLflow
        4. Score all tracks
        5. Write predictions to ml.predictions

    Returns:
        None
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("ML Batch Scoring — Starting")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        import mlflow
        import mlflow.spark

        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        # Read feature store
        logger.info("Reading ml.feature_store...")
        df = spark.read.jdbc(
            url=JDBC_URL,
            table="ml.feature_store",
            properties=JDBC_PROPERTIES,
        )
        logger.info("Feature store rows: %d", df.count())

        # Drop rows with nulls in feature columns
        df_clean = df.dropna(subset=FEATURE_COLS)

        # Try to load and score with popularity model
        predicted_popularity = None
        try:
            logger.info("Loading popularity_predictor model...")
            popularity_model = mlflow.spark.load_model(
                "models:/popularity_predictor/latest"
            )
            popularity_preds = popularity_model.transform(df_clean)
            popularity_preds = popularity_preds.withColumnRenamed(
                "prediction", "predicted_popularity"
            )
            predicted_popularity = popularity_preds
            logger.info("Popularity predictions generated.")
        except Exception as err:
            logger.warning("Could not load popularity model: %s", err)

        # Try to load and score with genre model
        predicted_genre = None
        try:
            logger.info("Loading genre_classifier model...")
            genre_model = mlflow.spark.load_model(
                "models:/genre_classifier/latest"
            )
            genre_input = df_clean.filter(F.col("genre").isNotNull())
            genre_preds = genre_model.transform(genre_input)
            predicted_genre = genre_preds
            logger.info("Genre predictions generated.")
        except Exception as err:
            logger.warning("Could not load genre model: %s", err)

        # Build final predictions DataFrame
        if predicted_popularity is not None:
            results = predicted_popularity.select(
                F.col("track_id"),
                F.col("predicted_popularity").cast("float"),
                F.lit(None).cast("string").alias("predicted_genre"),
                F.lit("v1.0").alias("model_version"),
                F.current_timestamp().alias("scored_at"),
            )

            logger.info("Writing %d predictions to ml.predictions...", results.count())
            results.write.jdbc(
                url=JDBC_URL,
                table="ml.predictions",
                mode="overwrite",
                properties=JDBC_PROPERTIES,
            )
            logger.info("Predictions written successfully.")
        else:
            logger.warning("No predictions generated — no models available.")

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Batch scoring complete in %.1f seconds", elapsed)
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
