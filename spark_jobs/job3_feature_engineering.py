"""
Spark Job 3 — Feature Engineering for ML.

Reads from marts.fact_streams and marts.dim_track (dbt outputs),
computes rolling 30-day play counts per track, assembles audio
feature vectors, creates a binary popularity label, and writes
the feature store to ml.feature_store.
"""

import os
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import FloatType, IntegerType

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job3_feature_engineering")

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


def create_spark_session():
    """
    Create and return a SparkSession with PostgreSQL JDBC support.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("SpotifyJob3_FeatureEngineering")
        .getOrCreate()
    )


def read_jdbc_table(spark, table_name):
    """
    Read a PostgreSQL table into a Spark DataFrame via JDBC.

    Args:
        spark: SparkSession instance.
        table_name: Fully qualified table name (schema.table).

    Returns:
        pyspark.sql.DataFrame: Table contents.
    """
    logger.info("Reading table: %s", table_name)
    df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)
    count = df.count()
    logger.info("  Row count: %d", count)
    return df


def build_feature_store(df_tracks, df_streams):
    """
    Build the ML feature store from mart tables.

    Steps:
        1. Compute rolling_30d_plays per track_id from fact_streams
        2. Join with dim_track for audio features
        3. Create popularity_label: 1 if popularity >= 70, else 0
        4. Select final feature columns

    Args:
        df_tracks: dim_track DataFrame (audio features + metadata).
        df_streams: fact_streams DataFrame (play counts by date).

    Returns:
        pyspark.sql.DataFrame: Feature store DataFrame.
    """
    logger.info("Building feature store...")

    # Compute rolling 30-day play count per track
    if "date_id" in df_streams.columns:
        window_spec = (
            Window
            .partitionBy("track_id")
            .orderBy(F.col("date_id").cast("long"))
            .rangeBetween(-30 * 86400, 0)  # 30 days in seconds
        )
        stream_features = (
            df_streams
            .groupBy("track_id")
            .agg(
                F.sum("play_count").alias("rolling_30d_plays"),
            )
        )
    else:
        # Fallback: sum all plays per track if no date column
        stream_features = (
            df_streams
            .groupBy("track_id")
            .agg(
                F.sum("play_count").alias("rolling_30d_plays"),
            )
        )

    # Join tracks with stream features
    features = df_tracks.join(
        stream_features,
        on="track_id",
        how="left",
    )

    # Fill missing rolling_30d_plays with 0
    features = features.withColumn(
        "rolling_30d_plays",
        F.coalesce(F.col("rolling_30d_plays"), F.lit(0)).cast(IntegerType()),
    )

    # Create binary popularity label
    features = features.withColumn(
        "popularity_label",
        F.when(F.col("popularity") >= 70, F.lit(1)).otherwise(F.lit(0)),
    )

    # Select final feature store columns
    feature_cols = [
        "track_id",
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence",
        "tempo", "duration_ms", "rolling_30d_plays",
        "popularity_label", "genre",
    ]

    # Only use columns that exist
    existing = [c for c in feature_cols if c in features.columns]
    features = features.select(*existing)

    # If 'genre' column doesn't exist, add it as null
    if "genre" not in features.columns:
        features = features.withColumn("genre", F.lit(None).cast("string"))

    logger.info("Feature store: %d rows, %d columns", features.count(), len(features.columns))
    return features


def main():
    """
    Main entry point: reads mart tables, builds features, writes to ml.feature_store.

    Returns:
        None
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Spark Job 3 — Feature Engineering — Starting")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Read mart tables
        df_tracks = read_jdbc_table(spark, "marts.dim_track")
        df_streams = read_jdbc_table(spark, "marts.fact_streams")

        # Build feature store
        df_features = build_feature_store(df_tracks, df_streams)

        # Write to ml.feature_store
        logger.info("Writing feature store to ml.feature_store...")
        df_features.write.jdbc(
            url=JDBC_URL,
            table="ml.feature_store",
            mode="overwrite",
            properties={**JDBC_PROPERTIES, "truncate": "true"},
        )

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Job 3 completed in %.1f seconds", elapsed)
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
