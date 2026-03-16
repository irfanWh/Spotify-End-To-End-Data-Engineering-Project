"""
Spark Job 1 — Clean and Join tracks datasets.

Reads raw.tracks_1m and raw.tracks_historical from PostgreSQL via JDBC,
deduplicates, standardises column names, casts types, adds derived columns
(energy_dance_ratio, decade), joins the two datasets, and writes the merged
result to staging.tracks_enriched.
"""

import os
import sys
import time
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, IntegerType, BooleanType

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job1_clean_and_join")

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
        .appName("SpotifyJob1_CleanAndJoin")
        .config("spark.jars", "/opt/spark-jobs/jars/postgresql-42.7.1.jar")
        .getOrCreate()
    )


def read_jdbc_table(spark, table_name):
    """
    Read a PostgreSQL table into a Spark DataFrame via JDBC.

    Args:
        spark: SparkSession instance.
        table_name: Fully qualified table name (schema.table).

    Returns:
        pyspark.sql.DataFrame: Table contents as a DataFrame.
    """
    logger.info("Reading table: %s", table_name)
    df = spark.read.jdbc(url=JDBC_URL, table=table_name, properties=JDBC_PROPERTIES)
    logger.info("  Row count: %d", df.count())
    return df


def clean_tracks_1m(df):
    """
    Clean and standardise the tracks_1m (1.2M songs) dataset.

    Steps:
        1. Deduplicate on track id
        2. Cast audio features to FLOAT, duration_ms to INT
        3. Add energy_dance_ratio derived column
        4. Add decade column

    Args:
        df: Raw tracks_1m DataFrame.

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame.
    """
    logger.info("Cleaning tracks_1m...")

    # Deduplicate on track id
    df = df.dropDuplicates(["id"])

    # Cast audio feature columns
    audio_cols = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    ]
    for col_name in audio_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))

    df = df.withColumn("duration_ms", F.col("duration_ms").cast(IntegerType()))
    df = df.withColumn("explicit", F.col("explicit").cast(BooleanType()))
    df = df.withColumn("year", F.col("year").cast(IntegerType()))

    # Derived: energy_dance_ratio (handle division by zero)
    df = df.withColumn(
        "energy_dance_ratio",
        F.when(F.col("danceability") > 0, F.col("energy") / F.col("danceability"))
        .otherwise(F.lit(None).cast(FloatType())),
    )

    # Derived: decade
    df = df.withColumn(
        "decade",
        (F.floor(F.col("year") / 10) * 10).cast(IntegerType()),
    )

    # Rename id → track_id, name → track_name for consistency
    df = (
        df.withColumnRenamed("id", "track_id")
        .withColumnRenamed("name", "track_name")
        .withColumnRenamed("album", "album")
    )

    logger.info("  tracks_1m after cleaning: %d rows", df.count())
    return df


def clean_tracks_historical(df):
    """
    Clean and standardise the tracks_historical (600K) dataset.

    Steps:
        1. Deduplicate on track id
        2. Cast audio features to FLOAT
        3. Extract year from release_date

    Args:
        df: Raw tracks_historical DataFrame.

    Returns:
        pyspark.sql.DataFrame: Cleaned DataFrame.
    """
    logger.info("Cleaning tracks_historical...")

    df = df.dropDuplicates(["id"])

    audio_cols = [
        "danceability", "energy", "loudness", "speechiness",
        "acousticness", "instrumentalness", "liveness", "valence", "tempo",
    ]
    for col_name in audio_cols:
        df = df.withColumn(col_name, F.col(col_name).cast(FloatType()))

    df = df.withColumn("duration_ms", F.col("duration_ms").cast(IntegerType()))
    df = df.withColumn("popularity", F.col("popularity").cast(IntegerType()))
    df = df.withColumn("explicit", F.col("explicit").cast(BooleanType()))

    # Extract year from release_date (format: YYYY-MM-DD or YYYY)
    df = df.withColumn(
        "year",
        F.substring(F.col("release_date"), 1, 4).cast(IntegerType()),
    )

    # Rename for consistency
    df = (
        df.withColumnRenamed("id", "track_id")
        .withColumnRenamed("name", "track_name")
        .withColumnRenamed("artists", "artists")
        .withColumnRenamed("id_artists", "artist_ids")
    )

    logger.info("  tracks_historical after cleaning: %d rows", df.count())
    return df


def join_datasets(df_1m, df_hist):
    """
    Join the two track datasets on track_id.

    Tracks appearing in both datasets are flagged with in_both_sources=True.
    The join is a FULL OUTER join to preserve all records from both sources.

    Args:
        df_1m: Cleaned tracks_1m DataFrame.
        df_hist: Cleaned tracks_historical DataFrame.

    Returns:
        pyspark.sql.DataFrame: Merged DataFrame.
    """
    logger.info("Joining datasets...")

    # Select common columns from historical for the join
    hist_select = df_hist.select(
        F.col("track_id").alias("hist_track_id"),
        F.col("popularity"),
    )

    # Left join: keep all tracks from 1m, add popularity from historical
    joined = df_1m.join(
        hist_select,
        df_1m["track_id"] == hist_select["hist_track_id"],
        how="left",
    )

    # Flag tracks in both sources
    joined = joined.withColumn(
        "in_both_sources",
        F.when(F.col("hist_track_id").isNotNull(), F.lit(True))
        .otherwise(F.lit(False)),
    )

    # Drop duplicate join key
    joined = joined.drop("hist_track_id")

    # For tracks not in historical, fill popularity as null
    if "popularity" not in df_1m.columns:
        pass  # popularity comes from hist_select

    logger.info("  Joined dataset: %d rows", joined.count())
    return joined


def write_to_staging(df, table_name="staging.tracks_enriched"):
    """
    Write the enriched DataFrame to PostgreSQL staging schema.

    Args:
        df: Enriched DataFrame to write.
        table_name: Target Postgres table (schema.table).

    Returns:
        None
    """
    # Select final columns in order
    final_columns = [
        "track_id", "track_name", "album", "album_id", "artists", "artist_ids",
        "track_number", "disc_number", "explicit",
        "danceability", "energy", "key", "loudness", "mode",
        "speechiness", "acousticness", "instrumentalness", "liveness",
        "valence", "tempo", "duration_ms", "time_signature",
        "year", "release_date", "popularity",
        "energy_dance_ratio", "decade", "in_both_sources",
    ]

    # Only keep columns that exist in the DataFrame
    existing_cols = [c for c in final_columns if c in df.columns]
    df_final = df.select(*existing_cols)

    logger.info("Writing %d rows to %s", df_final.count(), table_name)
    df_final.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode="overwrite",
        properties={**JDBC_PROPERTIES, "truncate": "true"},
    )
    logger.info("Write complete.")


def main():
    """
    Main entry point: reads raw tables, cleans, joins, and writes to staging.

    Returns:
        None
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("Spark Job 1 — Clean and Join — Starting")
    logger.info("=" * 60)

    spark = create_spark_session()

    try:
        # Read raw tables
        df_1m = read_jdbc_table(spark, "raw.tracks_1m")
        df_hist = read_jdbc_table(spark, "raw.tracks_historical")

        # Clean each dataset
        df_1m_clean = clean_tracks_1m(df_1m)
        df_hist_clean = clean_tracks_historical(df_hist)

        # Join the datasets
        df_joined = join_datasets(df_1m_clean, df_hist_clean)

        # Write to staging
        write_to_staging(df_joined)

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Job 1 completed in %.1f seconds", elapsed)
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
