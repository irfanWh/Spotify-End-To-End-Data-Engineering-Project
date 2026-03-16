"""
Spark Job 2 — Structured Streaming from Kafka.

Consumes play events from the Kafka topic spotify.play_events using
Spark Structured Streaming. Applies a 5-minute tumbling window on the
event timestamp and aggregates play, skip, and like counts per track
per window. Results are written to raw.play_events_windowed in PostgreSQL.
"""

import os
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# ──────────────── Configuration ────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("job2_structured_streaming")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "spotify.play_events")

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

CHECKPOINT_LOCATION = "/tmp/spark_checkpoints/play_events"

# Schema for the Kafka JSON messages
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_played", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("ts", StringType(), True),
])


def create_spark_session():
    """
    Create a SparkSession configured for Kafka + JDBC.

    Returns:
        SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder
        .appName("SpotifyJob2_StructuredStreaming")
        .getOrCreate()
    )


def write_to_postgres(batch_df, batch_id):
    """
    Write a micro-batch of windowed aggregations to PostgreSQL.

    This function is called by foreachBatch on each micro-batch
    of the streaming query.

    Args:
        batch_df: DataFrame containing windowed aggregation results.
        batch_id: Unique identifier for the micro-batch.

    Returns:
        None
    """
    if batch_df.count() == 0:
        logger.info("Batch %d: empty, skipping.", batch_id)
        return

    logger.info("Batch %d: writing %d rows to raw.play_events_windowed",
                batch_id, batch_df.count())

    batch_df.write.jdbc(
        url=JDBC_URL,
        table="raw.play_events_windowed",
        mode="append",
        properties=JDBC_PROPERTIES,
    )


def main():
    """
    Main entry point: starts Spark Structured Streaming from Kafka.

    Reads JSON play events, parses them, applies a 5-minute tumbling
    window, aggregates counts by event type, and writes to PostgreSQL
    using foreachBatch.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("Spark Job 2 — Structured Streaming — Starting")
    logger.info("=" * 60)

    spark = create_spark_session()

    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON messages
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", F.to_timestamp(F.col("ts")))
        .filter(F.col("event_ts").isNotNull())
    )

    # Apply 5-minute tumbling window and aggregate
    windowed = (
        parsed
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "5 minutes"),
            F.col("track_id"),
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "play", 1).otherwise(0))
            .alias("play_count"),
            F.sum(F.when(F.col("event_type") == "skip", 1).otherwise(0))
            .alias("skip_count"),
            F.sum(F.when(F.col("event_type") == "like", 1).otherwise(0))
            .alias("like_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("track_id"),
            F.col("play_count"),
            F.col("skip_count"),
            F.col("like_count"),
        )
    )

    # Write to PostgreSQL using foreachBatch
    query = (
        windowed.writeStream
        .outputMode("update")
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="1 minute")
        .start()
    )

    logger.info("Streaming query started. Waiting for termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
