"""
Kafka Producer — Simulates real-time Spotify play events.

Generates synthetic play events using real track IDs from the
tracks_features.csv dataset and publishes them as JSON messages
to the Kafka topic spotify.play_events.
"""

import csv
import json
import os
import random
import time
import uuid
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ──────────────── Configuration ────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "spotify.play_events")
DATA_FILE = os.environ.get("DATA_FILE", "/opt/data/raw/tracks_features.csv")

EVENT_TYPES = ["play", "skip", "like", "share"]
EVENT_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

DEVICE_TYPES = ["mobile", "desktop", "tablet", "smart_speaker"]
DEVICE_WEIGHTS = [0.50, 0.30, 0.12, 0.08]

COUNTRIES = [
    "US", "GB", "DE", "FR", "BR", "IN", "JP", "MX", "KR", "AU",
    "CA", "ES", "IT", "NL", "SE", "AR", "CL", "CO", "PL", "TR",
]

SLEEP_BETWEEN_EVENTS = 0.5  # seconds


def load_track_ids(file_path):
    """
    Load track IDs from the tracks_features CSV file.

    Args:
        file_path: Absolute path to tracks_features.csv.

    Returns:
        list[str]: List of track ID strings.
    """
    track_ids = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                tid = row.get("id", "").strip()
                if tid:
                    track_ids.append(tid)
    except FileNotFoundError:
        logger.error("Data file not found: %s", file_path)
        return []

    logger.info("Loaded %d track IDs from %s", len(track_ids), file_path)
    return track_ids


def create_play_event(track_ids):
    """
    Generate a single synthetic play event.

    Args:
        track_ids: List of valid track IDs to sample from.

    Returns:
        dict: Play event with all required fields.
    """
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]

    # Skips have shorter duration
    if event_type == "skip":
        duration_played = random.randint(5, 30)
    else:
        duration_played = random.randint(30, 300)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "track_id": random.choice(track_ids),
        "event_type": event_type,
        "duration_played": duration_played,
        "device_type": random.choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS, k=1)[0],
        "country": random.choice(COUNTRIES),
        "ts": datetime.now(timezone.utc).isoformat(),
    }


def wait_for_kafka(bootstrap_servers, max_retries=30, delay=3):
    """
    Wait for Kafka brokers to become available.

    Args:
        bootstrap_servers: Kafka bootstrap server address.
        max_retries: Maximum number of connection attempts.
        delay: Seconds to wait between retries.

    Returns:
        KafkaProducer: Connected producer instance.

    Raises:
        ConnectionError: If Kafka is not available after max_retries.
    """
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
            )
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Waiting for Kafka... attempt %d/%d", attempt, max_retries
            )
            time.sleep(delay)

    raise ConnectionError("Kafka not available after %d retries." % max_retries)


def main():
    """
    Main loop: continuously produces play events to Kafka.

    Loads track IDs from disk, connects to Kafka, and emits
    random play events in an infinite loop with a configurable
    sleep interval.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("Spotify Kafka Producer — Starting")
    logger.info("=" * 60)

    track_ids = load_track_ids(DATA_FILE)
    if not track_ids:
        logger.error("No track IDs loaded. Exiting.")
        return

    producer = wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS)

    events_sent = 0
    try:
        while True:
            event = create_play_event(track_ids)
            producer.send(
                KAFKA_TOPIC,
                key=event["track_id"],
                value=event,
            )
            events_sent += 1

            if events_sent % 100 == 0:
                logger.info("Events sent: %d (latest: %s)", events_sent, event["ts"])

            time.sleep(SLEEP_BETWEEN_EVENTS)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user. Total events sent: %d", events_sent)
    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed.")


if __name__ == "__main__":
    main()
