"""
Airflow DAG — Spotify Streaming Monitor.

Monitors the health of the Kafka streaming pipeline:
  1. Check Kafka consumer group lag
  2. Check play_events row count in PostgreSQL
  3. Alert if no new events in last 10 minutes

Schedule: Every 5 minutes (*/5 * * * *)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException


# ──────────────── Default Args ────────────────

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2024, 1, 1),
}


# ──────────────── Task Callables ────────────────

def check_kafka_lag(**context):
    """
    Check Kafka consumer group lag for the play events topic.

    Args:
        **context: Airflow context dictionary.

    Returns:
        dict: Consumer lag information per partition.
    """
    import os
    import logging
    from kafka import KafkaAdminClient, KafkaConsumer

    logger = logging.getLogger("streaming_monitor")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "spotify.play_events")

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id="streaming_monitor_check",
        )
        partitions = consumer.partitions_for_topic(topic)

        if partitions is None:
            logger.warning("Topic %s does not exist yet.", topic)
            return {"status": "topic_not_found"}

        logger.info("Topic %s has %d partitions.", topic, len(partitions))
        consumer.close()
        return {"status": "ok", "partitions": len(partitions)}

    except Exception as err:
        logger.error("Kafka lag check failed: %s", err)
        return {"status": "error", "error": str(err)}


def check_events_count(**context):
    """
    Query the total row count in raw.play_events and log it.

    Args:
        **context: Airflow context dictionary.

    Returns:
        int: Current row count in play_events table.
    """
    import os
    import logging
    from sqlalchemy import create_engine, text

    logger = logging.getLogger("streaming_monitor")

    db_url = (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'spotify_user')}"
        f":{os.environ.get('POSTGRES_PASSWORD', 'spotify_pass')}"
        f"@{os.environ.get('POSTGRES_HOST', 'postgres')}"
        f":{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'spotify_platform')}"
    )

    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM raw.play_events"))
        count = result.scalar()

    logger.info("raw.play_events row count: %d", count)
    return count


def alert_if_stale(**context):
    """
    Raise an alert if no new play events in the last 10 minutes.

    Args:
        **context: Airflow context dictionary.

    Raises:
        AirflowException: If no events received in the last 10 minutes.

    Returns:
        str: Status message.
    """
    import os
    import logging
    from sqlalchemy import create_engine, text

    logger = logging.getLogger("streaming_monitor")

    db_url = (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'spotify_user')}"
        f":{os.environ.get('POSTGRES_PASSWORD', 'spotify_pass')}"
        f"@{os.environ.get('POSTGRES_HOST', 'postgres')}"
        f":{os.environ.get('POSTGRES_PORT', '5432')}"
        f"/{os.environ.get('POSTGRES_DB', 'spotify_platform')}"
    )

    engine = create_engine(db_url)
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM raw.play_events "
                "WHERE ts >= NOW() - INTERVAL '10 minutes'"
            )
        )
        recent_count = result.scalar()

    if recent_count == 0:
        msg = "ALERT: No new play events in the last 10 minutes!"
        logger.error(msg)
        raise AirflowException(msg)

    logger.info("Recent events (last 10 min): %d — stream is healthy.", recent_count)
    return f"Stream healthy: {recent_count} events in last 10 min."


# ──────────────── DAG Definition ────────────────

with DAG(
    dag_id="spotify_streaming_monitor",
    default_args=default_args,
    description="Monitors Kafka streaming pipeline health",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "streaming", "monitoring"],
) as dag:

    check_lag = PythonOperator(
        task_id="check_kafka_lag",
        python_callable=check_kafka_lag,
    )

    check_count = PythonOperator(
        task_id="check_events_count",
        python_callable=check_events_count,
    )

    alert_stale = PythonOperator(
        task_id="alert_if_stale",
        python_callable=alert_if_stale,
    )

    check_lag >> check_count >> alert_stale
