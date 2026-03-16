"""
Unit tests for Spark job configurations.

Tests Spark job configurations and helper functions without
requiring a live Spark cluster (mocked SparkSession).
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock


class TestSparkJobConfig:
    """Tests for Spark job environment configuration."""

    @patch.dict(os.environ, {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "SPARK_POSTGRES_JDBC_URL": "jdbc:postgresql://testhost:5432/testdb",
    })
    def test_job1_jdbc_config(self):
        """Job 1 should read JDBC config from environment."""
        # Import after patching env
        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs")
        )
        import importlib
        import job1_clean_and_join as job1

        importlib.reload(job1)
        assert job1.JDBC_URL == "jdbc:postgresql://testhost:5432/testdb"
        assert job1.JDBC_PROPERTIES["user"] == "test_user"
        assert job1.JDBC_PROPERTIES["password"] == "test_pass"

    @patch.dict(os.environ, {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka-test:9092",
        "KAFKA_TOPIC": "test.topic",
    })
    def test_job2_kafka_config(self):
        """Job 2 should read Kafka config from environment."""
        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs")
        )
        import importlib
        import job2_structured_streaming as job2

        importlib.reload(job2)
        assert job2.KAFKA_BOOTSTRAP_SERVERS == "kafka-test:9092"
        assert job2.KAFKA_TOPIC == "test.topic"


class TestSparkJobConstants:
    """Tests for Spark job constants and defaults."""

    def test_checkpoint_location_set(self):
        """Job 2 should have a checkpoint location defined."""
        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs")
        )
        from job2_structured_streaming import CHECKPOINT_LOCATION

        assert CHECKPOINT_LOCATION is not None
        assert len(CHECKPOINT_LOCATION) > 0

    def test_event_schema_defined(self):
        """Job 2 should have a schema defined for Kafka events."""
        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..", "spark_jobs")
        )
        from job2_structured_streaming import EVENT_SCHEMA

        field_names = [f.name for f in EVENT_SCHEMA.fields]
        assert "event_id" in field_names
        assert "track_id" in field_names
        assert "event_type" in field_names
        assert "ts" in field_names
