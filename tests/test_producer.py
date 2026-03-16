"""
Unit tests for the Kafka producer module.

Tests event generation, track ID loading, and message format
without requiring a live Kafka cluster.
"""

import os
import sys
import json
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Add producer module to path
sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "data_sources", "kafka_producer"),
)


class TestCreatePlayEvent:
    """Tests for the create_play_event function."""

    def test_event_has_all_required_fields(self):
        """Generated event should have all required JSON fields."""
        from producer import create_play_event

        track_ids = ["track_001", "track_002", "track_003"]
        event = create_play_event(track_ids)

        required_fields = [
            "event_id", "user_id", "track_id", "event_type",
            "duration_played", "device_type", "country", "ts",
        ]
        for field in required_fields:
            assert field in event, f"Missing field: {field}"

    def test_event_track_id_from_list(self):
        """Track ID should come from the provided list."""
        from producer import create_play_event

        track_ids = ["abc123", "def456"]
        event = create_play_event(track_ids)
        assert event["track_id"] in track_ids

    def test_event_type_is_valid(self):
        """Event type should be one of play, skip, like, share."""
        from producer import create_play_event

        track_ids = ["track_001"]
        for _ in range(50):
            event = create_play_event(track_ids)
            assert event["event_type"] in ["play", "skip", "like", "share"]

    def test_user_id_format(self):
        """User ID should match format user_XXXX."""
        from producer import create_play_event

        event = create_play_event(["track_001"])
        assert event["user_id"].startswith("user_")
        assert len(event["user_id"]) == 9  # "user_" + 4 digits

    def test_duration_played_is_positive(self):
        """Duration played should be a positive integer."""
        from producer import create_play_event

        event = create_play_event(["track_001"])
        assert isinstance(event["duration_played"], int)
        assert event["duration_played"] > 0

    def test_country_is_iso_code(self):
        """Country should be a 2-letter ISO code."""
        from producer import create_play_event

        event = create_play_event(["track_001"])
        assert len(event["country"]) == 2
        assert event["country"].isupper()

    def test_timestamp_is_iso_format(self):
        """Timestamp should be valid ISO 8601 format."""
        from producer import create_play_event

        event = create_play_event(["track_001"])
        # Should not throw
        datetime.fromisoformat(event["ts"].replace("Z", "+00:00"))

    def test_event_is_json_serializable(self):
        """Generated event should be JSON serializable."""
        from producer import create_play_event

        event = create_play_event(["track_001"])
        json_str = json.dumps(event)
        assert isinstance(json_str, str)


class TestEventTypes:
    """Tests for event type configuration."""

    def test_event_types_defined(self):
        """EVENT_TYPES should be defined and non-empty."""
        from producer import EVENT_TYPES

        assert len(EVENT_TYPES) > 0

    def test_device_types_defined(self):
        """DEVICE_TYPES should be defined and non-empty."""
        from producer import DEVICE_TYPES

        assert len(DEVICE_TYPES) > 0
