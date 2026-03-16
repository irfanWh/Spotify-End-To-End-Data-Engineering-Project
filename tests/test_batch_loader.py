"""
Unit tests for the batch loader module.

Tests CSV loading configuration, column cleaning, and database
connection logic without requiring a live PostgreSQL instance.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

import pandas as pd

# Add ingestion module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestion"))


class TestCleanColumnNames:
    """Tests for the clean_column_names function."""

    def test_lowercase_conversion(self):
        """Column names with uppercase letters should be lowercased."""
        from batch_loader import clean_column_names

        df = pd.DataFrame(columns=["Track Name", "Artist ID", "LOUDNESS"])
        result = clean_column_names(df)
        assert list(result.columns) == ["track_name", "artist_id", "loudness"]

    def test_special_characters_replaced(self):
        """Special characters in column names should become underscores."""
        from batch_loader import clean_column_names

        df = pd.DataFrame(columns=["danceability_%", "energy (norm)", "key#"])
        result = clean_column_names(df)
        for col in result.columns:
            assert all(c.isalnum() or c == "_" for c in col)

    def test_multiple_underscores_collapsed(self):
        """Multiple consecutive underscores should be collapsed to one."""
        from batch_loader import clean_column_names

        df = pd.DataFrame(columns=["track___name", "artist---id"])
        result = clean_column_names(df)
        assert "_" * 2 not in result.columns[0]
        assert "_" * 2 not in result.columns[1]

    def test_leading_trailing_underscores_stripped(self):
        """Leading and trailing underscores should be stripped."""
        from batch_loader import clean_column_names

        df = pd.DataFrame(columns=["_track_", "__artist__"])
        result = clean_column_names(df)
        for col in result.columns:
            assert not col.startswith("_")
            assert not col.endswith("_")


class TestDatasetConfig:
    """Tests for the dataset configuration."""

    def test_datasets_list_not_empty(self):
        """DATASETS should contain at least one entry."""
        from batch_loader import DATASETS

        assert len(DATASETS) > 0

    def test_each_dataset_has_required_keys(self):
        """Each dataset config must have file, table, encoding, chunk_size."""
        from batch_loader import DATASETS

        required_keys = {"file", "table", "encoding", "chunk_size"}
        for ds in DATASETS:
            assert required_keys.issubset(ds.keys()), f"Missing keys in {ds}"

    def test_table_names_have_schema(self):
        """Table names should include schema prefix (e.g. raw.table)."""
        from batch_loader import DATASETS

        for ds in DATASETS:
            assert "." in ds["table"], f"Table {ds['table']} missing schema prefix"

    def test_chunk_size_is_positive(self):
        """Chunk sizes should be positive integers."""
        from batch_loader import DATASETS

        for ds in DATASETS:
            assert ds["chunk_size"] > 0


class TestDatabaseConfig:
    """Tests for database connection configuration."""

    @patch.dict(os.environ, {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_pass",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5433",
        "POSTGRES_DB": "test_db",
    })
    def test_database_url_from_env(self):
        """DATABASE_URL should use environment variables."""
        # Re-import to pick up patched env
        import importlib
        import batch_loader

        importlib.reload(batch_loader)
        assert "test_user" in batch_loader.DATABASE_URL
        assert "test_host" in batch_loader.DATABASE_URL
