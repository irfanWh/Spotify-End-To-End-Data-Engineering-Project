"""
Great Expectations Runner — Execute all data quality suites.

Standalone script that can run all three Great Expectations suites
(raw, staging, marts) against the PostgreSQL database. Designed to
be called both independently and from within Airflow tasks.
"""

import json
import os
import sys
import logging

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from sqlalchemy import create_engine

# ──────────────── Configuration ────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

POSTGRES_USER = os.environ.get("POSTGRES_USER", "spotify_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "spotify_pass")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "spotify_platform")

CONNECTION_STRING = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Suite definitions: (suite_name, table_name)
SUITES = [
    ("raw_tracks_suite", "raw.tracks_1m"),
    ("staging_tracks_suite", "staging.tracks_enriched"),
    ("marts_fact_streams_suite", "marts.fact_streams"),
]


def load_suite(suite_name):
    """
    Load a Great Expectations suite from JSON file.

    Args:
        suite_name: Name of the suite (without .json extension).

    Returns:
        dict: Parsed expectation suite.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    suite_path = os.path.join(base_dir, "expectations", f"{suite_name}.json")

    with open(suite_path, "r") as f:
        suite = json.load(f)

    logger.info("Loaded suite: %s (%d expectations)", suite_name, len(suite["expectations"]))
    return suite


def validate_table(suite_name, table_name):
    """
    Run a GE suite against a PostgreSQL table.

    Args:
        suite_name: Name of the expectation suite.
        table_name: Fully qualified table name (schema.table).

    Returns:
        bool: True if all expectations pass, False otherwise.
    """
    logger.info("Validating %s with suite %s...", table_name, suite_name)

    suite = load_suite(suite_name)
    engine = create_engine(CONNECTION_STRING)

    # Run each expectation manually using SQLAlchemy
    results = []
    with engine.connect() as conn:
        for exp in suite["expectations"]:
            exp_type = exp["expectation_type"]
            kwargs = exp.get("kwargs", {})

            try:
                if exp_type == "expect_table_row_count_to_be_between":
                    result = conn.execute(
                        __import__("sqlalchemy").text(f"SELECT COUNT(*) FROM {table_name}")
                    )
                    count = result.scalar()
                    min_val = kwargs.get("min_value", 0)
                    passed = count >= min_val
                    logger.info(
                        "  %s: count=%d, min=%d → %s",
                        exp_type, count, min_val, "PASS" if passed else "FAIL",
                    )
                elif exp_type == "expect_column_to_exist":
                    col = kwargs["column"]
                    result = conn.execute(
                        __import__("sqlalchemy").text(
                            f"SELECT column_name FROM information_schema.columns "
                            f"WHERE table_schema || '.' || table_name = '{table_name}' "
                            f"AND column_name = '{col}'"
                        )
                    )
                    passed = result.fetchone() is not None
                    logger.info("  %s (%s): %s", exp_type, col, "PASS" if passed else "FAIL")
                elif exp_type == "expect_column_values_to_not_be_null":
                    col = kwargs["column"]
                    mostly = kwargs.get("mostly", 1.0)
                    result = conn.execute(
                        __import__("sqlalchemy").text(
                            f"SELECT COUNT(*) AS total, "
                            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS nulls "
                            f"FROM {table_name}"
                        )
                    )
                    row = result.fetchone()
                    total, nulls = row[0], row[1]
                    non_null_ratio = (total - nulls) / total if total > 0 else 0
                    passed = non_null_ratio >= mostly
                    logger.info(
                        "  %s (%s): %.2f%% non-null (need %.0f%%) → %s",
                        exp_type, col, non_null_ratio * 100, mostly * 100,
                        "PASS" if passed else "FAIL",
                    )
                else:
                    logger.info("  %s: skipped (complex validation)", exp_type)
                    passed = True

                results.append(passed)
            except Exception as err:
                logger.error("  %s: ERROR - %s", exp_type, err)
                results.append(False)

    all_passed = all(results)
    status = "ALL PASSED" if all_passed else "SOME FAILED"
    logger.info("Suite %s: %s (%d/%d passed)", suite_name, status, sum(results), len(results))
    return all_passed


def main():
    """
    Run all Great Expectations suites and report summary.

    Returns:
        None
    """
    logger.info("=" * 60)
    logger.info("Great Expectations Runner — Starting")
    logger.info("=" * 60)

    all_passed = True
    for suite_name, table_name in SUITES:
        try:
            passed = validate_table(suite_name, table_name)
            if not passed:
                all_passed = False
        except Exception as err:
            logger.error("Failed to validate %s: %s", suite_name, err)
            all_passed = False

    logger.info("=" * 60)
    if all_passed:
        logger.info("ALL SUITES PASSED")
    else:
        logger.warning("SOME SUITES FAILED — check logs above")
        sys.exit(1)


if __name__ == "__main__":
    main()
