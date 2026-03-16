"""
Airflow DAG — Spotify Batch Pipeline.

Orchestrates the full batch data pipeline:
  1. Load raw CSVs → PostgreSQL
  2. Run Great Expectations on raw data
  3. Spark Job 1: clean and join tracks
  4. Run Great Expectations on staging data
  5. dbt: staging → intermediate → marts models
  6. dbt tests
  7. Run Great Expectations on marts data
  8. Spark Job 3: feature engineering
  9. ML batch scoring
  10. Notify success

Schedule: @hourly
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator


# ──────────────── Default Args ────────────────

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


# ──────────────── Callback Helpers ────────────────

def run_batch_loader(**context):
    """
    Execute the batch loader to ingest CSVs into raw schema.

    Args:
        **context: Airflow context dictionary.

    Returns:
        str: Summary message with row counts.
    """
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "batch_loader", "/opt/airflow/ingestion/batch_loader.py"
    )
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)
    loader.main()
    return "Batch load complete."


def log_pipeline_summary(**context):
    """
    Log a summary of the pipeline run.

    Args:
        **context: Airflow context dictionary.

    Returns:
        str: Pipeline completion summary.
    """
    import logging

    logger = logging.getLogger("spotify_pipeline")
    dag_run = context.get("dag_run")
    execution_date = context.get("execution_date")

    logger.info("=" * 60)
    logger.info("PIPELINE RUN SUMMARY")
    logger.info("  DAG: %s", dag_run.dag_id if dag_run else "N/A")
    logger.info("  Execution date: %s", execution_date)
    logger.info("  Status: SUCCESS")
    logger.info("=" * 60)
    return "Pipeline completed successfully."


# ──────────────── DAG Definition ────────────────

with DAG(
    dag_id="spotify_batch_pipeline",
    default_args=default_args,
    description="End-to-end Spotify batch data pipeline",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "batch", "production"],
) as dag:

    # Task 1: Load raw CSV data
    t1_load_raw = PythonOperator(
        task_id="t1_load_raw",
        python_callable=run_batch_loader,
    )

    # Task 2: Great Expectations on raw data
    t2_ge_raw = BashOperator(
        task_id="t2_ge_raw",
        bash_command=(
            "cd /opt/airflow/quality && "
            "python run_expectations.py"
        ),
    )

    # Task 3: Spark Job 1 — Clean and Join
    t3_spark_job1 = BashOperator(
        task_id="t3_spark_job1",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--jars /opt/spark-jobs/jars/postgresql-42.7.1.jar "
            "/opt/airflow/spark_jobs/job1_clean_and_join.py"
        ),
    )

    # Task 4: Great Expectations on staging data
    t4_ge_staging = BashOperator(
        task_id="t4_ge_staging",
        bash_command=(
            "cd /opt/airflow/quality && "
            "python run_expectations.py"
        ),
    )

    # Task 5: dbt run — staging models
    t5_dbt_staging = BashOperator(
        task_id="t5_dbt_staging",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project "
            "--select staging"
        ),
    )

    # Task 6: dbt run — intermediate models
    t6_dbt_intermediate = BashOperator(
        task_id="t6_dbt_intermediate",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project "
            "--select intermediate"
        ),
    )

    # Task 7: dbt run — marts models
    t7_dbt_marts = BashOperator(
        task_id="t7_dbt_marts",
        bash_command=(
            "dbt run "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project "
            "--select marts"
        ),
    )

    # Task 8: dbt test — all models
    t8_dbt_test = BashOperator(
        task_id="t8_dbt_test",
        bash_command=(
            "dbt test "
            "--project-dir /opt/airflow/dbt_project "
            "--profiles-dir /opt/airflow/dbt_project"
        ),
    )

    # Task 9: Great Expectations on marts data
    t9_ge_marts = BashOperator(
        task_id="t9_ge_marts",
        bash_command=(
            "cd /opt/airflow/quality && "
            "python run_expectations.py"
        ),
    )

    # Task 10: Spark Job 3 — Feature Engineering
    t10_spark_job3 = BashOperator(
        task_id="t10_spark_job3",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--jars /opt/spark-jobs/jars/postgresql-42.7.1.jar "
            "/opt/airflow/spark_jobs/job3_feature_engineering.py"
        ),
    )

    # Task 11: ML Batch Prediction
    t11_ml_predict = BashOperator(
        task_id="t11_ml_predict",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--jars /opt/spark-jobs/jars/postgresql-42.7.1.jar "
            "/opt/airflow/ml/predict_batch.py"
        ),
    )

    # Task 12: Notify success
    t12_notify_success = PythonOperator(
        task_id="t12_notify_success",
        python_callable=log_pipeline_summary,
    )

    # ──────────────── Dependencies ────────────────
    (
        t1_load_raw
        >> t2_ge_raw
        >> t3_spark_job1
        >> t4_ge_staging
        >> t5_dbt_staging
        >> t6_dbt_intermediate
        >> t7_dbt_marts
        >> t8_dbt_test
        >> t9_ge_marts
        >> t10_spark_job3
        >> t11_ml_predict
        >> t12_notify_success
    )
