.PHONY: up down build logs test lint dbt-run dbt-test dbt-docs ge-run \
       spark-job1 spark-job2 spark-job3 ml-train seed-kafka

# ──────────────── Docker ────────────────
up:
	docker-compose up -d

down:
	docker-compose down -v

build:
	docker-compose build --no-cache

logs:
	docker-compose logs -f

# ──────────────── Testing & Linting ────────────────
test:
	pytest tests/ -v

lint:
	ruff check .

# ──────────────── dbt ────────────────
dbt-run:
	docker exec spotify-airflow-scheduler dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

dbt-test:
	docker exec spotify-airflow-scheduler dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project

dbt-docs:
	docker exec spotify-airflow-scheduler bash -c "dbt docs generate --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project && dbt docs serve --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project --port 8085"

# ──────────────── Great Expectations ────────────────
ge-run:
	docker exec spotify-airflow-scheduler python /opt/airflow/quality/run_expectations.py

# ──────────────── Spark Jobs ────────────────
spark-job1:
	docker exec -u root spotify-spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar /opt/spark-jobs/job1_clean_and_join.py

spark-job2:
	docker exec -u root spotify-spark-master spark-submit --conf spark.jars.ivy=/tmp/.ivy --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar /opt/spark-jobs/job2_structured_streaming.py

spark-job3:
	docker exec -u root spotify-spark-master spark-submit --master spark://spark-master:7077 --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar /opt/spark-jobs/job3_feature_engineering.py

# ──────────────── ML ────────────────
ml-train:
	docker exec -u root spotify-spark-master bash -c "spark-submit --master spark://spark-master:7077 --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar /opt/ml/train_popularity_model.py && spark-submit --master spark://spark-master:7077 --jars /opt/spark-jobs/jars/postgresql-42.7.1.jar /opt/ml/train_genre_classifier.py"

# ──────────────── Kafka ────────────────
seed-kafka:
	docker-compose --profile producer up -d kafka-producer
