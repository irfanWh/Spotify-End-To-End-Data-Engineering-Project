FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies for dbt and PostgreSQL
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    dbt-core==1.7.4 \
    dbt-postgres==1.7.4 \
    great-expectations==0.18.8 \
    kafka-python==2.0.2 \
    pandas==2.1.4
