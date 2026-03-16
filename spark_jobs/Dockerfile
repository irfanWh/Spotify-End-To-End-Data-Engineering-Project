FROM bitnami/spark:3.5

USER root

# Download PostgreSQL JDBC driver for Spark
RUN mkdir -p /opt/spark-jobs/jars && \
    curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
         -o /opt/spark-jobs/jars/postgresql-42.7.1.jar

# Install additional Python packages
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    kafka-python==2.0.2

USER 1001
