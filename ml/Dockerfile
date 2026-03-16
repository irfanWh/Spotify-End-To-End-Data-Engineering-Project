FROM public.ecr.aws/bitnami/spark:3.5

USER root

# Download PostgreSQL JDBC driver
RUN mkdir -p /opt/spark-jobs/jars && \
    curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
         -o /opt/spark-jobs/jars/postgresql-42.7.1.jar

# Install ML and tracking dependencies
RUN pip install --no-cache-dir \
    mlflow==2.11.3 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4

USER 1001
