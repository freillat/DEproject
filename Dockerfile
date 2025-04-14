FROM apache/airflow:2.8.1

USER root
RUN apt-get update && apt-get install -y gcc

USER airflow

# Install required Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and resources into the container
COPY dags/ /opt/airflow/dags/
COPY gcp-service-account.json /opt/airflow/gcp-service-account.json

# Download GCS connector
RUN curl -L -o /opt/airflow/dags/resources/gcs-connector-hadoop3-latest.jar \
    https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Download BigQuery connector
ARG BQ_CONNECTOR_VERSION=0.36.0
RUN curl -L -o /opt/airflow/dags/resources/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar \
    https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/${BQ_CONNECTOR_VERSION}/spark-bigquery-with-dependencies_2.12-${BQ_CONNECTOR_VERSION}.jar

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-service-account.json