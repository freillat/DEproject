FROM bitnami/spark:3.5.0

USER root

# Install Google Cloud connector jars
ENV SPARK_HOME=/opt/bitnami/spark
ENV HADOOP_VERSION=3.3.1

RUN apt-get update && apt-get install -y curl && apt-get clean

# Download GCS connector and BigQuery connector
RUN mkdir -p $SPARK_HOME/jars && \
    curl -o $SPARK_HOME/jars/gcs-connector-hadoop3-latest.jar https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar && \
    curl -L -o $SPARK_HOME/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar

# Install Python packages (optional)
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /app
COPY app/ /app