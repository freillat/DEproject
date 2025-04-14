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

ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-service-account.json