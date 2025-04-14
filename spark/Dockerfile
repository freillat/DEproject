FROM openjdk:11-jre-slim

# Set environment variables
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
ENV SCALA_VERSION=2.12
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Install necessary packages
RUN apt-get update && apt-get install -y wget curl python3 python3-pip --no-install-recommends && rm -rf /var/lib/apt/lists/*

# Download and install Spark with Hadoop
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -P /tmp/ && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install gcloud CLI
RUN curl https://sdk.cloud.google.com | bash -s -- --install-dir=/opt/google-cloud-sdk --quiet

# Add gcloud to PATH
ENV PATH=$PATH:/opt/google-cloud-sdk/bin

# Install gcs-connector for Hadoop (for Spark to interact with GCS)
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/

# Set up working directory
WORKDIR /app

# Copy your application files
COPY /app/stock_analytics.py /app/

# Command to run Spark (you might override this in your docker-compose)
# CMD ["spark-submit", "your_spark_application.py"]