# Use the official Apache Airflow image as base
FROM apache/airflow:latest

# Switch to root user to install system dependencies
USER root

# Update package lists and install system dependencies safely
RUN apt-get update --allow-releaseinfo-change && apt-get install -y curl

# Switch to 'airflow' user before installing PySpark
USER airflow

# Install PySpark inside the Airflow container
RUN pip install --no-cache-dir pyspark
