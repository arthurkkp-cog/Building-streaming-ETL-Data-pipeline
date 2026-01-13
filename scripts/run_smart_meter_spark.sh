#!/bin/bash

# Smart Meter Data Processing - Spark Submit Script
# This script submits the smart meter streaming application to the Spark cluster

echo "=== Smart Meter Spark Processing Pipeline ==="
echo "Copying smart meter processing application to Spark container..."

# Copy the smart meter processing script to the Spark container
docker cp ../spark_app/smart_meter_processing.py spark_master:/opt/spark/

echo "Submitting Spark application..."

# Set environment variables for Minio credentials
export MINIO_ACCESS_KEY=MINIOAIRFLOW01
export MINIO_SECRET_KEY=AIRFLOW123

# Submit the Spark application with Kafka packages
docker exec -e MINIO_ACCESS_KEY=MINIOAIRFLOW01 -e MINIO_SECRET_KEY=AIRFLOW123 spark_master /opt/spark/bin/spark-submit \
    --master local[2] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/smart_meter_processing.py

echo "=== Spark job completed ==="
