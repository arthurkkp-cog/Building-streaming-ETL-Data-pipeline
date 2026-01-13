#!/bin/bash

# Smart Meter Data Processing - Spark Submit Script
# This script submits the smart meter streaming application to the Spark cluster

echo "=== Smart Meter Spark Processing Pipeline ==="
echo "Copying smart meter processing application to Spark container..."

# Copy the smart meter processing script to the Spark container
docker cp ../spark_app/smart_meter_processing.py spark_master:/opt/bitnami/spark/

echo "Submitting Spark application..."

# Submit the Spark application with all required JAR dependencies
docker exec spark_master /opt/bitnami/spark/bin/spark-submit \
    --master local[2] \
    --jars /opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar,/opt/bitnami/spark/jars/aws-java-sdk-s3-1.11.375.jar,/opt/bitnami/spark/jars/aws-java-sdk-core-1.11.375.jar,/opt/bitnami/spark/jars/kafka-clients-7.7.0-ce.jar,/opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.3.0.jar,/opt/bitnami/spark/jars/spark-streaming_2.12-3.3.0.jar,/opt/bitnami/spark/jars/commons-pool2-2.11.0.jar,/opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.3.0.jar \
    /opt/bitnami/spark/smart_meter_processing.py

echo "=== Spark job completed ==="
