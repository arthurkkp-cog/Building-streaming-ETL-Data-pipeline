import logging
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as spark_sum, count, max as spark_max,
    min as spark_min, stddev, when, lit, hour, dayofweek, to_timestamp,
    current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, TimestampType, DoubleType
)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("smart_meter_spark_processing")


def initialize_spark_session(app_name, access_key, secret_key):
    """
    Initialize the Spark Session with S3 (Minio) configurations.
    
    :param app_name: Name of the spark application.
    :param access_key: Access key for S3/Minio.
    :param secret_key: Secret key for S3/Minio.
    :return: Spark session object or None if there's an error.
    """
    try:
        spark = SparkSession \
            .builder \
            .appName(app_name) \
            .config("spark.hadoop.fs.s3a.access.key", access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session initialized successfully')
        return spark

    except Exception as e:
        logger.error(f"Spark session initialization failed. Error: {e}")
        return None


def get_smart_meter_schema():
    """
    Define the schema for smart meter data enriched with weather information.
    
    Schema matches the output from smart_meter_streaming.py DAG.
    """
    return StructType([
        StructField("meter_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("energy_kwh", FloatType(), False),
        StructField("tariff_type", StringType(), True),
        StructField("acorn_group", StringType(), True),
        StructField("acorn_category", StringType(), True),
        StructField("temperature", FloatType(), True),
        StructField("humidity", FloatType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("cloud_cover", FloatType(), True),
        StructField("weather_summary", StringType(), True),
        StructField("precip_intensity", FloatType(), True),
        StructField("pressure", FloatType(), True),
        StructField("ingestion_time", StringType(), True)
    ])


def get_streaming_dataframe(spark, brokers, topic):
    """
    Get a streaming dataframe from Kafka smart meter topic.
    
    :param spark: Initialized Spark session.
    :param brokers: Comma-separated list of Kafka brokers.
    :param topic: Kafka topic to subscribe to.
    :return: Dataframe object or None if there's an error.
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", brokers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("mode", "PERMISSIVE") \
            .load()
        logger.info("Streaming dataframe fetched successfully from Kafka")
        return df

    except Exception as e:
        logger.warning(f"Failed to fetch streaming dataframe. Error: {e}")
        return None


def transform_smart_meter_data(df):
    """
    Transform raw Kafka messages into structured smart meter data.
    
    Applies schema parsing and adds derived columns for analytics.
    
    :param df: Raw dataframe from Kafka.
    :return: Transformed dataframe with structured columns.
    """
    schema = get_smart_meter_schema()
    
    transformed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("reading_timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("hour_of_day", hour(col("reading_timestamp"))) \
        .withColumn("day_of_week", dayofweek(col("reading_timestamp"))) \
        .withColumn("is_peak_hour", 
            when((col("hour_of_day") >= 17) & (col("hour_of_day") <= 21), lit(True))
            .otherwise(lit(False))) \
        .withColumn("is_weekend",
            when((col("day_of_week") == 1) | (col("day_of_week") == 7), lit(True))
            .otherwise(lit(False))) \
        .withColumn("temperature_band",
            when(col("temperature") < 5, lit("Cold"))
            .when((col("temperature") >= 5) & (col("temperature") < 15), lit("Mild"))
            .when((col("temperature") >= 15) & (col("temperature") < 25), lit("Warm"))
            .otherwise(lit("Hot"))) \
        .withColumn("consumption_level",
            when(col("energy_kwh") < 0.2, lit("Low"))
            .when((col("energy_kwh") >= 0.2) & (col("energy_kwh") < 0.5), lit("Medium"))
            .when((col("energy_kwh") >= 0.5) & (col("energy_kwh") < 1.0), lit("High"))
            .otherwise(lit("Very High"))) \
        .withColumn("processing_time", current_timestamp())
    
    return transformed_df


def calculate_aggregations(df):
    """
    Calculate real-time aggregations for demand monitoring.
    
    Creates windowed aggregations by ACORN group and tariff type
    for utilities company demand forecasting.
    
    :param df: Transformed smart meter dataframe.
    :return: Aggregated dataframe with consumption metrics.
    """
    aggregated_df = df \
        .withWatermark("reading_timestamp", "1 minute") \
        .groupBy(
            window(col("reading_timestamp"), "5 minutes", "1 minute"),
            col("acorn_category"),
            col("tariff_type"),
            col("temperature_band")
        ) \
        .agg(
            spark_sum("energy_kwh").alias("total_energy_kwh"),
            avg("energy_kwh").alias("avg_energy_kwh"),
            spark_max("energy_kwh").alias("max_energy_kwh"),
            spark_min("energy_kwh").alias("min_energy_kwh"),
            stddev("energy_kwh").alias("stddev_energy_kwh"),
            count("*").alias("reading_count"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("acorn_category"),
            col("tariff_type"),
            col("temperature_band"),
            col("total_energy_kwh"),
            col("avg_energy_kwh"),
            col("max_energy_kwh"),
            col("min_energy_kwh"),
            col("stddev_energy_kwh"),
            col("reading_count"),
            col("avg_temperature"),
            col("avg_humidity")
        )
    
    return aggregated_df


def detect_anomalies(df):
    """
    Detect anomalous energy consumption patterns.
    
    Flags readings that are significantly higher than typical consumption
    for potential fraud detection or equipment issues.
    
    :param df: Transformed smart meter dataframe.
    :return: Dataframe with anomaly flags.
    """
    anomaly_df = df \
        .withColumn("is_anomaly",
            when(
                (col("energy_kwh") > 2.0) |
                ((col("hour_of_day") >= 2) & (col("hour_of_day") <= 5) & (col("energy_kwh") > 0.5)) |
                ((col("is_weekend") == True) & (col("hour_of_day") >= 9) & (col("hour_of_day") <= 17) & (col("energy_kwh") > 1.5)),
                lit(True)
            ).otherwise(lit(False))) \
        .withColumn("anomaly_reason",
            when(col("energy_kwh") > 2.0, lit("Extremely high consumption"))
            .when((col("hour_of_day") >= 2) & (col("hour_of_day") <= 5) & (col("energy_kwh") > 0.5), 
                  lit("High nighttime consumption"))
            .when((col("is_weekend") == True) & (col("hour_of_day") >= 9) & (col("hour_of_day") <= 17) & (col("energy_kwh") > 1.5),
                  lit("High weekend daytime consumption"))
            .otherwise(lit(None)))
    
    return anomaly_df


def initiate_streaming_to_bucket(df, path, checkpoint_location, output_mode="append"):
    """
    Start streaming transformed data to Minio S3 bucket in parquet format.
    
    :param df: Transformed dataframe to write.
    :param path: S3 bucket path for output.
    :param checkpoint_location: Checkpoint location for fault tolerance.
    :param output_mode: Output mode (append, complete, update).
    :return: None
    """
    logger.info(f"Initiating streaming to Minio S3: {path}")
    stream_query = (df.writeStream
                    .format("parquet")
                    .outputMode(output_mode)
                    .trigger(processingTime='5 seconds')
                    .option("path", path)
                    .option("checkpointLocation", checkpoint_location)
                    .start())
    return stream_query


def initiate_aggregation_streaming(df, path, checkpoint_location):
    """
    Start streaming aggregated metrics to Minio S3 bucket.
    
    Uses 'update' mode for windowed aggregations.
    
    :param df: Aggregated dataframe to write.
    :param path: S3 bucket path for output.
    :param checkpoint_location: Checkpoint location for fault tolerance.
    :return: StreamingQuery object.
    """
    logger.info(f"Initiating aggregation streaming to Minio S3: {path}")
    stream_query = (df.writeStream
                    .format("parquet")
                    .outputMode("append")
                    .trigger(processingTime='10 seconds')
                    .option("path", path)
                    .option("checkpointLocation", checkpoint_location)
                    .start())
    return stream_query


def main():
    """
    Main entry point for smart meter data processing pipeline.
    
    Orchestrates:
    1. Reading from Kafka smart meter topic
    2. Transforming and enriching data
    3. Detecting anomalies
    4. Calculating aggregations for demand forecasting
    5. Writing processed data to Minio S3
    """
    app_name = "SmartMeterStreamingToMinioS3"
    access_key = os.environ.get("MINIO_ACCESS_KEY", "ENTER_YOUR_MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "ENTER_YOUR_MINIO_SECRET_KEY")
    brokers = os.environ.get("KAFKA_BROKERS", "broker-1:9092,broker-2:9093,broker-3:9094")
    topic = os.environ.get("KAFKA_TOPIC", "smart-meter-topic")
    
    raw_data_path = "s3a://smart-meter-data/raw/"
    aggregated_path = "s3a://smart-meter-data/aggregated/"
    anomalies_path = "s3a://smart-meter-data/anomalies/"
    
    raw_checkpoint = "s3a://smart-meter-data/checkpoints/raw/"
    aggregated_checkpoint = "s3a://smart-meter-data/checkpoints/aggregated/"
    anomalies_checkpoint = "s3a://smart-meter-data/checkpoints/anomalies/"

    logger.info("Starting Smart Meter Streaming Pipeline...")
    
    spark = initialize_spark_session(app_name, access_key, secret_key)
    if not spark:
        logger.error("Failed to initialize Spark session. Exiting.")
        sys.exit(1)
    
    raw_df = get_streaming_dataframe(spark, brokers, topic)
    if not raw_df:
        logger.error("Failed to get streaming dataframe. Exiting.")
        sys.exit(1)
    
    transformed_df = transform_smart_meter_data(raw_df)
    
    anomaly_df = detect_anomalies(transformed_df)
    anomalies_only = anomaly_df.filter(col("is_anomaly") == True)
    
    aggregated_df = calculate_aggregations(transformed_df)
    
    logger.info("Starting streaming queries...")
    
    raw_query = initiate_streaming_to_bucket(
        transformed_df, raw_data_path, raw_checkpoint
    )
    
    anomaly_query = initiate_streaming_to_bucket(
        anomalies_only, anomalies_path, anomalies_checkpoint
    )
    
    aggregation_query = initiate_aggregation_streaming(
        aggregated_df, aggregated_path, aggregated_checkpoint
    )
    
    logger.info("All streaming queries started. Awaiting termination...")
    
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
