"""
Smart Meters London Data to BigQuery Pipeline

This script uses PySpark to:
1. Read smart meter energy consumption data from CSV files
2. Transform and optimize data for BigQuery (schema flattening, type conversions, partitioning)
3. Upload directly to BigQuery via API

Dataset: https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london
"""

import logging
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, to_timestamp, hour, dayofweek, month, year,
    when, lit, avg, sum as spark_sum, count, max as spark_max,
    min as spark_min, stddev, coalesce, trim, upper,
    date_format, concat_ws, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, DoubleType,
    TimestampType, DateType, IntegerType
)
from google.cloud import bigquery
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s'
)
logger = logging.getLogger("smart_meters_to_bigquery")


def get_bigquery_client(credentials_path: str) -> bigquery.Client:
    """
    Initialize BigQuery client with service account credentials.
    
    :param credentials_path: Path to service account JSON key file
    :return: BigQuery client instance
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    logger.info(f"BigQuery client initialized for project: {credentials.project_id}")
    return client


def initialize_spark_session(app_name: str) -> SparkSession:
    """
    Initialize Spark session for data processing.
    
    :param app_name: Name of the Spark application
    :return: SparkSession instance
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session initialized: {app_name}")
    return spark


def get_energy_schema() -> StructType:
    """
    Define schema for half-hourly energy consumption data.
    """
    return StructType([
        StructField("LCLid", StringType(), False),
        StructField("tstp", StringType(), False),
        StructField("energy_kwh_hh", StringType(), True)
    ])


def get_household_schema() -> StructType:
    """
    Define schema for household information data.
    """
    return StructType([
        StructField("LCLid", StringType(), False),
        StructField("stdorToU", StringType(), True),
        StructField("Acorn", StringType(), True),
        StructField("Acorn_grouped", StringType(), True),
        StructField("file", StringType(), True)
    ])


def get_weather_hourly_schema() -> StructType:
    """
    Define schema for hourly weather data.
    """
    return StructType([
        StructField("time", StringType(), False),
        StructField("summary", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("precipIntensity", StringType(), True),
        StructField("precipProbability", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("apparentTemperature", StringType(), True),
        StructField("dewPoint", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("pressure", StringType(), True),
        StructField("windSpeed", StringType(), True),
        StructField("windBearing", StringType(), True),
        StructField("cloudCover", StringType(), True),
        StructField("uvIndex", StringType(), True),
        StructField("visibility", StringType(), True)
    ])


def get_weather_daily_schema() -> StructType:
    """
    Define schema for daily weather data.
    """
    return StructType([
        StructField("time", StringType(), False),
        StructField("summary", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("sunriseTime", StringType(), True),
        StructField("sunsetTime", StringType(), True),
        StructField("moonPhase", StringType(), True),
        StructField("precipIntensity", StringType(), True),
        StructField("precipIntensityMax", StringType(), True),
        StructField("precipProbability", StringType(), True),
        StructField("precipType", StringType(), True),
        StructField("temperatureHigh", StringType(), True),
        StructField("temperatureHighTime", StringType(), True),
        StructField("temperatureLow", StringType(), True),
        StructField("temperatureLowTime", StringType(), True),
        StructField("apparentTemperatureHigh", StringType(), True),
        StructField("apparentTemperatureHighTime", StringType(), True),
        StructField("apparentTemperatureLow", StringType(), True),
        StructField("apparentTemperatureLowTime", StringType(), True),
        StructField("dewPoint", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("pressure", StringType(), True),
        StructField("windSpeed", StringType(), True),
        StructField("windGust", StringType(), True),
        StructField("windBearing", StringType(), True),
        StructField("cloudCover", StringType(), True),
        StructField("uvIndex", StringType(), True),
        StructField("visibility", StringType(), True),
        StructField("temperatureMin", StringType(), True),
        StructField("temperatureMax", StringType(), True)
    ])


def load_energy_data(spark: SparkSession, data_path: str):
    """
    Load half-hourly energy consumption data from CSV files.
    
    :param spark: SparkSession instance
    :param data_path: Path to data directory
    :return: DataFrame with energy consumption data
    """
    energy_files = os.path.join(data_path, "halfhourly_dataset*.csv")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(energy_files)
    
    energy_col_name = None
    for c in df.columns:
        if "energy" in c.lower():
            energy_col_name = c
            break
    
    if energy_col_name and energy_col_name != "energy_kwh_hh":
        df = df.withColumnRenamed(energy_col_name, "energy_kwh_hh")
    
    logger.info(f"Loaded energy data with {df.count()} records")
    return df


def load_household_data(spark: SparkSession, data_path: str):
    """
    Load household information data from CSV.
    
    :param spark: SparkSession instance
    :param data_path: Path to data directory
    :return: DataFrame with household information
    """
    household_file = os.path.join(data_path, "informations_households.csv")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(household_file)
    
    logger.info(f"Loaded household data with {df.count()} records")
    return df


def load_weather_hourly_data(spark: SparkSession, data_path: str):
    """
    Load hourly weather data from CSV.
    
    :param spark: SparkSession instance
    :param data_path: Path to data directory
    :return: DataFrame with hourly weather data
    """
    weather_file = os.path.join(data_path, "weather_hourly_darksky.csv")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(weather_file)
    
    logger.info(f"Loaded hourly weather data with {df.count()} records")
    return df


def load_weather_daily_data(spark: SparkSession, data_path: str):
    """
    Load daily weather data from CSV.
    
    :param spark: SparkSession instance
    :param data_path: Path to data directory
    :return: DataFrame with daily weather data
    """
    weather_file = os.path.join(data_path, "weather_daily_darksky.csv")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .csv(weather_file)
    
    logger.info(f"Loaded daily weather data with {df.count()} records")
    return df


def transform_energy_data(energy_df, household_df, weather_hourly_df):
    """
    Transform and denormalize energy data with household and weather information.
    
    Optimizations for BigQuery:
    - Schema flattening (join household and weather data)
    - Data type conversions (proper numeric and timestamp types)
    - Add partition columns (date-based)
    - Add clustering columns (meter_id, acorn_group)
    - Add derived analytics columns
    
    :param energy_df: Energy consumption DataFrame
    :param household_df: Household information DataFrame
    :param weather_hourly_df: Hourly weather DataFrame
    :return: Transformed and denormalized DataFrame
    """
    energy_transformed = energy_df \
        .withColumn("reading_timestamp", to_timestamp(col("tstp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("energy_kwh", col("energy_kwh_hh").cast(DoubleType())) \
        .withColumn("reading_date", to_date(col("reading_timestamp"))) \
        .withColumn("reading_hour", hour(col("reading_timestamp"))) \
        .withColumn("reading_day_of_week", dayofweek(col("reading_timestamp"))) \
        .withColumn("reading_month", month(col("reading_timestamp"))) \
        .withColumn("reading_year", year(col("reading_timestamp"))) \
        .withColumnRenamed("LCLid", "meter_id") \
        .drop("tstp", "energy_kwh_hh")
    
    household_transformed = household_df \
        .withColumnRenamed("LCLid", "meter_id") \
        .withColumnRenamed("stdorToU", "tariff_type") \
        .withColumnRenamed("Acorn", "acorn_category") \
        .withColumnRenamed("Acorn_grouped", "acorn_group") \
        .withColumn("tariff_type", 
            when(col("tariff_type") == "Std", lit("Standard"))
            .when(col("tariff_type") == "ToU", lit("Time-of-Use"))
            .otherwise(col("tariff_type"))) \
        .drop("file")
    
    weather_transformed = weather_hourly_df \
        .withColumn("weather_timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("weather_hour", hour(col("weather_timestamp"))) \
        .withColumn("weather_date", to_date(col("weather_timestamp"))) \
        .withColumn("temperature_celsius", col("temperature").cast(DoubleType())) \
        .withColumn("apparent_temperature_celsius", col("apparentTemperature").cast(DoubleType())) \
        .withColumn("humidity_pct", col("humidity").cast(DoubleType())) \
        .withColumn("pressure_hpa", col("pressure").cast(DoubleType())) \
        .withColumn("wind_speed_mps", col("windSpeed").cast(DoubleType())) \
        .withColumn("wind_bearing_deg", col("windBearing").cast(IntegerType())) \
        .withColumn("cloud_cover_pct", col("cloudCover").cast(DoubleType())) \
        .withColumn("visibility_km", col("visibility").cast(DoubleType())) \
        .withColumn("precip_intensity_mm", col("precipIntensity").cast(DoubleType())) \
        .withColumn("precip_probability", col("precipProbability").cast(DoubleType())) \
        .withColumn("uv_index", col("uvIndex").cast(IntegerType())) \
        .withColumnRenamed("summary", "weather_summary") \
        .withColumnRenamed("icon", "weather_icon") \
        .select(
            "weather_date", "weather_hour", "weather_summary", "weather_icon",
            "temperature_celsius", "apparent_temperature_celsius", "humidity_pct",
            "pressure_hpa", "wind_speed_mps", "wind_bearing_deg", "cloud_cover_pct",
            "visibility_km", "precip_intensity_mm", "precip_probability", "uv_index"
        )
    
    denormalized_df = energy_transformed \
        .join(household_transformed, on="meter_id", how="left") \
        .join(
            weather_transformed,
            (energy_transformed.reading_date == weather_transformed.weather_date) &
            (energy_transformed.reading_hour == weather_transformed.weather_hour),
            how="left"
        ) \
        .drop("weather_date", "weather_hour")
    
    final_df = denormalized_df \
        .withColumn("is_peak_hour",
            when((col("reading_hour") >= 17) & (col("reading_hour") <= 21), lit(True))
            .otherwise(lit(False))) \
        .withColumn("is_weekend",
            when((col("reading_day_of_week") == 1) | (col("reading_day_of_week") == 7), lit(True))
            .otherwise(lit(False))) \
        .withColumn("time_of_day",
            when(col("reading_hour") < 6, lit("Night"))
            .when(col("reading_hour") < 12, lit("Morning"))
            .when(col("reading_hour") < 18, lit("Afternoon"))
            .otherwise(lit("Evening"))) \
        .withColumn("temperature_band",
            when(col("temperature_celsius").isNull(), lit("Unknown"))
            .when(col("temperature_celsius") < 5, lit("Cold"))
            .when(col("temperature_celsius") < 15, lit("Mild"))
            .when(col("temperature_celsius") < 25, lit("Warm"))
            .otherwise(lit("Hot"))) \
        .withColumn("consumption_level",
            when(col("energy_kwh").isNull(), lit("Unknown"))
            .when(col("energy_kwh") < 0.2, lit("Low"))
            .when(col("energy_kwh") < 0.5, lit("Medium"))
            .when(col("energy_kwh") < 1.0, lit("High"))
            .otherwise(lit("Very High"))) \
        .withColumn("energy_kwh", spark_round(col("energy_kwh"), 4)) \
        .withColumn("temperature_celsius", spark_round(col("temperature_celsius"), 2)) \
        .withColumn("apparent_temperature_celsius", spark_round(col("apparent_temperature_celsius"), 2)) \
        .withColumn("humidity_pct", spark_round(col("humidity_pct"), 2)) \
        .withColumn("wind_speed_mps", spark_round(col("wind_speed_mps"), 2)) \
        .withColumn("cloud_cover_pct", spark_round(col("cloud_cover_pct"), 2)) \
        .withColumn("visibility_km", spark_round(col("visibility_km"), 2)) \
        .withColumn("precip_intensity_mm", spark_round(col("precip_intensity_mm"), 2)) \
        .withColumn("precip_probability", spark_round(col("precip_probability"), 2))
    
    final_columns = [
        "meter_id",
        "reading_timestamp",
        "reading_date",
        "reading_hour",
        "reading_day_of_week",
        "reading_month",
        "reading_year",
        "energy_kwh",
        "tariff_type",
        "acorn_category",
        "acorn_group",
        "is_peak_hour",
        "is_weekend",
        "time_of_day",
        "consumption_level",
        "weather_summary",
        "weather_icon",
        "temperature_celsius",
        "apparent_temperature_celsius",
        "temperature_band",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed_mps",
        "wind_bearing_deg",
        "cloud_cover_pct",
        "visibility_km",
        "precip_intensity_mm",
        "precip_probability",
        "uv_index"
    ]
    
    return final_df.select(final_columns)


def transform_weather_daily_data(weather_daily_df):
    """
    Transform daily weather data for BigQuery.
    
    :param weather_daily_df: Daily weather DataFrame
    :return: Transformed DataFrame
    """
    transformed_df = weather_daily_df \
        .withColumn("weather_date", to_date(col("time"), "yyyy-MM-dd")) \
        .withColumn("sunrise_time", to_timestamp(col("sunriseTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("sunset_time", to_timestamp(col("sunsetTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("moon_phase", col("moonPhase").cast(DoubleType())) \
        .withColumn("precip_intensity_mm", col("precipIntensity").cast(DoubleType())) \
        .withColumn("precip_intensity_max_mm", col("precipIntensityMax").cast(DoubleType())) \
        .withColumn("precip_probability", col("precipProbability").cast(DoubleType())) \
        .withColumnRenamed("precipType", "precip_type") \
        .withColumn("temperature_high_celsius", col("temperatureHigh").cast(DoubleType())) \
        .withColumn("temperature_low_celsius", col("temperatureLow").cast(DoubleType())) \
        .withColumn("temperature_min_celsius", col("temperatureMin").cast(DoubleType())) \
        .withColumn("temperature_max_celsius", col("temperatureMax").cast(DoubleType())) \
        .withColumn("apparent_temp_high_celsius", col("apparentTemperatureHigh").cast(DoubleType())) \
        .withColumn("apparent_temp_low_celsius", col("apparentTemperatureLow").cast(DoubleType())) \
        .withColumn("dew_point_celsius", col("dewPoint").cast(DoubleType())) \
        .withColumn("humidity_pct", col("humidity").cast(DoubleType())) \
        .withColumn("pressure_hpa", col("pressure").cast(DoubleType())) \
        .withColumn("wind_speed_mps", col("windSpeed").cast(DoubleType())) \
        .withColumn("wind_gust_mps", col("windGust").cast(DoubleType())) \
        .withColumn("wind_bearing_deg", col("windBearing").cast(IntegerType())) \
        .withColumn("cloud_cover_pct", col("cloudCover").cast(DoubleType())) \
        .withColumn("uv_index", col("uvIndex").cast(IntegerType())) \
        .withColumn("visibility_km", col("visibility").cast(DoubleType())) \
        .withColumnRenamed("summary", "weather_summary") \
        .withColumnRenamed("icon", "weather_icon")
    
    final_columns = [
        "weather_date",
        "weather_summary",
        "weather_icon",
        "sunrise_time",
        "sunset_time",
        "moon_phase",
        "precip_intensity_mm",
        "precip_intensity_max_mm",
        "precip_probability",
        "precip_type",
        "temperature_high_celsius",
        "temperature_low_celsius",
        "temperature_min_celsius",
        "temperature_max_celsius",
        "apparent_temp_high_celsius",
        "apparent_temp_low_celsius",
        "dew_point_celsius",
        "humidity_pct",
        "pressure_hpa",
        "wind_speed_mps",
        "wind_gust_mps",
        "wind_bearing_deg",
        "cloud_cover_pct",
        "uv_index",
        "visibility_km"
    ]
    
    return transformed_df.select(final_columns)


def transform_household_data(household_df):
    """
    Transform household data for BigQuery.
    
    :param household_df: Household DataFrame
    :return: Transformed DataFrame
    """
    transformed_df = household_df \
        .withColumnRenamed("LCLid", "meter_id") \
        .withColumn("tariff_type",
            when(col("stdorToU") == "Std", lit("Standard"))
            .when(col("stdorToU") == "ToU", lit("Time-of-Use"))
            .otherwise(col("stdorToU"))) \
        .withColumnRenamed("Acorn", "acorn_category") \
        .withColumnRenamed("Acorn_grouped", "acorn_group") \
        .withColumnRenamed("file", "source_file")
    
    return transformed_df.select(
        "meter_id", "tariff_type", "acorn_category", "acorn_group", "source_file"
    )


def create_bigquery_dataset(client: bigquery.Client, dataset_id: str):
    """
    Create BigQuery dataset if it doesn't exist.
    
    :param client: BigQuery client
    :param dataset_id: Dataset ID to create
    """
    dataset_ref = f"{client.project}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    
    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_ref} created or already exists")
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        raise


def upload_to_bigquery(
    client: bigquery.Client,
    df,
    dataset_id: str,
    table_id: str,
    partition_field: str = None,
    clustering_fields: list = None
):
    """
    Upload PySpark DataFrame to BigQuery with partitioning and clustering.
    
    :param client: BigQuery client
    :param df: PySpark DataFrame to upload
    :param dataset_id: BigQuery dataset ID
    :param table_id: BigQuery table ID
    :param partition_field: Field to partition by (DATE type)
    :param clustering_fields: List of fields to cluster by
    """
    import pandas as pd
    
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    
    pandas_df = df.toPandas()
    
    for col_name in pandas_df.columns:
        if col_name.endswith("_date"):
            if pandas_df[col_name].dtype == 'object':
                pandas_df[col_name] = pd.to_datetime(pandas_df[col_name]).dt.date
            elif hasattr(pandas_df[col_name].iloc[0] if len(pandas_df) > 0 else None, 'date'):
                pass
    
    for col_name in pandas_df.columns:
        if "timestamp" in col_name or col_name.endswith("_time"):
            if pandas_df[col_name].dtype == 'object':
                pandas_df[col_name] = pd.to_datetime(pandas_df[col_name])
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
    )
    
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        logger.info(f"Partitioning by: {partition_field}")
    
    if clustering_fields:
        job_config.clustering_fields = clustering_fields
        logger.info(f"Clustering by: {clustering_fields}")
    
    schema = []
    for col_name in pandas_df.columns:
        dtype = str(pandas_df[col_name].dtype)
        sample_val = pandas_df[col_name].iloc[0] if len(pandas_df) > 0 else None
        
        if col_name.endswith("_date"):
            schema.append(bigquery.SchemaField(col_name, "DATE"))
        elif "datetime" in dtype or (sample_val is not None and hasattr(sample_val, 'timestamp')):
            schema.append(bigquery.SchemaField(col_name, "TIMESTAMP"))
        elif "float" in dtype:
            schema.append(bigquery.SchemaField(col_name, "FLOAT64"))
        elif "int" in dtype:
            schema.append(bigquery.SchemaField(col_name, "INT64"))
        elif "bool" in dtype:
            schema.append(bigquery.SchemaField(col_name, "BOOL"))
        else:
            schema.append(bigquery.SchemaField(col_name, "STRING"))
    
    job_config.schema = schema
    
    logger.info(f"Uploading {len(pandas_df)} rows to {table_ref}")
    
    job = client.load_table_from_dataframe(
        pandas_df,
        table_ref,
        job_config=job_config
    )
    
    job.result()
    
    table = client.get_table(table_ref)
    logger.info(f"Loaded {table.num_rows} rows to {table_ref}")
    
    return table


def main():
    """
    Main entry point for the Smart Meters to BigQuery pipeline.
    
    Orchestrates:
    1. Loading data from CSV files
    2. Transforming and optimizing for BigQuery
    3. Uploading to BigQuery with partitioning and clustering
    """
    credentials_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/tmp/bigquery_credentials.json"
    )
    
    if "GOOGLE_KEY" in os.environ and not os.path.exists(credentials_path):
        with open(credentials_path, "w") as f:
            f.write(os.environ["GOOGLE_KEY"])
        logger.info(f"Wrote credentials to {credentials_path}")
    
    data_path = os.environ.get(
        "DATA_PATH",
        "/home/ubuntu/repos/Building-streaming-ETL-Data-pipeline/data/sample"
    )
    
    dataset_id = os.environ.get("BIGQUERY_DATASET", "smart_meters_london")
    
    logger.info("=" * 60)
    logger.info("Smart Meters London to BigQuery Pipeline")
    logger.info("=" * 60)
    logger.info(f"Data path: {data_path}")
    logger.info(f"BigQuery dataset: {dataset_id}")
    
    bq_client = get_bigquery_client(credentials_path)
    
    spark = initialize_spark_session("SmartMetersToBigQuery")
    
    try:
        logger.info("Loading data from CSV files...")
        energy_df = load_energy_data(spark, data_path)
        household_df = load_household_data(spark, data_path)
        weather_hourly_df = load_weather_hourly_data(spark, data_path)
        weather_daily_df = load_weather_daily_data(spark, data_path)
        
        logger.info("Transforming data for BigQuery optimization...")
        
        energy_denormalized_df = transform_energy_data(
            energy_df, household_df, weather_hourly_df
        )
        logger.info(f"Denormalized energy data: {energy_denormalized_df.count()} records")
        
        weather_daily_transformed_df = transform_weather_daily_data(weather_daily_df)
        logger.info(f"Transformed daily weather: {weather_daily_transformed_df.count()} records")
        
        household_transformed_df = transform_household_data(household_df)
        logger.info(f"Transformed household data: {household_transformed_df.count()} records")
        
        logger.info("Creating BigQuery dataset...")
        create_bigquery_dataset(bq_client, dataset_id)
        
        logger.info("Uploading energy readings to BigQuery...")
        upload_to_bigquery(
            bq_client,
            energy_denormalized_df,
            dataset_id,
            "energy_readings",
            partition_field="reading_date",
            clustering_fields=["meter_id", "acorn_group"]
        )
        
        logger.info("Uploading daily weather to BigQuery...")
        upload_to_bigquery(
            bq_client,
            weather_daily_transformed_df,
            dataset_id,
            "weather_daily",
            partition_field="weather_date"
        )
        
        logger.info("Uploading household info to BigQuery...")
        upload_to_bigquery(
            bq_client,
            household_transformed_df,
            dataset_id,
            "households",
            clustering_fields=["acorn_group", "tariff_type"]
        )
        
        logger.info("=" * 60)
        logger.info("Pipeline completed successfully!")
        logger.info(f"Data uploaded to BigQuery dataset: {bq_client.project}.{dataset_id}")
        logger.info("Tables created:")
        logger.info("  - energy_readings (partitioned by reading_date, clustered by meter_id, acorn_group)")
        logger.info("  - weather_daily (partitioned by weather_date)")
        logger.info("  - households (clustered by acorn_group, tariff_type)")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
