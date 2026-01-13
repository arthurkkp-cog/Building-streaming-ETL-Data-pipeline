# Smart Meters Energy Consumption Pipeline

This extension to the streaming ETL pipeline demonstrates a real-world data engineering use-case for utilities companies: **Real-time Energy Demand Monitoring and Forecasting**.

## Overview

The pipeline processes smart meter energy consumption data from London households, enriches it with weather information, and provides real-time analytics for:

- **Demand Forecasting**: Aggregate consumption patterns by demographic groups and time windows
- **Anomaly Detection**: Identify unusual consumption patterns for fraud detection or equipment issues
- **Weather Correlation**: Analyze the relationship between weather conditions and energy consumption

## Dataset

This pipeline is designed to work with the [Smart Meters in London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london) dataset from Kaggle, which contains:

- **5,567 London Households** participating in the UK Power Networks Low Carbon London project
- **Half-hourly energy consumption readings** from November 2011 to February 2014
- **ACORN demographic classification** for customer segmentation
- **Weather data** (hourly and daily) from the DarkSky API

### Data Files

| File | Description |
|------|-------------|
| `informations_households.csv` | Household metadata (meter ID, tariff type, ACORN group) |
| `halfhourly_dataset_block_*.csv` | Half-hourly energy readings (kWh) |
| `weather_hourly_darksky.csv` | Hourly weather conditions |
| `weather_daily_darksky.csv` | Daily weather summaries |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Data Ingestion (Airflow DAG)                  │
│  smart_meter_streaming.py                                        │
│  - Reads CSV files (energy readings, households, weather)        │
│  - Enriches readings with household and weather data             │
│  - Publishes to Kafka topic: smart-meter-topic                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Message Queue (Kafka)                         │
│  Topic: smart-meter-topic (6 partitions)                         │
│  - Enriched smart meter readings with weather context            │
│  - JSON format with 14 fields per message                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                Stream Processing (Spark)                         │
│  smart_meter_processing.py                                       │
│  - Transforms and validates incoming data                        │
│  - Adds derived columns (peak hours, temperature bands)          │
│  - Detects anomalies in consumption patterns                     │
│  - Calculates windowed aggregations by demographic group         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                    Data Lake (Minio S3)                          │
│  Bucket: smart-meter-data                                        │
│  ├── raw/           # All processed readings (Parquet)           │
│  ├── aggregated/    # 5-minute windowed aggregations             │
│  ├── anomalies/     # Flagged anomalous readings                 │
│  └── checkpoints/   # Spark streaming checkpoints                │
└─────────────────────────────────────────────────────────────────┘
```

## Data Schema

### Enriched Smart Meter Reading (Kafka Message)

```json
{
  "meter_id": "MAC000002",
  "timestamp": "2013-01-01 18:30:00",
  "energy_kwh": 0.985,
  "tariff_type": "Std",
  "acorn_group": "ACORN-A",
  "acorn_category": "Affluent",
  "temperature": 5.2,
  "humidity": 0.85,
  "wind_speed": 7.2,
  "cloud_cover": 0.65,
  "weather_summary": "Cloudy",
  "precip_intensity": 0.0,
  "pressure": 1012.8,
  "ingestion_time": "2024-01-15T10:30:00.000Z"
}
```

### Derived Columns (Spark Processing)

| Column | Description |
|--------|-------------|
| `hour_of_day` | Hour extracted from timestamp (0-23) |
| `day_of_week` | Day of week (1=Sunday, 7=Saturday) |
| `is_peak_hour` | True if 17:00-21:00 (evening peak) |
| `is_weekend` | True if Saturday or Sunday |
| `temperature_band` | Cold (<5°C), Mild (5-15°C), Warm (15-25°C), Hot (>25°C) |
| `consumption_level` | Low (<0.2), Medium (0.2-0.5), High (0.5-1.0), Very High (>1.0) |

### Anomaly Detection Rules

The pipeline flags readings as anomalous based on:

1. **Extremely high consumption**: > 2.0 kWh per half-hour
2. **High nighttime consumption**: > 0.5 kWh between 2:00-5:00 AM
3. **High weekend daytime consumption**: > 1.5 kWh on weekends 9:00 AM - 5:00 PM

## Setup Instructions

### 1. Prepare the Environment

Ensure all services are running:

```bash
docker compose up -d
```

### 2. Create Kafka Topic

Access Kafka UI at http://localhost:8888 and create:
- Topic name: `smart-meter-topic`
- Partitions: 6

### 3. Create Minio Bucket

Access Minio Console at http://localhost:9001 (use credentials from your .env file):
- Create bucket: `smart-meter-data`

### 4. Copy Data Files

The sample data files are in `data/sample/`. For production use, download the full dataset from Kaggle and place files in the appropriate location.

### 5. Download Spark Dependencies

```bash
cd scripts
./download_jars.sh
```

### 6. Run the Pipeline

**Start the Airflow DAG:**
- Access Airflow UI at http://localhost:8080 (credentials: airflow01/airflow01)
- Enable the `smart_meter_streaming_pipeline` DAG

**Start Spark Processing:**
```bash
cd scripts
./run_smart_meter_spark.sh
```

## Use Cases for Utilities Companies

### 1. Real-time Demand Monitoring
Monitor aggregate energy consumption across customer segments to balance grid load and optimize energy procurement.

### 2. Peak Demand Forecasting
Use windowed aggregations by ACORN demographic groups and weather conditions to predict peak demand periods.

### 3. Fraud Detection
Identify anomalous consumption patterns that may indicate meter tampering, energy theft, or equipment malfunction.

### 4. Customer Segmentation
Analyze consumption patterns by ACORN classification to design targeted energy efficiency programs and pricing strategies.

### 5. Weather-Correlated Analysis
Understand how temperature, humidity, and precipitation affect energy consumption for better demand forecasting.

## Monitoring

- **Airflow UI**: http://localhost:8080 - Monitor DAG runs and task status
- **Kafka UI**: http://localhost:8888 - View topic messages and consumer lag
- **Spark UI**: http://localhost:8085 - Monitor streaming job progress
- **Minio Console**: http://localhost:9001 - Verify output data in S3 buckets

## Sample Queries

Once data is in Minio, you can query the Parquet files using Spark SQL or tools like Trino/Presto:

```sql
-- Average consumption by ACORN category and temperature band
SELECT 
    acorn_category,
    temperature_band,
    AVG(total_energy_kwh) as avg_consumption,
    COUNT(*) as window_count
FROM aggregated_data
GROUP BY acorn_category, temperature_band
ORDER BY avg_consumption DESC;

-- Anomaly summary by meter
SELECT 
    meter_id,
    anomaly_reason,
    COUNT(*) as anomaly_count
FROM anomalies
GROUP BY meter_id, anomaly_reason
ORDER BY anomaly_count DESC;
```

## Configuration

Key configuration parameters in the pipeline:

| Parameter | Location | Default | Description |
|-----------|----------|---------|-------------|
| `KAFKA_TOPIC` | DAG | `smart-meter-topic` | Kafka topic for smart meter data |
| `STREAMING_DURATION` | DAG | 300 seconds | Duration of each streaming run |
| `PAUSE_INTERVAL` | DAG | 1 second | Delay between messages |
| `processingTime` | Spark | 5 seconds | Micro-batch trigger interval |
| `watermark` | Spark | 1 minute | Late data tolerance |

## License

This project is licensed under the MIT License. The Smart Meters in London dataset is available under the Open Database License (ODbL).
