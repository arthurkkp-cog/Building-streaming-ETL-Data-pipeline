import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import json
import time
import csv
import os
from confluent_kafka import Producer
import logging
import psycopg2

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("smart_meter_streaming")


def log_pipeline_start(dag_id):
    """Log the start of a pipeline run to the database."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT pipeline_ops.log_run_start(%s);"
    return postgres_hook.get_first(sql, parameters=[dag_id])[0]


def log_pipeline_complete(run_id, records_processed, status='SUCCESS', error_message=None):
    """Log the completion of a pipeline run to the database."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
    SELECT pipeline_ops.log_run_complete(%s, CURRENT_TIMESTAMP, %s, %s, %s);
    """
    postgres_hook.run(sql, parameters=[run_id, records_processed, status, error_message])


def check_data_completeness(run_id, expected_count, actual_count):
    """Check data completeness and log the result."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT pipeline_ops.check_data_completeness(%s, %s, %s);"
    return postgres_hook.get_first(sql, parameters=[run_id, expected_count, actual_count])[0]


KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker_1:19092',
                           'kafka_broker_2:19093', 'kafka_broker_3:19094']
KAFKA_TOPIC = "smart-meter-topic"
PAUSE_INTERVAL = 1
STREAMING_DURATION = 300

DATA_DIR = "/opt/airflow/data/sample"
HOUSEHOLDS_FILE = os.path.join(DATA_DIR, "informations_households.csv")
ENERGY_FILE = os.path.join(DATA_DIR, "halfhourly_dataset_block_0.csv")
WEATHER_FILE = os.path.join(DATA_DIR, "weather_hourly_darksky.csv")


def load_households_info() -> dict:
    """Load household information including ACORN classification and tariff type."""
    households = {}
    try:
        with open(HOUSEHOLDS_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                households[row['LCLid']] = {
                    'tariff_type': row['stdorToU'],
                    'acorn_group': row['Acorn'],
                    'acorn_category': row['Acorn_grouped']
                }
        logger.info(f"Loaded {len(households)} household records")
    except FileNotFoundError:
        logger.warning(f"Households file not found: {HOUSEHOLDS_FILE}")
    return households


def load_weather_data() -> dict:
    """Load hourly weather data indexed by timestamp."""
    weather = {}
    try:
        with open(WEATHER_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                weather[row['time']] = {
                    'temperature': float(row['temperature']),
                    'humidity': float(row['humidity']),
                    'wind_speed': float(row['windSpeed']),
                    'cloud_cover': float(row['cloudCover']),
                    'weather_summary': row['summary'],
                    'precip_intensity': float(row['precipIntensity']),
                    'pressure': float(row['pressure'])
                }
        logger.info(f"Loaded {len(weather)} weather records")
    except FileNotFoundError:
        logger.warning(f"Weather file not found: {WEATHER_FILE}")
    return weather


def load_energy_readings() -> list:
    """Load half-hourly energy consumption readings."""
    readings = []
    try:
        with open(ENERGY_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                readings.append({
                    'meter_id': row['LCLid'],
                    'timestamp': row['tstp'],
                    'energy_kwh': float(row['energy(kWh/hh)'])
                })
        logger.info(f"Loaded {len(readings)} energy readings")
    except FileNotFoundError:
        logger.warning(f"Energy file not found: {ENERGY_FILE}")
    return readings


def get_hour_key(timestamp: str) -> str:
    """Extract hour key from timestamp for weather lookup."""
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    return dt.strftime('%Y-%m-%d %H:00:00')


def enrich_reading(reading: dict, households: dict, weather: dict) -> dict:
    """Enrich energy reading with household and weather information."""
    meter_id = reading['meter_id']
    timestamp = reading['timestamp']
    hour_key = get_hour_key(timestamp)
    
    household_info = households.get(meter_id, {
        'tariff_type': 'Unknown',
        'acorn_group': 'Unknown',
        'acorn_category': 'Unknown'
    })
    
    weather_info = weather.get(hour_key, {
        'temperature': 0.0,
        'humidity': 0.0,
        'wind_speed': 0.0,
        'cloud_cover': 0.0,
        'weather_summary': 'Unknown',
        'precip_intensity': 0.0,
        'pressure': 0.0
    })
    
    enriched = {
        'meter_id': meter_id,
        'timestamp': timestamp,
        'energy_kwh': reading['energy_kwh'],
        'tariff_type': household_info['tariff_type'],
        'acorn_group': household_info['acorn_group'],
        'acorn_category': household_info['acorn_category'],
        'temperature': weather_info['temperature'],
        'humidity': weather_info['humidity'],
        'wind_speed': weather_info['wind_speed'],
        'cloud_cover': weather_info['cloud_cover'],
        'weather_summary': weather_info['weather_summary'],
        'precip_intensity': weather_info['precip_intensity'],
        'pressure': weather_info['pressure'],
        'ingestion_time': datetime.utcnow().isoformat()
    }
    
    return enriched


def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'smart_meter_producer'
    }
    return Producer(settings)


def delivery_status(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [Partition: {msg.partition()}]')


def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    producer.produce(topic, value=json.dumps(data).encode('utf-8'), callback=delivery_status)
    producer.flush()


def stream_smart_meter_data(**context):
    """Main function to stream enriched smart meter data to Kafka."""
    logger.info("Starting smart meter data streaming...")
    
    dag_id = context.get('dag').dag_id if context.get('dag') else 'smart_meter_streaming_pipeline'
    run_id = None
    
    try:
        run_id = log_pipeline_start(dag_id)
        logger.info(f"Pipeline run started with run_id: {run_id}")
    except Exception as e:
        logger.warning(f"Could not log pipeline start: {e}")
    
    households = load_households_info()
    weather = load_weather_data()
    readings = load_energy_readings()
    
    if not readings:
        logger.error("No energy readings found. Exiting.")
        if run_id:
            try:
                log_pipeline_complete(run_id, 0, 'FAILED', 'No energy readings found')
            except Exception as e:
                logger.warning(f"Could not log pipeline failure: {e}")
        return
    
    kafka_producer = configure_kafka()
    
    reading_index = 0
    total_readings = len(readings)
    messages_sent = 0
    
    try:
        start_time = time.time()
        while (time.time() - start_time) < STREAMING_DURATION:
            reading = readings[reading_index % total_readings]
            enriched_reading = enrich_reading(reading, households, weather)
            
            publish_to_kafka(kafka_producer, KAFKA_TOPIC, enriched_reading)
            messages_sent += 1
            
            if messages_sent % 10 == 0:
                logger.info(f"Sent {messages_sent} messages to Kafka")
            
            reading_index += 1
            time.sleep(PAUSE_INTERVAL)
        
        logger.info(f"Streaming complete. Total messages sent: {messages_sent}")
        
        if run_id:
            try:
                log_pipeline_complete(run_id, messages_sent, 'SUCCESS')
                check_data_completeness(run_id, STREAMING_DURATION // PAUSE_INTERVAL, messages_sent)
            except Exception as e:
                logger.warning(f"Could not log pipeline completion: {e}")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        if run_id:
            try:
                log_pipeline_complete(run_id, messages_sent, 'FAILED', str(e))
            except Exception as log_e:
                logger.warning(f"Could not log pipeline failure: {log_e}")
        raise


if __name__ == "__main__":
    stream_smart_meter_data()


DAG_DEFAULT_ARGS = {
    'owner': 'utilities_company',
    'start_date': datetime(2024, 5, 3, 10, 00),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG(
    'smart_meter_streaming_pipeline',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    description='Stream smart meter energy consumption data enriched with weather to Kafka',
    max_active_runs=1
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_smart_meter_data',
        python_callable=stream_smart_meter_data,
        provide_context=True,
        dag=dag
    )

    streaming_task
