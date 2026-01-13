import uuid
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import time
from confluent_kafka import Producer
import logging

KAFKA_BOOTSTRAP_SERVERS = ['kafka_broker_1:19092',
                           'kafka_broker_2:19093', 'kafka_broker_3:19094']
KAFKA_TOPIC = "streaming-topic"
PAUSE_INTERVAL = 10
STREAMING_DURATION = 120

SAMPLE_HOUSEHOLDS = [
    {"LCLid": "MAC000002", "stdorToU": "Std", "Acorn": "ACORN-A", "Acorn_grouped": "Affluent"},
    {"LCLid": "MAC000246", "stdorToU": "Std", "Acorn": "ACORN-A", "Acorn_grouped": "Affluent"},
    {"LCLid": "MAC000387", "stdorToU": "Std", "Acorn": "ACORN-B", "Acorn_grouped": "Affluent"},
    {"LCLid": "MAC001074", "stdorToU": "ToU", "Acorn": "ACORN-E", "Acorn_grouped": "Comfortable"},
    {"LCLid": "MAC003613", "stdorToU": "Std", "Acorn": "ACORN-Q", "Acorn_grouped": "Adversity"},
    {"LCLid": "MAC004387", "stdorToU": "Std", "Acorn": "ACORN-A", "Acorn_grouped": "Affluent"},
    {"LCLid": "MAC005492", "stdorToU": "ToU", "Acorn": "ACORN-E", "Acorn_grouped": "Comfortable"},
]


def generate_smart_meter_reading() -> dict:
    """Generates a simulated smart meter reading for London households."""
    household = random.choice(SAMPLE_HOUSEHOLDS)
    current_time = datetime.utcnow()
    half_hour_slot = current_time.replace(
        minute=30 if current_time.minute >= 30 else 0,
        second=0,
        microsecond=0
    )
    hour = current_time.hour
    if 6 <= hour < 9 or 17 <= hour < 21:
        base_consumption = random.uniform(0.3, 1.2)
    elif 9 <= hour < 17:
        base_consumption = random.uniform(0.1, 0.4)
    elif 21 <= hour < 24:
        base_consumption = random.uniform(0.2, 0.6)
    else:
        base_consumption = random.uniform(0.05, 0.2)

    energy_kwh = round(base_consumption * random.uniform(0.8, 1.2), 3)

    return {
        "LCLid": household["LCLid"],
        "stdorToU": household["stdorToU"],
        "Acorn": household["Acorn"],
        "Acorn_grouped": household["Acorn_grouped"],
        "tstp": half_hour_slot.strftime("%Y-%m-%d %H:%M:%S"),
        "energy_kWh": energy_kwh
    }


def configure_kafka(servers=KAFKA_BOOTSTRAP_SERVERS):
    """Creates and returns a Kafka producer instance."""
    settings = {
        'bootstrap.servers': ','.join(servers),
        'client.id': 'producer_instance'
    }
    return Producer(settings)


def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic."""
    producer.produce(topic, value=json.dumps(
        data).encode('utf-8'), callback=delivery_status)
    producer.flush()


def delivery_status(err, msg):
    """Reports the delivery status of the message to Kafka."""
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic(),
              '[Partition: {}]'.format(msg.partition()))


def initiate_stream():
    """Initiates the process to stream smart meter data to Kafka."""
    kafka_producer = configure_kafka()
    for _ in range(STREAMING_DURATION // PAUSE_INTERVAL):
        smart_meter_data = generate_smart_meter_reading()
        publish_to_kafka(kafka_producer, KAFKA_TOPIC, smart_meter_data)
        time.sleep(PAUSE_INTERVAL)


if __name__ == "__main__":
    initiate_stream()


# Define airflow dag for streaming service
DAG_DEFAULT_ARGS = {
    'owner': 'Coder2f',
    'start_date': datetime(2024, 5, 3, 10, 00),  # 2024 May 03 at 10:00 AM
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

# Creating the DAG with its configuration
with DAG(
    'london_smart_meters_etl_pipeline',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    description='Stream London smart meter energy consumption data to Kafka topic',
    max_active_runs=1
) as dag:

    # Defining the data streaming task using PythonOperator
    streaming_task = PythonOperator(
        task_id='stream_to_kafka_task',
        python_callable=initiate_stream,
        dag=dag
    )

    streaming_task
