from kafka import KafkaProducer
import requests
import json
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)

KAFKA_BROKER = "kafka:9092"
TOPIC = "iot-data"

API_URL = "https://api.sensor.community/static/v2/data.json"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    max_request_size=20971520,
)


def fetch_iot_data():
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    return response.json()


def produce_to_kafka(data):
    for record in data:
        producer.send(
            TOPIC,
            key=(str(record["id"]) + "|" + str(record["timestamp"])).encode(),
            value=record,
        )
        logging.info(f"Produced: {str(record['id'])}")

    # logging.info(f"Produced: {data}")

    producer.flush()


while True:

    logging.info("Starting IoT sync job")

    try:
        data = fetch_iot_data()

        produce_to_kafka(data)

        logging.info("Job completed successfully")

    except Exception as e:
        logging.error(f"Job failed: {e}")

    logging.info("Sleeping for 5 minutes...")

    time.sleep(300)
