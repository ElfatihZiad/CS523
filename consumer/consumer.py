from kafka import KafkaConsumer
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()],
)

consumer = KafkaConsumer(
    "iot-data",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="iot-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

logging.info("Listening for IoT data...")

for message in consumer:
    data = message.value
    logging.info(f"Received IoT Data | offset={message.offset} | data={data}")
