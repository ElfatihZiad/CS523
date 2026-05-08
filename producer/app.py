from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
import time

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "iot-sensor-data"


@app.route("/iot-data", methods=["POST"])
def send_iot_data():

    data = request.json

    payload = {
        "device_id": data.get("device_id"),
        "temperature": data.get("temperature"),
        "humidity": data.get("humidity"),
        "timestamp": time.time(),
    }

    producer.send(TOPIC, payload)
    producer.flush()

    return jsonify({"status": "sent", "payload": payload})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
