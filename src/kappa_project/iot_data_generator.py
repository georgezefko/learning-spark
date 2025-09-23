# This file is the same used for the Lambda Archtiecture project
# The whole project can be found here


import json
import random
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
import os

# load_dotenv()

NUM_DEVICES = 10
EVENTS_PER_SECOND = 10  # total across all devices
LATE_PROB = 0.0  # set to e.g. 0.05 to send 5% late events
LATE_MAX_SECONDS = 180  # max lateness for late events
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def now_iso_utc_ms():
    # e.g. 2025-09-21T12:34:56.789Z
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def iso_utc_ms_from_epoch(ts: float):
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# Function to generate fake telemetry data
def generate_telemetry(device_id):
    # Base telemetry with slight tendency to degrade over time
    temp = round(random.uniform(0.0, 40.0), 1)

    # optionally send a late timestamp to demo watermarking
    if LATE_PROB > 0 and random.random() < LATE_PROB:
        late_by = random.randint(1, LATE_MAX_SECONDS)
        ts = iso_utc_ms_from_epoch(time.time() - late_by)
    else:
        ts = now_iso_utc_ms()

    return {"device_id": device_id, "timestamp": ts, "temperature": temp}


# Function to deliver reports (callback)
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    # Kafka configuration
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,  # Kafka broker address
        "client.id": "iot-data-producer",
        "on_delivery": delivery_report,
    }

    # Create a Kafka producer
    producer = Producer(conf)

    # Topics to send data to
    telemetry_topic = "iot-telemetry"

    # Simulate IoT devices sending data
    device_ids = [
        f"device_{i}" for i in range(1, NUM_DEVICES + 1)
    ]  # Simulate 10 devices

    interval = 1.0 / max(1.0, EVENTS_PER_SECOND)
    idx = 0
    try:
        while True:
            device_id = device_ids[idx]
            telemetry_data = generate_telemetry(device_id)

            producer.produce(
                telemetry_topic,
                key=device_id,
                value=json.dumps(telemetry_data, separators=(",", ":")),
                on_delivery=delivery_report,
            )

            producer.poll(0)
            idx = (idx + 1) % len(device_ids)
            time.sleep(interval)  # Send data every second
    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        producer.flush(5)
    return {}


if __name__ == "__main__":
    load_data()
