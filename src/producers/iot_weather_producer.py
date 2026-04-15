"""
Project 1 — IoT Weather Sensor Producer
Polls Open-Meteo API every 60s for 5 Indonesian cities → Kafka topic: iot-weather-sensors
"""

import json
import time
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "iot-weather-sensors"
POLL_INTERVAL_SEC = 60

SENSOR_NODES = [
    {"node_id": "sensor_jkt_01", "city": "Jakarta",   "lat": -6.2,  "lon": 106.8},
    {"node_id": "sensor_sby_01", "city": "Surabaya",  "lat": -7.25, "lon": 112.75},
    {"node_id": "sensor_mdn_01", "city": "Medan",     "lat": 3.58,  "lon": 98.67},
    {"node_id": "sensor_bdg_01", "city": "Bandung",   "lat": -6.92, "lon": 107.61},
    {"node_id": "sensor_mks_01", "city": "Makassar",  "lat": -5.14, "lon": 119.41},
]

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def fetch_weather(lat: float, lon: float) -> dict | None:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,wind_direction_10m",
        "timezone": "Asia/Jakarta",
    }
    try:
        resp = requests.get(OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        return resp.json().get("current", {})
    except requests.RequestException as e:
        logger.error(f"Open-Meteo fetch error: {e}")
        return None


def build_record(sensor: dict, weather: dict) -> dict:
    return {
        "sensor_id": sensor["node_id"],
        "city": sensor["city"],
        "latitude": sensor["lat"],
        "longitude": sensor["lon"],
        "temperature_c": weather.get("temperature_2m"),
        "humidity_pct": weather.get("relative_humidity_2m"),
        "precipitation_mm": weather.get("precipitation"),
        "wind_speed_kmh": weather.get("wind_speed_10m"),
        "wind_direction_deg": weather.get("wind_direction_10m"),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "source": "open_meteo",
    }


def run():
    producer = create_producer()
    logger.info(f"IoT producer started. Polling {len(SENSOR_NODES)} nodes every {POLL_INTERVAL_SEC}s")

    while True:
        sent = 0
        for sensor in SENSOR_NODES:
            weather = fetch_weather(sensor["lat"], sensor["lon"])
            if not weather:
                continue
            record = build_record(sensor, weather)
            try:
                producer.send(TOPIC, key=record["sensor_id"], value=record)
                sent += 1
            except KafkaError as e:
                logger.error(f"Kafka send error [{sensor['node_id']}]: {e}")

        producer.flush()
        logger.info(f"Sent {sent}/{len(SENSOR_NODES)} sensor readings → [{TOPIC}]")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run()
