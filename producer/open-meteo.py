import json
import time
import os
import logging
import requests
import uuid

from datadog import initialize, statsd
from kafka import KafkaProducer


def parse_float_env(key: str, default: str) -> float:
    raw = os.getenv(key, default)
    if isinstance(raw, str):
        raw = raw.strip().strip('"').strip("'")
    return float(raw)


def parse_int_env(key: str, default: str) -> int:
    raw = os.getenv(key, default)
    if isinstance(raw, str):
        raw = raw.strip().strip('"').strip("'")
    return int(raw)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensores")
LAT = parse_float_env("LATITUDE", "-33.45")
LON = parse_float_env("LONGITUDE", "-70.66")
POLL_SECONDS = parse_int_env("POLL_SECONDS", "300")
DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

initialize(
    statsd_host=DATADOG_HOST,
    statsd_port=DATADOG_PORT
)

while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        logging.info("✅ Kafka listo")
        break
    except Exception as exc:
        logging.warning("Esperando Kafka... %s", exc)
        time.sleep(5)

while True:
    try:
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": LAT,
                "longitude": LON,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
                "timezone": "auto"
            },
            timeout=10
        )
        response.raise_for_status()
        payload = response.json()

        current = payload.get("current", {})

        event_id = uuid.uuid4().hex

        data = {
            "event_id": event_id,
            "temperature": current.get("temperature_2m"),
            "humidity": current.get("relative_humidity_2m"),
            "wind_speed": current.get("wind_speed_10m"),
            "timestamp": time.time(),
            "source_topic": KAFKA_TOPIC,
            "source": "open-meteo"
        }

        if data["temperature"] is None or data["wind_speed"] is None or data["humidity"] is None:
            raise ValueError("Open-Meteo returned incomplete current weather data")

        producer.send(KAFKA_TOPIC, data)
        statsd.increment("produccion.clima.enviados")
        statsd.gauge("clima.temperatura", data["temperature"])
        statsd.gauge("clima.humedad", data["humidity"])
        statsd.gauge("clima.velocidad_viento", data["wind_speed"])
        logging.info("Enviado: event_id=%s %s", event_id, data)
    except Exception as exc:
        logging.error("Error obteniendo datos meteorológicos: %s", exc, exc_info=True)

    time.sleep(POLL_SECONDS)