from kafka import KafkaConsumer
import json
import logging
import time
import sys
import os

from datadog import initialize, statsd

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

from lake.uploader import upload


# ------------------------
# Datadog
# ------------------------

initialize(
    api_key="4e040f2fee6f2337ce4a56646eccb1fc",
    statsd_host="datadog",
    statsd_port=8125
)

logging.basicConfig(level=logging.INFO)


# ------------------------
# Esperar Kafka
# ------------------------

while True:
    try:

        consumer = KafkaConsumer(
            "sensores",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        logging.info("✅ Conectado a Kafka")
        break

    except Exception as e:

        logging.warning(
            f"⏳ Esperando Kafka: {e}"
        )

        time.sleep(5)


# ------------------------
# Consumo de eventos
# ------------------------

for msg in consumer:

    data = msg.value

    temperature = data["temperature"]
    humidity = data["humidity"]
    wind = data["wind_speed"]

    logging.info(f"Dato recibido: {data}")

    # ------------------------
    # Risk Score
    # ------------------------

    risk = 0

    if temperature > 35:
        risk += 40

    if humidity < 20:
        risk += 30

    if wind > 25:
        risk += 30

    logging.info(f"Risk Score: {risk}")

    # ------------------------
    # Datadog Metrics
    # ------------------------

    statsd.gauge(
        "wildfire.temperature",
        temperature
    )

    statsd.gauge(
        "wildfire.humidity",
        humidity
    )

    statsd.gauge(
        "wildfire.wind_speed",
        wind
    )

    statsd.gauge(
        "wildfire.risk_score",
        risk
    )

    # ------------------------
    # Alerta
    # ------------------------

    if risk >= 70:

        logging.warning(
            f"""
🔥 ALERTA DE INCENDIO

Temperatura: {temperature}
Humedad: {humidity}
Viento: {wind}
Risk Score: {risk}
"""
        )

        statsd.increment(
            "wildfire.alerts"
        )

    # ------------------------
    # Data Lake (MinIO)
    # ------------------------

    record = {
        "temperature": temperature,
        "humidity": humidity,
        "wind_speed": wind,
        "risk": risk,
        "timestamp": data["timestamp"]
    }

    try:

        upload(record)

        logging.info(
            "✅ Registro enviado a MinIO"
        )

    except Exception as e:

        logging.error(
            f"❌ Error subiendo a MinIO: {e}"
        )