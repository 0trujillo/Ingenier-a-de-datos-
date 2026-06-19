from kafka import KafkaConsumer
import json
import logging
import time
import sys
import os
import uuid

from datadog import initialize, statsd

sys.path.append(
    os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))
    )
)

from lake.bronze.bronze import upload

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))

initialize(
    statsd_host=DATADOG_HOST,
    statsd_port=DATADOG_PORT
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# ------------------------
# Esperar Kafka
# ------------------------

while True:
    try:

        consumer = KafkaConsumer(
            "sensores",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8"))
        )

        logging.info("✅ Conectado a Kafka")
        break

    except Exception as e:
        logging.warning("⏳ Esperando Kafka: %s", e)

        time.sleep(5)


# ------------------------
# Consumo de eventos
# ------------------------

for msg in consumer:

    data = msg.value

    # Asegurar trazabilidad mínima
    if not data.get("event_id"):
        data["event_id"] = uuid.uuid4().hex

    # Añadir metadata de ingestión
    data["kafka_topic"] = msg.topic
    data["received_at"] = time.time()

    try:
        upload(data)
        statsd.increment("bronze.registros.recibidos")
        logging.info("✅ Registro enviado a Bronze: event_id=%s", data.get("event_id"))
    except Exception as exc:
        statsd.increment("bronze.registros.fallidos")
        logging.error("❌ Error subiendo a Bronze event_id=%s: %s", data.get("event_id"), exc, exc_info=True)