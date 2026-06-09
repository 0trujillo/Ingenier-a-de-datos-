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

from lake.bronze.bronze import upload


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

    try:

        upload(data)

        logging.info(
            "✅ Registro enviado a Bronze"
        )

    except Exception as e:

        logging.error(
            f"❌ Error subiendo a Bronze: {e}"
        )