from kafka import KafkaConsumer
import json
import logging
from datadog import initialize, statsd

# Datadog config
initialize(api_key="4e040f2fee6f2337ce4a56646eccb1fc")

logging.basicConfig(level=logging.INFO)

consumer = KafkaConsumer(
    "sensores",
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for msg in consumer:
    data = msg.value
    temp = data["temp"]

    logging.info(f"Dato recibido: {data}")

    # métrica
    statsd.gauge('sensor.temperatura', temp)

    if temp > 70:
        logging.warning(f"🔥 Evento crítico: {data}")