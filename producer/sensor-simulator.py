"""
Productor simulado de datos de sensores IoT.
Genera métricas adicionales como presión, radiación solar, calidad del aire, etc.
"""

import json
import time
import os
import logging
import random

from datadog import initialize, statsd
from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_SENSORS", "sensores-iot")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
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
        logging.info("✅ Kafka listo (Sensor Simulator)")
        break
    except Exception as exc:
        logging.warning("Esperando Kafka... %s", exc)
        time.sleep(5)

logging.info("Sensor Simulator iniciado")

while True:
    try:
        # Simular múltiples sensores distribuidos
        sensor_id = random.randint(1, 5)
        
        # Valores realistas con variación
        temperature = 20 + random.uniform(-5, 35)
        humidity = max(0, min(100, 50 + random.gauss(0, 15)))
        pressure = 1013 + random.gauss(0, 10)  # mb
        solar_radiation = max(0, 500 + random.gauss(0, 200))  # W/m²
        air_quality_index = max(0, 50 + random.gauss(0, 30))  # AQI
        co2_level = 400 + random.gauss(0, 50)  # ppm
        
        data = {
            "sensor_id": sensor_id,
            "sensor_type": "environmental",
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "pressure": round(pressure, 2),
            "solar_radiation": round(solar_radiation, 2),
            "air_quality_index": round(air_quality_index, 2),
            "co2_level": round(co2_level, 2),
            "timestamp": time.time(),
            "source": "sensor-simulator"
        }

        producer.send(KAFKA_TOPIC, data)
        statsd.increment("produccion.sensores.enviados")
        statsd.gauge("sensor.temperatura", data["temperature"])
        statsd.gauge("sensor.humedad", data["humidity"])
        statsd.gauge("sensor.indice_calidad_aire", data["air_quality_index"])
        logging.info("Sensor #%d enviado: temp=%.1f°C, humedad=%.1f%%, AQI=%.1f",
                     sensor_id, data["temperature"], data["humidity"], data["air_quality_index"])

    except Exception as exc:
        logging.error("Error obteniendo datos de sensores: %s", exc, exc_info=True)

    time.sleep(POLL_SECONDS)
