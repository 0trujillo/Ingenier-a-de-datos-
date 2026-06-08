import json
import time
import requests

from kafka import KafkaProducer


while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

        print("✅ Kafka listo")
        break

    except Exception as e:
        print("Esperando Kafka...", e)
        time.sleep(5)

LAT = -33.45
LON = -70.66

while True:

    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": LAT,
            "longitude": LON,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m"
        }
    )

    current = response.json()["current"]

    data = {
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "timestamp": time.time()
    }

    producer.send("sensores", data)

    print("Enviado:", data)

    time.sleep(300)