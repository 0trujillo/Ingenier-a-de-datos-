import json, time, random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "temp": random.uniform(20, 100),
        "timestamp": time.time()
    }

    print("Enviando:", data)
    producer.send("sensores", data)

    time.sleep(1)