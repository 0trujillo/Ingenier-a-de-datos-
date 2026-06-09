import json
import time
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="admin",
    aws_secret_access_key="admin123"
)

# Crear bucket plata
try:
    s3.head_bucket(Bucket="plata")
except:
    s3.create_bucket(Bucket="plata")


def process_record(data):

    temp = data["temperature"]
    humidity = data["humidity"]
    wind = data["wind_speed"]

    # Validaciones básicas
    if temp < -50 or temp > 60:
        return None

    if humidity < 0 or humidity > 100:
        return None

    if wind < 0:
        return None

    # Transformación
    heat_index = temp + (wind * 0.2)

    return {
        "temperature": temp,
        "humidity": humidity,
        "wind_speed": wind,
        "heat_index": round(heat_index, 2),
        "timestamp": data["timestamp"],
        "processed_at": time.time()
    }


def upload_to_silver(record):

    s3.put_object(
        Bucket="plata",
        Key=f"silver_{int(time.time()*1000)}.json",
        Body=json.dumps(record)
    )