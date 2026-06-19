import json
import time
import os
import logging
import boto3

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
PLATA_BUCKET = os.getenv("PLATA_BUCKET", "plata")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def ensure_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception as exc:
        logging.warning("Bucket %s no existe, creando... %s", bucket_name, exc)
        s3.create_bucket(Bucket=bucket_name)


ensure_bucket(PLATA_BUCKET)


def process_record(data):
    temp = data.get("temperature")
    humidity = data.get("humidity")
    wind = data.get("wind_speed")

    if temp is None or humidity is None or wind is None:
        return None

    if temp < -50 or temp > 60:
        return None

    if humidity < 0 or humidity > 100:
        return None

    if wind < 0:
        return None

    heat_index = temp + (wind * 0.2)
    # Mantener trazabilidad
    event_id = data.get("event_id") or data.get("id")
    source_topic = data.get("source_topic") or data.get("kafka_topic") or data.get("source")

    return {
        "event_id": event_id,
        "source_topic": source_topic,
        "temperature": temp,
        "humidity": humidity,
        "wind_speed": wind,
        "heat_index": round(heat_index, 2),
        "timestamp": data.get("timestamp"),
        "processed_at": time.time()
    }


def upload_to_silver(record):
    key = f"silver_{int(time.time() * 1000)}.json"
    try:
        s3.put_object(
            Bucket=PLATA_BUCKET,
            Key=key,
            Body=json.dumps(record)
        )
        logging.info("✅ Registro Silver subido: %s", key)
    except Exception as exc:
        logging.error("❌ Error subiendo a Silver: %s", exc, exc_info=True)
        raise