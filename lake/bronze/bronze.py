import boto3
import json
import logging
import os
import time

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

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


ensure_bucket(BRONZE_BUCKET)


def upload(data):
    ts = int(time.time() * 1000)
    event_id = data.get("event_id")
    if event_id:
        key = f"bronze_{event_id}_{ts}.json"
    else:
        key = f"bronze_{ts}.json"
    try:
        s3.put_object(
            Bucket=BRONZE_BUCKET,
            Key=key,
            Body=json.dumps(data)
        )
        logging.info("✅ Bronze upload successful: %s (event_id=%s)", key, event_id)
    except Exception as exc:
        logging.error("❌ Error al subir objeto a Bronze: %s", exc, exc_info=True)
        raise