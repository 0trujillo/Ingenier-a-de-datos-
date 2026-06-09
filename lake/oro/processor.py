import json
import os
import time
import logging
import requests
import boto3

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
MODEL_API_URL = os.getenv("MODEL_API_URL", "http://ml-api:8000/predict")
ORO_BUCKET = os.getenv("ORO_BUCKET", "oro")

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


ensure_bucket(ORO_BUCKET)


def enrich(record):
    payload = {
        "temperature": record.get("temperature"),
        "humidity": record.get("humidity"),
        "wind_speed": record.get("wind_speed")
    }

    try:
        response = requests.post(MODEL_API_URL, json=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        body = getattr(exc.response, "text", None) if hasattr(exc, "response") else None
        logging.error(
            "❌ Error llamando a ML API %s: %s - response=%s",
            MODEL_API_URL,
            exc,
            body,
            exc_info=True
        )
        raise

    try:
        prediction = response.json()
    except ValueError as exc:
        logging.error("Respuesta inválida de ML API: %s", exc, exc_info=True)
        raise

    final_record = {
        **record,
        "risk_score": prediction.get("risk_score"),
        "risk_level": prediction.get("risk_level")
    }

    try:
        s3.put_object(
            Bucket=ORO_BUCKET,
            Key=f"gold_{int(time.time() * 1000)}.json",
            Body=json.dumps(final_record)
        )
        logging.info("✅ Registro Oro guardado en %s", ORO_BUCKET)
    except Exception as exc:
        logging.error("❌ Error guardando Oro: %s", exc, exc_info=True)
        raise

    return final_record