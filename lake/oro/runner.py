import json
import time
import logging
import os
import sys
import boto3

from datadog import initialize, statsd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from lake.oro.processor import enrich

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
PLATA_BUCKET = os.getenv("PLATA_BUCKET", "plata")
ORO_BUCKET = os.getenv("ORO_BUCKET", "oro")
PROCESSED_BUCKET = os.getenv("PROCESSED_BUCKET", "oro-processed")
DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

logger.info(f"Inicializando DogStatsD: {DATADOG_HOST}:{DATADOG_PORT}")
initialize(
    statsd_host=DATADOG_HOST,
    statsd_port=DATADOG_PORT
)

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


def processed_key(key):
    return f"processed/{key}"


def is_processed(key):
    try:
        s3.head_object(Bucket=PROCESSED_BUCKET, Key=processed_key(key))
        return True
    except Exception:
        return False


def mark_processed(key):
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=processed_key(key), Body=b"")


def list_all_objects(bucket_name):
    """Generador que permite iterar sobre todos los objetos con paginación automática."""
    continuation_token = None
    
    while True:
        list_kwargs = {"Bucket": bucket_name}
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token
        
        response = s3.list_objects_v2(**list_kwargs)
        
        if "Contents" not in response:
            break
        
        for obj in response["Contents"]:
            yield obj
        
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break


ensure_bucket(PLATA_BUCKET)
ensure_bucket(ORO_BUCKET)
ensure_bucket(PROCESSED_BUCKET)

logging.info("Gold Runner iniciado")

while True:
    try:
        has_items = False
        
        for obj in list_all_objects(PLATA_BUCKET):
            key = obj["Key"]
            has_items = True

            if is_processed(key):
                continue

            try:
                file_obj = s3.get_object(Bucket=PLATA_BUCKET, Key=key)
                data = json.loads(file_obj["Body"].read())
            except Exception as exc:
                logging.error("Error leyendo objeto %s: %s", key, exc, exc_info=True)
                mark_processed(key)
                continue

            try:
                data = enrich(data)
                logger.info("✅ Enriquecido -> %s", key)
                statsd.increment("oro.registros.enriquecidos")
                
                # Enviar métrica de riesgo usando el registro enriquecido devuelto
                risk_score = data.get("risk_score")
                if risk_score is not None:
                    try:
                        event_id = data.get("event_id") or data.get("id") or "unknown"
                        risk_level = data.get("risk_level", "UNKNOWN")
                        statsd.gauge(
                            "incendio.riesgo.puntaje",
                            float(risk_score),
                            tags=[f"nivel:{risk_level}", f"event_id:{event_id}"]
                        )
                        logger.info(
                            "📊 Métrica enviada: incendio.riesgo.puntaje=%s nivel=%s event_id=%s",
                            risk_score,
                            risk_level,
                            event_id
                        )
                    except Exception as exc:
                        logger.error(f"❌ Error enviando métrica: {exc}", exc_info=True)
                else:
                    logger.warning("⚠️  risk_score no disponible en datos")
                
                mark_processed(key)
            except Exception as exc:
                logger.warning("⚠️  Fallo al enriquecer %s, se reintentará más tarde: %s", key, exc)
                statsd.increment("oro.registros.fallidos")

        if not has_items:
            logging.debug("No hay nuevos registros en Silver")
            time.sleep(5)
        else:
            time.sleep(1)
            
    except Exception as exc:
        logging.error("Error en Gold Runner: %s", exc, exc_info=True)
        time.sleep(10)