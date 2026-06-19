#!/usr/bin/env python3
"""
Diagnóstico completo de Datadog y pipeline
"""

import os
import json
import boto3
import socket
import logging
from datetime import datetime
from datadog import initialize, statsd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Config
DD_API_KEY = os.getenv("DD_API_KEY", "")
DATADOG_HOST = os.getenv("DATADOG_HOST", "datadog")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
ORO_BUCKET = os.getenv("ORO_BUCKET", "oro")

def check_datadog_connectivity():
    """Verifica conexión a Datadog agent (DogStatsD)"""
    logger.info("\n=== 🔗 DATADOG CONNECTIVITY ===")
    logger.info(f"Datadog Host: {DATADOG_HOST}")
    logger.info(f"Datadog Port: {DATADOG_PORT}")
    logger.info(f"API Key configured: {'✅' if DD_API_KEY else '❌'}")
    
    # Intentar conectar al socket UDP
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2)
        sock.sendto(b"test", (DATADOG_HOST, DATADOG_PORT))
        sock.close()
        logger.info(f"✅ Conexión UDP a {DATADOG_HOST}:{DATADOG_PORT} OK")
        return True
    except Exception as exc:
        logger.error(f"❌ No se puede conectar a {DATADOG_HOST}:{DATADOG_PORT}: {exc}")
        return False

def test_metric_send():
    """Intenta enviar una métrica de prueba"""
    logger.info("\n=== 📤 TEST METRIC SEND ===")
    
    try:
        initialize(statsd_host=DATADOG_HOST, statsd_port=DATADOG_PORT)
        
        # Enviar métrica de prueba
        test_value = 42.5
        statsd.gauge("test.diagnostic.metric", test_value)
        logger.info(f"✅ Métrica de prueba enviada: test.diagnostic.metric = {test_value}")
        
        # Enviar métrica simulada de riesgo
        risk_score = 75.3
        statsd.gauge("incendio.riesgo.puntaje", risk_score)
        logger.info(f"✅ Métrica simulada enviada: incendio.riesgo.puntaje = {risk_score}")
        
        return True
    except Exception as exc:
        logger.error(f"❌ Error enviando métrica: {exc}", exc_info=True)
        return False

def check_minio_data():
    """Verifica si hay datos en MinIO"""
    logger.info("\n=== 📦 MINIO DATA CHECK ===")
    
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
        
        # Listar buckets
        response = s3.list_buckets()
        buckets = [b["Name"] for b in response.get("Buckets", [])]
        logger.info(f"✅ Buckets disponibles: {buckets}")
        
        # Verificar ORO_BUCKET
        try:
            response = s3.list_objects_v2(Bucket=ORO_BUCKET, MaxKeys=5)
            
            if "Contents" in response:
                objects = response["Contents"]
                logger.info(f"✅ Encontrados {len(objects)} objetos en {ORO_BUCKET}:")
                for obj in objects:
                    logger.info(f"   - {obj['Key']}")
                    
                    # Leer el primero para ver risk_score
                    if obj["Key"].endswith(".json"):
                        file_obj = s3.get_object(Bucket=ORO_BUCKET, Key=obj["Key"])
                        data = json.loads(file_obj["Body"].read())
                        if "risk_score" in data:
                            logger.info(f"     📊 risk_score: {data['risk_score']}")
                        break
            else:
                logger.warning(f"⚠️  {ORO_BUCKET} está vacío - no hay datos enriquecidos")
                
        except Exception as exc:
            logger.error(f"❌ Error accediendo a {ORO_BUCKET}: {exc}")
    
    except Exception as exc:
        logger.error(f"❌ Error conectando a MinIO: {exc}", exc_info=True)

def print_summary():
    """Imprime resumen de configuración"""
    logger.info("\n=== ⚙️  CONFIGURATION ===")
    logger.info(f"DD_API_KEY: {DD_API_KEY[:20]}..." if DD_API_KEY else "DD_API_KEY: NOT SET")
    logger.info(f"DATADOG_HOST: {DATADOG_HOST}")
    logger.info(f"DATADOG_PORT: {DATADOG_PORT}")
    logger.info(f"MINIO_ENDPOINT_URL: {MINIO_ENDPOINT_URL}")
    logger.info(f"ORO_BUCKET: {ORO_BUCKET}")

def main():
    logger.info("=" * 60)
    logger.info("🔍 DIAGNÓSTICO DATADOG Y PIPELINE")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("=" * 60)
    
    print_summary()
    
    connected = check_datadog_connectivity()
    check_minio_data()
    
    if connected:
        test_metric_send()
        logger.info("\n💡 Tip: Ve a Datadog > Metrics > Metrics Explorer")
        logger.info("   y busca 'incendio.riesgo.puntaje' o 'test.diagnostic.metric'")
    else:
        logger.error("\n⚠️  No hay conexión con Datadog agent")
        logger.error("   Verifica que el contenedor 'datadog' esté corriendo:")
        logger.error("   docker ps | grep datadog")
        logger.error("   docker logs datadog")

if __name__ == "__main__":
    main()
