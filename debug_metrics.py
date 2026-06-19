#!/usr/bin/env python3
"""
Script de diagnóstico para verificar la métrica incendio.riesgo.puntaje
"""

import os
import json
import boto3
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configuración
MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
PLATA_BUCKET = os.getenv("PLATA_BUCKET", "plata")
ORO_BUCKET = os.getenv("ORO_BUCKET", "oro")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def check_bucket_contents(bucket_name, limit=5):
    """Verifica contenido de un bucket."""
    logger.info(f"📦 Revisando bucket: {bucket_name}")
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, MaxKeys=limit)
        
        if "Contents" not in response:
            logger.warning(f"   ❌ Bucket {bucket_name} está vacío")
            return 0
        
        count = 0
        for obj in response["Contents"]:
            key = obj["Key"]
            count += 1
            logger.info(f"   ✅ {key} ({obj['Size']} bytes)")
            
            # Mostrar contenido si es JSON
            if key.endswith(".json"):
                try:
                    file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                    data = json.loads(file_obj["Body"].read())
                    
                    if "risk_score" in data:
                        logger.info(f"      📊 risk_score: {data['risk_score']}")
                    if "risk_level" in data:
                        logger.info(f"      🚨 risk_level: {data['risk_level']}")
                except Exception as e:
                    logger.error(f"      ❌ Error leyendo {key}: {e}")
        
        return count
    
    except Exception as exc:
        logger.error(f"❌ Error listando {bucket_name}: {exc}", exc_info=True)
        return -1

def main():
    logger.info("🔍 Iniciando diagnóstico de métricas...")
    logger.info(f"   MinIO: {MINIO_ENDPOINT_URL}")
    logger.info(f"   PLATA_BUCKET: {PLATA_BUCKET}")
    logger.info(f"   ORO_BUCKET: {ORO_BUCKET}")
    
    plata_count = check_bucket_contents(PLATA_BUCKET)
    oro_count = check_bucket_contents(ORO_BUCKET)
    
    logger.info(f"\n📊 Resumen:")
    logger.info(f"   - Objetos en {PLATA_BUCKET}: {plata_count}")
    logger.info(f"   - Objetos en {ORO_BUCKET}: {oro_count}")
    
    if plata_count == 0:
        logger.warning("\n⚠️  No hay datos en PLATA_BUCKET")
        logger.warning("   → Verifica que el consumer esté corriendo")
        logger.warning("   → Verifica que el productor esté enviando eventos")
    
    if oro_count == 0:
        logger.warning("\n⚠️  No hay datos en ORO_BUCKET")
        logger.warning("   → Verifica que el gold runner esté corriendo")
        logger.warning("   → Verifica que el modelo ML esté disponible")
    
    if oro_count > 0:
        logger.info("\n✅ Hay datos enriquecidos. La métrica debería estar llegando a Datadog")

if __name__ == "__main__":
    main()
