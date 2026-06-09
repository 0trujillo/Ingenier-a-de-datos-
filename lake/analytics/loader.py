"""
Analytics Loader - Carga datos del oro bucket a DuckDB para análisis.
Crear esta carpeta y archivo:
"""

import json
import time
import logging
import os
import sys
import boto3
import duckdb
from pathlib import Path


MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")
ORO_BUCKET = os.getenv("ORO_BUCKET", "oro")
ANALYTICS_BUCKET = os.getenv("ANALYTICS_BUCKET", "analytics-processed")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/analytics.duckdb")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)


def ensure_bucket(bucket_name):
    """Asegura que el bucket existe."""
    try:
        s3.head_bucket(Bucket=bucket_name)
    except Exception:
        logger.info("Creando bucket: %s", bucket_name)
        s3.create_bucket(Bucket=bucket_name)


def processed_key(key):
    """Genera la key de marca de procesado."""
    return f"processed/{key}"


def is_processed(key):
    """Verifica si ya fue procesado."""
    try:
        s3.head_object(Bucket=ANALYTICS_BUCKET, Key=processed_key(key))
        return True
    except Exception:
        return False


def mark_processed(key):
    """Marca como procesado."""
    s3.put_object(Bucket=ANALYTICS_BUCKET, Key=processed_key(key), Body=b"")


def list_all_objects(bucket_name):
    """Generador para listar objetos con paginación."""
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


def init_duckdb():
    """Inicializa la base de datos DuckDB."""
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)
    
    db = duckdb.connect(DUCKDB_PATH)
    
    # Crear tabla principal
    db.execute("""
        CREATE TABLE IF NOT EXISTS enriched_events (
            id VARCHAR,
            temperature FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            heat_index FLOAT,
            risk_score FLOAT,
            risk_level VARCHAR,
            processed_at TIMESTAMP,
            enriched_at TIMESTAMP,
            PRIMARY KEY(id)
        )
    """)
    
    # Crear tabla de estadísticas
    db.execute("""
        CREATE TABLE IF NOT EXISTS hourly_stats (
            hour TIMESTAMP,
            avg_temperature FLOAT,
            avg_humidity FLOAT,
            avg_wind_speed FLOAT,
            avg_risk_score FLOAT,
            max_risk_score FLOAT,
            high_risk_count INTEGER,
            record_count INTEGER,
            PRIMARY KEY(hour)
        )
    """)
    
    db.execute("CREATE INDEX IF NOT EXISTS idx_enriched_risk ON enriched_events(risk_score)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_enriched_time ON enriched_events(processed_at)")
    
    logger.info("✅ DuckDB inicializado")
    db.close()


def load_to_duckdb(data):
    """Carga un registro a DuckDB."""
    try:
        db = duckdb.connect(DUCKDB_PATH)
        
        db.execute("""
            INSERT INTO enriched_events 
            (id, temperature, humidity, wind_speed, heat_index, 
             risk_score, risk_level, processed_at, enriched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            data.get("id", f"event_{int(data.get('processed_at', 0))}"),
            data.get("temperature"),
            data.get("humidity"),
            data.get("wind_speed"),
            data.get("heat_index"),
            data.get("risk_score"),
            data.get("risk_level"),
            data.get("processed_at"),
            data.get("enriched_at")
        ])
        
        db.close()
        return True
    except Exception as exc:
        logger.error("Error cargando a DuckDB: %s", exc, exc_info=True)
        return False


def compute_hourly_stats():
    """Calcula estadísticas horarias."""
    db = duckdb.connect(DUCKDB_PATH)
    
    try:
        # Eliminar stats antiguas
        db.execute("DELETE FROM hourly_stats")
        
        # Insertar nuevas stats
        db.execute("""
            INSERT INTO hourly_stats
            SELECT
                date_trunc('hour', processed_at) as hour,
                AVG(temperature),
                AVG(humidity),
                AVG(wind_speed),
                AVG(risk_score),
                MAX(risk_score),
                COUNT_IF(risk_level = 'ALTO'),
                COUNT(*)
            FROM enriched_events
            GROUP BY date_trunc('hour', processed_at)
            ORDER BY hour DESC
        """)
        
        logger.info("✅ Stats horarias calculadas")
    except Exception as exc:
        logger.error("Error calculando stats: %s", exc, exc_info=True)
    finally:
        db.close()


def get_stats():
    """Obtiene estadísticas de la base de datos."""
    db = duckdb.connect(DUCKDB_PATH)
    
    try:
        # Total de eventos
        total = db.execute(
            "SELECT COUNT(*) FROM enriched_events"
        ).fetchone()[0]
        
        # Riesgo promedio
        avg_risk = db.execute(
            "SELECT AVG(risk_score) FROM enriched_events"
        ).fetchone()[0]
        
        # Eventos de alto riesgo
        high_risk = db.execute(
            "SELECT COUNT(*) FROM enriched_events WHERE risk_level = 'ALTO'"
        ).fetchone()[0]
        
        return {
            "total_events": total,
            "avg_risk_score": round(avg_risk or 0, 2),
            "high_risk_events": high_risk
        }
    except Exception as exc:
        logger.error("Error obteniendo stats: %s", exc, exc_info=True)
        return {}
    finally:
        db.close()


# Main loop
ensure_bucket(ORO_BUCKET)
ensure_bucket(ANALYTICS_BUCKET)
init_duckdb()

logger.info("🚀 Analytics Loader iniciado")

while True:
    try:
        has_items = False
        
        for obj in list_all_objects(ORO_BUCKET):
            key = obj["Key"]
            
            if is_processed(key):
                continue
            
            has_items = True
            
            try:
                file_obj = s3.get_object(Bucket=ORO_BUCKET, Key=key)
                data = json.loads(file_obj["Body"].read())
                
                if load_to_duckdb(data):
                    logger.info("✅ Cargado a DuckDB: %s", key)
                    mark_processed(key)
            except Exception as exc:
                logger.error("Error procesando %s: %s", key, exc)
                mark_processed(key)
        
        # Calcular stats cada 60 segundos
        compute_hourly_stats()
        stats = get_stats()
        logger.info("📊 Stats: %s", stats)
        
        if not has_items:
            logger.debug("No hay nuevos registros")
            time.sleep(30)
        else:
            time.sleep(5)
            
    except Exception as exc:
        logger.error("Error en Analytics Loader: %s", exc, exc_info=True)
        time.sleep(10)
