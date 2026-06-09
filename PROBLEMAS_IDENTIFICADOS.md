# Problemas y oportunidades de mejora identificados

Durante la revisión de la arquitectura y del código fuente se identificaron algunos aspectos que requieren ajustes para mejorar la robustez, mantenibilidad y comportamiento del sistema en un entorno más cercano a producción.

---

## 1. Generación y carga del modelo de Machine Learning

Actualmente, la API de Machine Learning carga el archivo `model.pkl` al iniciar:

```python
model = joblib.load(MODEL_PATH)
```

Sin embargo, el contenedor de la API no genera automáticamente dicho archivo durante el proceso de construcción. Esto puede provocar errores de inicialización si el modelo no existe previamente en el entorno de ejecución.

**Recomendación:** Incorporar la ejecución del script `train.py` durante el proceso de build del contenedor o incluir el modelo previamente generado dentro de la imagen Docker.

**Solución propuesta:**
```dockerfile
# ml-api/Dockerfile
FROM python:3.11
WORKDIR /app
COPY . .
RUN pip install -r requirements.txt
RUN python ml-api/train.py
CMD ["uvicorn", "ml-api.app:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## 2. Importaciones dependientes de la ubicación de ejecución

Los módulos de las capas Silver y Gold utilizan importaciones relativas como:

```python
from limpieza import process_record
from processor import enrich
```

Este enfoque puede generar errores dependiendo del directorio desde el cual se ejecute el contenedor o el intérprete de Python.

**Recomendación:** Utilizar importaciones absolutas dentro del proyecto y convertir los directorios correspondientes en paquetes Python mediante archivos `__init__.py`.

**Solución propuesta:**

En `lake/plata/runner.py`:
```python
from lake.plata.limpieza import process_record, upload_to_silver
```

En `lake/oro/runner.py`:
```python
from lake.oro.processor import enrich
```

---

## 3. Obtención de datos meteorológicos desde Open-Meteo

La implementación actual obtiene la temperatura y velocidad del viento desde el bloque `current_weather`, mientras que la humedad relativa se obtiene desde el bloque `hourly`.

```python
current = payload.get("current_weather", {})
hourly = payload.get("hourly", {})
humidity_values = hourly.get("relativehumidity_2m", [])
humidity = humidity_values[-1] if humidity_values else None
```

Esto puede provocar inconsistencias temporales, ya que la humedad utilizada podría no corresponder exactamente al mismo instante de tiempo que el resto de las variables meteorológicas.

**Recomendación:** Utilizar la API actual de Open-Meteo mediante el parámetro `current` (no `current_weather`), obteniendo todas las variables meteorológicas desde una misma fuente temporal.

**Solución propuesta:**
```python
response = requests.get(
    "https://api.open-meteo.com/v1/forecast",
    params={
        "latitude": LAT,
        "longitude": LON,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
        "timezone": "auto"
    },
    timeout=10
)
current = response.json().get("current", {})
data = {
    "temperature": current.get("temperature_2m"),
    "humidity": current.get("relative_humidity_2m"),
    "wind_speed": current.get("wind_speed_10m"),
    "timestamp": time.time()
}
```

---

## 4. Escalabilidad en la lectura de objetos desde MinIO

Los procesos Silver y Gold utilizan la operación:

```python
response = s3.list_objects_v2(Bucket=PLATA_BUCKET)
```

Actualmente no se implementa paginación de resultados. Aunque esto no representa un problema para pruebas o demostraciones, puede convertirse en una limitación cuando el volumen de archivos almacenados supera los 1.000 objetos.

**Recomendación:** Implementar soporte para `ContinuationToken` y paginación de resultados.

**Solución propuesta:**
```python
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
```

Luego utilizar:
```python
for obj in list_all_objects(BRONZE_BUCKET):
    # procesar objeto
```

---

## 5. Observabilidad limitada del pipeline

El proyecto incorpora Datadog como solución de monitoreo; sin embargo, actualmente se utilizan principalmente logs y no se aprovechan métricas operacionales que permitan medir el comportamiento del pipeline.

Esto dificulta el monitoreo de:

- Cantidad de registros procesados
- Velocidad de procesamiento
- Número de errores por etapa
- Volumen de alertas generadas
- Riesgo promedio detectado por el modelo

**Recomendación:** Incorporar métricas personalizadas utilizando DogStatsD.

**Solución propuesta:**
```python
from datadog import initialize, statsd
import os

initialize(
    api_key=os.getenv("DD_API_KEY"),
    app_key=os.getenv("DD_APP_KEY")
)

# En consumer/consumer.py
statsd.increment("bronze.records.received")
statsd.gauge("bronze.processing_time", elapsed_ms)

# En lake/plata/runner.py
statsd.increment("silver.records.processed")
statsd.gauge("silver.quality_score", quality)

# En lake/oro/runner.py
statsd.increment("gold.records.enriched")
statsd.gauge("wildfire.risk_score", risk_score)
statsd.increment("wildfire.alerts", sample_rate=0.1)
```

---

## 6. Capa analítica aún no implementada

La arquitectura propuesta contempla una capa de análisis compuesta por DuckDB y Metabase:

```
Gold (MinIO)
     ↓
  DuckDB
     ↓
  Metabase
```

Sin embargo, actualmente dicha capa aún no se encuentra implementada dentro del proyecto.

**Recomendación:** Desarrollar un proceso de carga desde la capa Gold hacia DuckDB y posteriormente conectar Metabase para construir dashboards y consultas analíticas.

**Solución propuesta:**
Crear un nuevo servicio `analytics-loader` que:
1. Periodicamente lea archivos desde el bucket `oro`
2. Cargue los datos en una tabla DuckDB
3. Mantenga un índice de registros procesados
4. Exponga métricas agregadas (temperatura promedio, alertas por zona, etc.)

---

## 7. Manejo de estados transitorios en runners

Actualmente, los runners (Silver y Gold) persisten el estado de procesamiento en MinIO mediante marcadores S3. Sin embargo, no existe un mecanismo para:

- Limpiar marcadores antiguos después de N días
- Reintentar procesos que fallan
- Registrar errores persistentes para análisis posterior

**Recomendación:** Implementar una estrategia de reintentos con backoff exponencial y logging estructurado de errores.

**Solución propuesta:**
```python
import logging
from functools import wraps
import time

def retry_with_backoff(max_attempts=3, base_delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_attempts - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    logging.warning("Intento %d/%d fallido. Reintentando en %d segundos...",
                                  attempt + 1, max_attempts, delay)
                    time.sleep(delay)
        return wrapper
    return decorator
```

---

## 8. Falta de validación y tipado en datos

Los datos que fluyen a través del pipeline (Bronze → Silver → Gold) no tienen esquemas formales definidos. Esto puede causar:

- Inconsistencias en los tipos de datos
- Dificultad en la depuración de errores
- Falta de documentación clara sobre la estructura esperada

**Recomendación:** Implementar validación de esquemas mediante Pydantic o JSON Schema.

**Solución propuesta:**
```python
from pydantic import BaseModel, Field, validator
from typing import Optional

class WeatherRecord(BaseModel):
    temperature: float = Field(..., ge=-50, le=60)
    humidity: float = Field(..., ge=0, le=100)
    wind_speed: float = Field(..., ge=0)
    timestamp: float
    
    @validator('temperature')
    def validate_temperature(cls, v):
        if v < -50 or v > 60:
            raise ValueError('Temperatura fuera de rango válido')
        return v

class RichWeatherRecord(WeatherRecord):
    heat_index: float
    processed_at: float
```

---

## 9. Coordinación entre múltiples fuentes de datos

El proyecto ahora contempla múltiples fuentes de datos (Open-Meteo, sensores IoT, video). Sin embargo, no existe:

- Un sistema de ordenamiento temporal de eventos
- Un mecanismo para correlacionar eventos de diferentes fuentes
- Un plan de escalabilidad para manejar múltiples tópicos Kafka

**Recomendación:** Implementar un "event aggregator" que:
1. Consume eventos de múltiples tópicos Kafka
2. Los ordena temporalmente
3. Los enriquece con metadatos de correlación
4. Los envía a la capa Bronze como eventos unificados

---

## 10. Falta de versionamiento en modelos de ML

El modelo de Machine Learning se actualiza mediante `train.py`, pero no existe:

- Control de versiones de modelos
- Registro de cambios o mejoras
- Mecanismo de rollback a versiones anteriores
- Comparativa de performance entre versiones

**Recomendación:** Implementar un sistema de versionamiento y registro de modelos (Model Registry).

**Solución propuesta:**
```python
import os
import json
from datetime import datetime

MODEL_REGISTRY_PATH = os.getenv("MODEL_REGISTRY", "/app/models")

def save_model_version(model, accuracy, precision, recall):
    """Guarda una versión del modelo con metadatos."""
    version = datetime.now().strftime("%Y%m%d_%H%M%S")
    version_path = os.path.join(MODEL_REGISTRY_PATH, f"model_v{version}")
    
    os.makedirs(version_path, exist_ok=True)
    
    joblib.dump(model, os.path.join(version_path, "model.pkl"))
    
    metadata = {
        "version": version,
        "timestamp": datetime.now().isoformat(),
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall
    }
    
    with open(os.path.join(version_path, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)
    
    logging.info("Modelo v%s guardado con accuracy=%.3f", version, accuracy)
    return version_path
```

---

## Conclusión

La arquitectura implementada presenta una estructura sólida basada en el patrón Medallion (Bronze, Silver y Gold), integra procesamiento de eventos mediante Kafka, contempla múltiples fuentes de datos y prevé inferencia de Machine Learning desacoplada mediante una API.

No obstante, existen oportunidades de mejora relacionadas con:

1. **Confiabilidad:** Generación automática del modelo, reintentos con backoff, validación de esquemas
2. **Escalabilidad:** Paginación en MinIO, agregación de múltiples fuentes, versionamiento de modelos
3. **Observabilidad:** Métricas personalizadas, logging estructurado, trazabilidad de eventos
4. **Completitud:** Capa analítica (DuckDB + Metabase), event correlation, model registry

La resolución sistemática de estos puntos permitirá aumentar significativamente la robustez del sistema y acercarlo a estándares utilizados en entornos productivos reales.

---

**Última actualización:** 2026-06-09

**Mantenedores:** Equipo de Data Engineering
