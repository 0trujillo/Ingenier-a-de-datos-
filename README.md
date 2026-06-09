# Ingenieria-de-datos-

## Mapeo AWS ↔ Local (Docker)

| AWS                   | Local (Docker)           |
| --------------------- | ------------------------ |
| S3 (Bronze/Plata/Oro) | MinIO                    |
| Kinesis               | Kafka                    |
| IoT Core              | Producer Python          |
| Flink                 | Spark Streaming o Python |
| SageMaker             | Scikit-learn + FastAPI   |
| Athena                | Trino / DuckDB           |
| QuickSight            | Metabase / Superset      |
| EventBridge           | Kafka / Python events    |
| SNS                   | Datadog Alerts           |
| CloudWatch            | Datadog                  |

## Configuración rápida

```bash
# Instalar dependencias
pip install -r requirements.txt

# Levantar servicios
docker-compose up -d

# Ver logs de Datadog
docker logs ingenier-a-de-datos--datadog-1
```

## Fuentes de datos disponibles

El sistema actualmente consume datos desde tres fuentes simuladas:

### 1. **Open-Meteo (producer/open-meteo.py)**
- **Tópico Kafka:** `sensores`
- **Interval:** 300s (configurable con `POLL_SECONDS`)
- **Datos:** temperatura, humedad, velocidad del viento
- **Ubicación:** Santiago, Chile (-33.45, -70.66)

### 2. **Sensor Simulator (producer/sensor-simulator.py)**
- **Tópico Kafka:** `sensores-iot`
- **Interval:** 60s (configurable con `POLL_SECONDS_SENSORS`)
- **Datos:** temperatura, humedad, presión, radiación solar, índice de calidad del aire, nivel de CO₂
- **Cantidad:** 5 sensores distribuidos simulados
- **Variación:** Datos realistas con desviación estándar Gaussiana

### 3. **Video Simulator (producer/video-simulator.py)**
- **Tópico Kafka:** `video-stream`
- **Interval:** 30s (configurable con `POLL_SECONDS_VIDEO`)
- **Datos:** metadatos de video, detecciones de movimiento, FPS, tipos de objetos
- **Cantidad:** 4 cámaras en diferentes zonas
- **Características:** Simula personas, vehículos, animales, escombros

## Indicadores de riesgo

| Valor | Significado    |
| ----- | -------------- |
| 25    | normal ✅       |
| 50    | carga media ⚠️ |
| 80    | caliente 🔥    |
| 95    | crítico 🚨     |

## Arquitectura del Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                     FUENTES DE DATOS                          │
├─────────────────────────────────────────────────────────────┤
│  • Open-Meteo (temperatura, humedad, viento)                │
│  • Sensor IoT (presión, radiación, AQI, CO₂)               │
│  • Video Surveillance (movimiento, detecciones, FPS)       │
└───────────────────┬──────────────────────────────────────────┘
                    │
                    ▼
          ┌─────────────────┐
          │    KAFKA        │
          │                 │
          │  sensores       │
          │  sensores-iot   │
          │  video-stream   │
          └────────┬────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
    ┌────────┐          ┌────────┐
    │ BRONZE │          │        │
    │ (MinIO)│          │ LOGS   │
    └───┬────┘          │Datadog │
        │               │        │
        ▼               └────────┘
    ┌────────┐
    │ SILVER │  ← Limpieza, validación, enriquecimiento
    │ (MinIO)│
    └───┬────┘
        │
        ▼
    ┌────────┐
    │ ML API │  ← Inferencia (risk_score, risk_level)
    │(FastAPI)
    └───┬────┘
        │
        ▼
    ┌────────┐
    │  GOLD  │  ← Datos enriquecidos
    │ (MinIO)│
    └───┬────┘
        │
        ▼
    ┌────────┐
    │ DuckDB │  ← Análisis (próximo)
    └───┬────┘
        │
        ▼
    ┌──────────┐
    │ Metabase │  ← Dashboards (próximo)
    └──────────┘
```

## Variables de entorno

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=sensores
KAFKA_TOPIC_SENSORS=sensores-iot
KAFKA_TOPIC_VIDEO=video-stream

# Open-Meteo
LATITUDE=-33.45
LONGITUDE=-70.66
POLL_SECONDS=300

# Sensores IoT
POLL_SECONDS_SENSORS=60

# Video
POLL_SECONDS_VIDEO=30

# MinIO
MINIO_ENDPOINT_URL=http://minio:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=admin123
BRONZE_BUCKET=bronze
PLATA_BUCKET=plata
ORO_BUCKET=oro

# ML API
MODEL_PATH=model.pkl

# Datadog
DD_API_KEY=<your-datadog-api-key>
DD_SITE=us5.datadoghq.com
DATADOG_HOST=datadog
DATADOG_PORT=8125
```

## Monitoreo

Para fortalecer la observabilidad del pipeline se implementó el uso de métricas en Datadog mediante DogStatsD, permitiendo monitorear el comportamiento de cada etapa del flujo de datos en tiempo real. Entre las métricas definidas se encuentran **Temperatura**, **Humedad**, **Velocidad del Viento** y **Puntaje de Riesgo**, las cuales reflejan las condiciones meteorológicas procesadas y el resultado de la inferencia realizada por el modelo de Machine Learning. Adicionalmente, se incorporó la métrica **Cantidad de Alertas**, utilizada para contabilizar eventos de riesgo detectados por el sistema.

### Métricas disponibles en Datadog

- `clima.temperatura` - temperatura actual recibida del productor meteorológico
- `clima.humedad` - humedad relativa actual
- `clima.velocidad_viento` - velocidad del viento

- `sensor.temperatura` - temperatura reportada por los sensores IoT
- `sensor.humedad` - humedad registrada por los sensores IoT
- `sensor.indice_calidad_aire` - índice de calidad del aire capturado por los sensores

- `video.fps` - cuadros por segundo procesados por la fuente de video
- `video.detecciones` - cantidad de objetos o eventos detectados
- `video.confianza_deteccion` - nivel de confianza promedio de las detecciones
- `video.movimiento.detectado` - conteo de eventos de movimiento detectados

- `incendio.riesgo.puntaje` - score de riesgo inferido por el modelo

- `produccion.clima.enviados` - eventos meteorológicos enviados a Kafka
- `produccion.sensores.enviados` - eventos IoT enviados a Kafka
- `produccion.video.enviados` - eventos de video enviados a Kafka

- `bronze.registros.recibidos` - registros recibidos y almacenados en la capa Bronze
- `bronze.registros.fallidos` - errores durante la ingesta en la capa Bronze

- `plata.registros.procesados` - registros procesados y validados en la capa Silver
- `plata.registros.invalidos` - registros rechazados por errores de validación en Silver

- `oro.registros.enriquecidos` - registros enriquecidos con datos agregados y análisis
- `oro.registros.fallidos` - errores ocurridos durante el procesamiento en Gold

- `agregador.eventos.clima` - eventos climáticos procesados por el agregador
- `agregador.eventos.sensores` - eventos de sensores procesados por el agregador
- `agregador.eventos.video` - eventos de video procesados por el agregador

Como mejora futura, se recomienda añadir métricas operacionales adicionales como **errores.pipeline** y mayores métricas de salud general para obtener una visión integral del rendimiento, la calidad del procesamiento y la salud del pipeline de datos.

## Problemas identificados y mejoras

Ver el archivo [PROBLEMAS_IDENTIFICADOS.md](PROBLEMAS_IDENTIFICADOS.md) para una lista detallada de:

1. Generación automática del modelo ML
2. Importaciones absolutas en Python
3. Sincronización temporal en datos
4. Escalabilidad en MinIO
5. Observabilidad mejorada
6. Capa analítica (DuckDB + Metabase)
7. Y más...

---

**Última actualización:** 2026-06-09
