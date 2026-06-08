# Ingenieria-de-datos-
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

instalar dependencias: pip install -r requirements.txt
Levantar docker: docker-compose up -d
docker logs ingenier-a-de-datos--datadog-1
visualizacion: sensor.temperatura

| Valor | Significado    |
| ----- | -------------- |
| 25    | normal ✅       |
| 50    | carga media ⚠️ |
| 80    | caliente 🔥    |
| 95    | crítico 🚨     |

Open-Meteo API
      ↓
AWS Lambda
      ↓
Amazon S3
      ↓
AWS Glue Catalog
      ↓
Amazon Athena
      ↓
Data Warehouse
      ↓
Dashboard React