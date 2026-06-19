import os
import time
from datadog import initialize, statsd

DATADOG_HOST = os.getenv("DATADOG_HOST", "localhost")
DATADOG_PORT = int(os.getenv("DATADOG_PORT", "8125"))
api_key = os.getenv("DD_API_KEY")
if not api_key:
    raise RuntimeError("DD_API_KEY no está definido en el entorno")

initialize(
    api_key=api_key,
    statsd_host=DATADOG_HOST,
    statsd_port=DATADOG_PORT,
)

print("Enviando métricas de prueba...")

for i in range(10):
    temperature = 20 + i * 2
    humidity = 15 + i * 3
    wind_speed = 5 + i * 1.5
    risk_score = round(min(100, max(0, 30 + i * 7)), 2)
    if risk_score >= 75:
        risk_level = "ALTO"
    elif risk_score >= 50:
        risk_level = "MEDIO"
    else:
        risk_level = "NORMAL"
    event_id = f"test-{i+1}"

    statsd.gauge("incendio.temperatura", temperature, tags=[f"event_id:{event_id}"])
    statsd.gauge("incendio.humedad", humidity, tags=[f"event_id:{event_id}"])
    statsd.gauge("incendio.velocidad_viento", wind_speed, tags=[f"event_id:{event_id}"])
    statsd.gauge(
        "incendio.riesgo.puntaje",
        risk_score,
        tags=[f"nivel:{risk_level}", f"event_id:{event_id}"]
    )

    statsd.increment("incendio.alertas", tags=[f"nivel:{risk_level}"])

    print(
        f"Prueba #{i+1}: score={risk_score}, nivel={risk_level}, "
        f"temp={temperature:.1f}, hum={humidity:.1f}, wind={wind_speed:.1f}"
    )

    time.sleep(2)

print("Finalizado")