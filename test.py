import os
import time
from datadog import initialize, statsd

api_key = os.getenv("DD_API_KEY")
if not api_key:
    raise RuntimeError("DD_API_KEY no está definido en el entorno")

initialize(
    api_key=api_key
)

print("Enviando métricas de prueba...")

for i in range(10):

    statsd.gauge("incendio.temperatura", 40)
    statsd.gauge("incendio.humedad", 10)
    statsd.gauge("incendio.velocidad_viento", 30)
    statsd.gauge("incendio.riesgo.puntaje", 100)

    statsd.increment("incendio.alertas")

    print(f"Prueba #{i+1}")

    time.sleep(2)

print("Finalizado")