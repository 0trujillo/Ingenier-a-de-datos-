from datadog import initialize, statsd
import time

initialize(
    api_key="4e040f2fee6f2337ce4a56646eccb1fc"
)

print("Enviando métricas de prueba...")

for i in range(10):

    statsd.gauge("wildfire.temperature", 40)
    statsd.gauge("wildfire.humidity", 10)
    statsd.gauge("wildfire.wind_speed", 30)
    statsd.gauge("wildfire.risk_score", 100)

    statsd.increment("wildfire.alerts")

    print(f"Prueba #{i+1}")

    time.sleep(2)

print("Finalizado")