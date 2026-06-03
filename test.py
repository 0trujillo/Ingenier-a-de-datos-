from datadog import initialize
from datadog.dogstatsd import DogStatsd
import time

# 👇 IMPORTANTE: usar host explícito
statsd = DogStatsd(host="127.0.0.1", port=8125)

for i in range(20):
    statsd.gauge('test.metric', i)
    print("enviado", i)
    time.sleep(1)