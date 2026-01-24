import time
import random

class LatencyInjector:
    def __init__(self, base_latency_ms=200, jitter_ms=100):
        self.base_latency = base_latency_ms
        self.jitter = jitter_ms

    def inject(self):
        latency = self.base_latency + random.randint(0, self.jitter)
        print(f"[LATENCY] Injecting {latency} ms delay")
        time.sleep(latency / 1000)
