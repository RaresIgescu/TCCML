import os
import threading
from fault_injector.latency_injection import LatencyInjector
from fault_injector.region_failure import RegionFailureInjector

class FaultRouter:
    def __init__(self, spark, storage_account_name):
        self.mode = os.getenv("FAULT_MODE", "none")

        self.latency = LatencyInjector(base_latency_ms=500, jitter_ms=300)
        self.region_failure = RegionFailureInjector(
            spark=spark,
            storage_account_name=storage_account_name
        )

    def before_write(self):
        if self.mode == "latency":
            self.latency.inject()

        elif self.mode == "region_failure":
            threading.Thread(
                target=self.region_failure.inject_failure,
                kwargs={"duration_sec": 20},
                daemon=True
            ).start()

        elif self.mode == "none":
            pass

        else:
            print(f"[WARN] Unknown FAULT_MODE={self.mode}")
