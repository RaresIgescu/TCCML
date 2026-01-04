import os
import threading

from fault_injectors.latency_injection import LatencyInjector
from fault_injectors.region_failure import RegionFailureInjector
from fault_injectors.network_partition import NetworkPartitionInjector


class FaultRouter:
    def __init__(self, spark, storage_account_name):
        self.mode = os.getenv("FAULT_MODE", "none")

        self.latency = LatencyInjector(
            base_latency_ms=500,
            jitter_ms=300
        )

        self.region_failure = RegionFailureInjector(
            spark=spark,
            storage_account_name=storage_account_name
        )

        self.network_partition = NetworkPartitionInjector(
            spark=spark,
            storage_account_name=storage_account_name,
            partition_probability=0.6
        )

    def before_write(self):
        """
        Activates the selected fault mode before a Delta Lake write.
        """
        if self.mode == "latency":
            print("[FAULT ROUTER] Latency Injection selected")
            self.latency.inject()

        elif self.mode == "region_failure":
            print("[FAULT ROUTER] Region Failure selected")
            threading.Thread(
                target=self.region_failure.inject_failure,
                kwargs={"duration_sec": 30},
                daemon=True
            ).start()

        elif self.mode == "network_partition":
            print("[FAULT ROUTER] Network Partition selected")
            threading.Thread(
                target=self.network_partition.inject,
                kwargs={
                    "duration_sec": 30,
                    "interval_sec": 3
                },
                daemon=True
            ).start()

        elif self.mode == "none":
            print("[FAULT ROUTER] No fault injection (baseline)")

        else:
            print(f"[WARN] Unknown FAULT_MODE={self.mode}")
