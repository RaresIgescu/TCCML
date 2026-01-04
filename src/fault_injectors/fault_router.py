import os
import time
from fault_injectors.latency_injection import LatencyInjector
from fault_injectors.region_failure import RegionFailureInjector


class FaultRouter:
    def __init__(self, spark, storage_account_name):
        self.mode = os.getenv("FAULT_MODE", "none").lower()
        self.spark = spark

        # Initialize injectors
        self.latency = LatencyInjector()
        self.region = RegionFailureInjector(spark, storage_account_name)

    def inject(self):
        """
        Executes the fault logic synchronously. 
        This guarantees the environment is 'broken' before the write begins.
        """
        if self.mode == "latency":
            self.latency.inject()  # Sleeps for X seconds

        elif self.mode == "region_failure":
            self.region.inject_failure()  # Invalidates the key immediately

        elif self.mode == "network_partition":
            # Logic to change protocol or timeout
            self.spark.conf.set("spark.network.timeout", "1s")

        elif self.mode == "none":
            pass