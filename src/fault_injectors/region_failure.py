# fault_injector/region_failure.py
import time

class RegionFailureInjector:
    def __init__(self, spark, storage_account_name):
        self.spark = spark
        self.storage_account_name = storage_account_name
        self.conf_key = f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net"
        self.original_key = spark.conf.get(self.conf_key, None)

    def inject_failure(self, duration_sec=30):
        print("[FAULT] Injecting REGION FAILURE")

        # Disable access
        self.spark.conf.set(self.conf_key, "INVALID_KEY")

        time.sleep(duration_sec)

        # Restore access
        if self.original_key:
            self.spark.conf.set(self.conf_key, self.original_key)

        print("[FAULT] Region recovered")
