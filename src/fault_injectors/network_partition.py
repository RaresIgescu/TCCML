import time
import random

class NetworkPartitionInjector:
    """
    Simulates a network partition by intermittently
    enabling and disabling access to ADLS Gen2.
    """

    def __init__(self, spark, storage_account_name, partition_probability=0.5):
        self.spark = spark
        self.storage_account_name = storage_account_name
        self.partition_probability = partition_probability

        self.conf_key = (
            f"spark.hadoop.fs.azure.account.key."
            f"{storage_account_name}.dfs.core.windows.net"
        )

        self.original_key = spark.conf.get(self.conf_key, None)

    def inject(self, duration_sec=30, interval_sec=3):
        """
        Simulates intermittent network connectivity.

        :param duration_sec: total duration of the partition simulation
        :param interval_sec: interval at which connectivity changes
        """

        print("[FAULT] Network partition simulation started")

        start_time = time.time()

        while time.time() - start_time < duration_sec:
            partitioned = random.random() < self.partition_probability

            if partitioned:
                # Simulate network cut
                print("[FAULT] Network PARTITIONED (storage unreachable)")
                self.spark.conf.set(self.conf_key, "INVALID_KEY")
            else:
                # Restore network
                print("[FAULT] Network CONNECTED (storage reachable)")
                if self.original_key:
                    self.spark.conf.set(self.conf_key, self.original_key)

            time.sleep(interval_sec)

        # Ensure network is restored at the end
        if self.original_key:
            self.spark.conf.set(self.conf_key, self.original_key)

        print("[FAULT] Network partition simulation ended – connectivity restored")
