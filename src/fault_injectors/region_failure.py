import time

class RegionFailureInjector:
    """
    Simulates a permanent regional outage.
    """

    def __init__(self, spark, storage_account_name):
        self.spark = spark
        self.storage_account_name = storage_account_name

        self.conf_key = (
            f"spark.hadoop.fs.azure.account.key."
            f"{storage_account_name}.dfs.core.windows.net"
        )

        self.original_key = spark.conf.get(self.conf_key, None)

    def inject_failure(self, duration_sec=30):
        print("[FAULT] REGION FAILURE – region unavailable")

        # Disable access once
        self.spark.conf.set(self.conf_key, "INVALID_KEY")

        # Remains unavailable for the whole duration
        # time.sleep(duration_sec)

        # # Explicit recovery (end of experiment)
        # if self.original_key:
        #     self.spark.conf.set(self.conf_key, self.original_key)

        # print("[FAULT] Region recovered (experiment ended)")
