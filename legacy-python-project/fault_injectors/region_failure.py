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

        # Access the Spark Context and Hadoop configuration
        sc = self.spark.sparkContext
        hadoop_conf = sc._jsc.hadoopConfiguration()

        # 3. Target the specific key for your storage account
        conf_key = f"fs.azure.account.key.{self.storage_account_name}.dfs.core.windows.net"
        hadoop_conf.set(conf_key, "INVALID_KEY_FOR_TESTING")
