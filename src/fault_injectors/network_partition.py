class NetworkPartitionInjector:
    """
    Simulates a network partition by closing existing connections
    and routing new traffic to a blackhole IP (Dead Proxy).
    """

    def __init__(self, spark, storage_account_name):
        self.spark = spark
        self.account = storage_account_name

    def inject_partition(self):
        print("[FAULT] NETWORK PARTITION – Hard-cutting storage link")

        sc = self.spark.sparkContext
        # Access the Java Hadoop Configuration
        hadoop_conf = sc._jsc.hadoopConfiguration()

        # 1. THE NUCLEAR OPTION: Close all cached FileSystem instances in the JVM.
        # This forces Spark to look at our poisoned config for the next request.
        try:
            sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.closeAll()
            print("[DEBUG] Purged JVM FileSystem Cache")
        except Exception as e:
            print(f"[DEBUG] Cache purge failed: {e}")

        # 2. Credential Poisoning
        # Remove the key so it cannot authenticate even if it reaches Azure.
        key_name = f"fs.azure.account.key.{self.account}.dfs.core.windows.net"
        hadoop_conf.unset(key_name)
        hadoop_conf.set(key_name, "PARTITION_ERROR")

        # 3. Protocol Sabotage
        # Change the auth type to something non-existent to break the handshake.
        hadoop_conf.set(f"fs.azure.account.auth.type.{self.account}.dfs.core.windows.net", "BrokenLink")

        # 4. Immediate Timeout
        hadoop_conf.set("fs.azure.io.retry.max.retries", "0")
        hadoop_conf.set("fs.azure.connect.timeout", "10")  # 10ms