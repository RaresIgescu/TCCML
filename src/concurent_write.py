import os
import sys
import pathlib
import random
import argparse
from threading import Thread
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# Load environment variables from .env file
load_dotenv()

# Set HADOOP_HOME for Windows to fix "HADOOP_HOME and hadoop.home.dir are unset" error
base_dir = os.path.dirname(os.path.abspath(__file__))
hadoop_home = os.path.join(base_dir, "hadoop")
if os.name == 'nt':
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

# Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# --- CONFIGURATION ---
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_account_key = os.getenv("AZURE_ACCESS_KEY")
container_name = os.getenv("CONTAINER_NAME")

if not all([storage_account_name, storage_account_key, container_name]):
    print("[ERROR] Missing required environment variables. Please check your .env file.")
    exit(1)

# Initialize Spark Session with Delta Lake support
packages = [
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-azure:3.3.4"
]

# Create a local ivy cache directory to avoid issues with corrupted local .m2 cache
ivy_cache_dir = os.path.join(base_dir, "spark_ivy_cache")
if not os.path.exists(ivy_cache_dir):
    os.makedirs(ivy_cache_dir)

ivy_settings_path = pathlib.Path(os.path.join(base_dir, "ivysettings.xml")).as_uri()

# Set PYSPARK_SUBMIT_ARGS to ensure Ivy settings are picked up during launch
submit_args = (
    f'--conf spark.jars.ivy="{ivy_cache_dir}" '
    f'--conf spark.jars.ivySettings="{ivy_settings_path}" '
    f'--packages {",".join(packages)} '
    'pyspark-shell'
)
os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args

spark = SparkSession.builder \
    .appName("DeltaLakeConcurrentWrite") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key) \
    .getOrCreate()

print("[*] Spark Session initialized and authenticated.")

# Define the ADLS Gen2 path
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/sensor_resilience_table"

def write_sensor_data(thread_id, sensor_id, capital_name):
    """Write sensor data to Delta Lake from a thread"""
    try:
        print(f"[Thread-{thread_id}] Starting write for sensor {sensor_id} in {capital_name}...")

        # Generate a random value between 410 and 480
        value = random.uniform(410, 480)

        # Create a single-row DataFrame with the required schema
        data = [(sensor_id, capital_name, value)]
        columns = ["id", "capital_name", "value"]

        df = spark.createDataFrame(data, columns).withColumn("timestamp", current_timestamp())

        # Write to Delta Lake in append mode
        df.write.format("delta") \
            .mode("append") \
            .save(adls_path)

        print(f"[Thread-{thread_id}] SUCCESS: Wrote sensor {sensor_id} with value {value:.2f} from {capital_name}")
    except Exception as e:
        print(f"[Thread-{thread_id}] ERROR: {str(e)}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Concurrent Delta Lake Writer")
    parser.add_argument("-t", "--threads", type=int, default=4,
                       help="Number of concurrent threads (default: 4)")
    args = parser.parse_args()

    num_threads = args.threads
    print(f"\n[*] Starting concurrent write with {num_threads} threads...\n")

    threads = []
    capital = "Paris"
    sensor_id = 26

    # Create and start threads
    for i in range(num_threads):

        t = Thread(target=write_sensor_data, args=(i, sensor_id, capital))
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    print(f"\n[*] All {num_threads} threads completed.")
    print(f"[*] Reading final data from Delta Lake...")

    # Display the final data
    df_final = spark.read.format("delta").load(adls_path)
    df_final.show(truncate=False)

    print(f"\n[SUCCESS] Concurrent write operation completed successfully.")

if __name__ == "__main__":
    main()
