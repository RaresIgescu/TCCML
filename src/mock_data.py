import os
import sys
import pathlib
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, to_timestamp, col

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
    .appName("DeltaLakeResilienceSimulation") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.debug.maxToStringFields", 1000) \
    .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key) \
    .getOrCreate()

print("[*] Spark Session initialized and authenticated.")

# Define the local JSON path and cloud table path
local_json_path = os.path.join(base_dir, "mock_data.json")
cloud_table_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/sensor_resilience_table"

# Load raw JSON file from local storage
print(f"[*] Reading raw JSON data from {local_json_path}...")
if not os.path.exists(local_json_path):
    print(f"[ERROR] Local JSON file not found at {local_json_path}")
    exit(1)

df = spark.read.json(local_json_path)

# Enforce types and add processing timestamp
print("[*] Cleaning and transforming data...")
df_cleaned = df.withColumn("timestamp", to_timestamp(col("timestamp"))) \
               .withColumn("processed_at", current_timestamp())

# Write to Delta Lake
print(f"[*] Writing Delta table to {cloud_table_path}...")
df_cleaned.write.format("delta") \
    .mode("append") \
    .save(cloud_table_path)

print("[SUCCESS] Raw data successfully loaded, cleaned, and written to Bronze Delta table.")
