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

print("[*] Reading sensor data for id=26 (Paris)...")

# Read Delta table
df = spark.read.format("delta").load(adls_path)

# Filter for sensor id 26 in Paris
df_paris = df.filter((df.id == 26) & (df.capital_name == "Paris"))

# Display the filtered data
print("[*] Data from sensor id=26 in Paris:")
df_paris.show(truncate=False)

# Show count and statistics
count = df_paris.count()
print(f"\n[*] Total records for sensor 26 (Paris): {count}")

if count > 0:
    print("[*] Value statistics:")
    df_paris.select("value").describe().show()

    print("[SUCCESS] Data retrieved successfully for Paris sensor (id=26).")
else:
    print("[WARNING] No data found for sensor id=26 in Paris.")
