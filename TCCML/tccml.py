import os
import sys


# Check for JAVA_HOME
if "JAVA_HOME" not in os.environ:
    print("[ERROR] JAVA_HOME is not set. Spark requires Java 8, 11, or 17.")
    print("Please install Java and set the JAVA_HOME environment variable.")
    # exit(1) # Optional: exit if Java is missing

# Set HADOOP_HOME for Windows to fix "HADOOP_HOME and hadoop.home.dir are unset" error
hadoop_home = os.path.abspath("hadoop")
if os.name == 'nt':
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

# Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current python executable
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# --- CONFIGURATION ---
storage_account_name = "datalakesimulation01"
storage_account_key = ""
container_name = "simulation-data"

# Initialize Spark Session with Delta Lake support
packages = [
    "io.delta:delta-spark_2.12:3.2.0",
    "org.apache.hadoop:hadoop-azure:3.3.4"
]

# Create a local ivy cache directory to avoid issues with corrupted local .m2 cache
ivy_cache_dir = os.path.abspath("spark_ivy_cache")
if not os.path.exists(ivy_cache_dir):
    os.makedirs(ivy_cache_dir)

import pathlib
ivy_settings_path = pathlib.Path(os.path.abspath("ivysettings.xml")).as_uri()

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
    .config(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key) \
    .getOrCreate()

print("[*] Spark Session initialized and authenticated.")

# Create sample data representing different geographic regions
data = [
    (1, "Sensor_Alpha", "East US", 22.5),
    (2, "Sensor_Beta", "West Europe", 19.8),
    (3, "Sensor_Gamma", "North Europe", 21.0)
]
columns = ["id", "sensor_name", "geo_region", "value"]

df = spark.createDataFrame(data, columns).withColumn("processed_at", current_timestamp())

# Define the ADLS Gen2 path (ABFSS protocol)
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/sensor_resilience_table"

# Write the data in Delta format
print(f"[*] Writing Delta table to {adls_path}...")
df.write.format("delta").mode("overwrite").save(adls_path)

print("[SUCCESS] Delta table is now live in your Data Lake.")