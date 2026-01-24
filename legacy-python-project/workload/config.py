import os
import sys
import pathlib
from pyspark.sql import SparkSession
from dotenv import load_dotenv


def get_spark_session(app_name="ResilienceSimulation"):

    # Azure credentials configuration from .env file
    load_dotenv()
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
    storage_account_key = os.getenv("AZURE_ACCESS_KEY")
    container_name = os.getenv("CONTAINER_NAME")

    # Set HADOOP_HOME variable, as required for Windows OS
    base_dir = os.path.dirname(os.path.abspath(__file__))
    hadoop_home = os.path.join(base_dir, "../hadoop")
    if os.name == 'nt':
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")

    # Set PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON to the current python executable
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # JAR-urile de care are nevoie Spark pentru a lucra cu Delta Lake si a accesa ADLS
    # Ele sunt descarcate o singura data, apoi stocate in cache-ul Ivy local
    packages = [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-azure:3.3.4"
    ]

    ivy_cache_dir = os.path.join(base_dir, "../spark_ivy_cache")
    if not os.path.exists(ivy_cache_dir):
        os.makedirs(ivy_cache_dir)

    # ivysettings.xml is in the parent folder
    ivy_settings_path = pathlib.Path(os.path.join(base_dir, "../ivysettings.xml")).as_uri()

    # Command-line string that tells PySpark to:
    # - Use the local Ivy cache directory instead of the default Maven cache
    # - Load custom Ivy settings from ivysettings.xml
    # - Download the Delta Lake and Hadoop Azure packages, if they are not already cached
    # - Use the pyspark-shell to execute the command
    submit_args = (
        f'--conf spark.jars.ivy="{ivy_cache_dir}" '
        f'--conf spark.jars.ivySettings="{ivy_settings_path}" '
        f'--packages {",".join(packages)} '
        'pyspark-shell'
    )

    # se seteaza aceste argumente la PYSPARK_SUBMIT_ARGS pentru a fi preluate la lansarea Spark
    os.environ['PYSPARK_SUBMIT_ARGS'] = submit_args

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config(f"spark.hadoop.fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key) \
        .config("spark.driver.memory", "512m") \
        .config("spark.driver.maxResultSize", "128m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.ui.retainedExecutions", "0") \
        .getOrCreate()

    # Path-ul pentru protocolul Azure Blob File System Secure, prin care se acceseaza ADLS Gen2
    # El trebuie sa contina folderul cu _delta_log
    adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/sensor_resilience_table"

    return spark, adls_path