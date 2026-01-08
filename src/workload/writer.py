import argparse
import time
import sys
import os
from idlelib.config_key import translate_key

from dotenv import load_dotenv

# Add parent directory to path to allow importing from sibling directories
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
from config import get_spark_session
from fault_injectors.fault_router import FaultRouter

def write_data(city, sensor_id, new_value, sleep_time = 0):

    load_dotenv()
    # Unique AppName helps debug in Spark UI( http://localhost:4040 )
    spark, adls_path = get_spark_session(f"Writer_{city}_{sensor_id}")
    storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")

    # Clean console output, set to WARN or INFO for more details/debugging
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[*] Writer started for {city} (ID: {sensor_id}). Target Value: {new_value}")

    router = FaultRouter(spark, storage_account_name)
    router.inject()  # <--- This applies the active fault mode
    transaction_successful = False

    try:
        # Initialize DeltaTable object (Needed for Updates)
        deltaTable = DeltaTable.forPath(spark, adls_path)

        # Simulate processing delay to increase chance of OCC conflicts
        if sleep_time > 0:
            time.sleep(sleep_time)

        print("[*] Attempting ACID Update...")

        deltaTable.update(
            condition=f"capital_name = '{city}' AND id = {sensor_id}",
            set={
                "value": lit(new_value),
                "timestamp": current_timestamp()
            }
        )

        transaction_successful = True
        print(f"[SUCCESS] Update committed for {city}.")

    # OCC failures
    except Exception as e:
        print(f"[CONFLICT] Transaction failed for {city}!")
        print(f"[ERROR] Details: {e}")
        transaction_successful = False

        # Special handling for Network Partitions
        # Since the link is severed, spark.stop() will often hang indefinitely
        # trying to contact the Azure catalog/log. We force-kill here.
        if os.getenv("FAULT_MODE") == "network_partition":
            print("[DEBUG] Network Partition detected: Force-killing to avoid shutdown hang.")
            sys.stdout.flush()
            os._exit(1)  # Exit with error code 1

    finally:
        # 1. Clean up Spark resources if they exist
        if 'spark' in locals():
            print("[DEBUG] Stopping Spark Session...")
            try:
                # We wrap this in a try/except because if the network is flaky,
                # spark.stop() might throw its own exception.
                spark.stop()
            except Exception:
                pass

        print("[DEBUG] Process exiting...")
        sys.stdout.flush()

        # 2. THE CRUCIAL FIX: Signal the actual result to the Orchestrator
        if transaction_successful:
            os._exit(0)  # Success
        else:
            os._exit(1)  # Failure (This will now be caught by your benchmark)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write/Update data to Delta Lake")
    parser.add_argument("--city", type=str, required=True, help="Capital city name")
    parser.add_argument("--id", type=int, required=True, help="Sensor ID")
    parser.add_argument("--value", type=float, required=True, help="New CO2 value")
    parser.add_argument("--sleep", type=float, default=0.0, help="Simulate delay (seconds)")

    args = parser.parse_args()

    # Move sleep here to stagger Spark session creation and avoid Ivy lock contention
    if args.sleep > 0:
        print(f"[DEBUG] Staggering start. Sleeping for {args.sleep:.2f}s...")
        time.sleep(args.sleep)

    write_data(args.city, args.id, args.value, 0) # Pass 0 because we already slept
