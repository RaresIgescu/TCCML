import argparse
import time
from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable
from config import get_spark_session


def write_data(city, sensor_id, new_value, sleep_time = 0):

    # Unique AppName helps debug in Spark UI( http://localhost:4040 )
    spark, adls_path = get_spark_session(f"Writer_{city}_{sensor_id}")
    # Clean console output, set to WARN or INFO for more details/debuggings
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[*] Writer started for {city} (ID: {sensor_id}). Target Value: {new_value}")

    try:
        # Initialize DeltaTable object (Needed for Updates)
        deltaTable = DeltaTable.forPath(spark, adls_path)

        # Simulate Processing Latency (Optional, for easy conflict testing)
        if sleep_time > 0:
            print(f"[*] Sleeping for {sleep_time} seconds to simulate processing...")
            time.sleep(sleep_time)

        print("[*] Attempting ACID Update...")

        deltaTable.update(
            condition=f"capital_name = '{city}' AND id = {sensor_id}",
            set={
                "value": lit(new_value),
                "timestamp": current_timestamp()
            }
        )

        print(f"[SUCCESS] Update committed for {city}.")

    # OCC failures
    except Exception as e:
        print(f"[CONFLICT] Transaction failed for {city}!")
        print(f"Error Details: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Write/Update data to Delta Lake")
    parser.add_argument("--city", type=str, required=True, help="Capital city name")
    parser.add_argument("--id", type=int, required=True, help="Sensor ID")
    parser.add_argument("--value", type=float, required=True, help="New CO2 value")
    parser.add_argument("--sleep", type=int, default=0, help="Simulate delay (seconds)")

    args = parser.parse_args()
    write_data(args.city, args.id, args.value, args.sleep)