import argparse
from config import get_spark_session


def read_data(city, sensor_id):

    # Unique AppName helps debug in Spark UI( http://localhost:4040 )
    spark, adls_path = get_spark_session(f"Reader_{city}_{sensor_id}")
    # Clean console output, set to WARN or INFO for more details/debuggings
    spark.sparkContext.setLogLevel("ERROR")
    print(f"[*] Reading data for City={city}, SensorID={sensor_id}...")

    try:
        # Se initiaza un DataFrameReader pentru formatul Delta Lake si se filtreaza datele
        # data va fi un pyspark.sql.DataFrame, similar cu un tabel SQL in memorie
        data = spark.read.format("delta").load(adls_path)
        filtered_data = data

        if city:
            filtered_data = filtered_data.filter(filtered_data.capital_name == city)
        if sensor_id:
            filtered_data = filtered_data.filter(filtered_data.id == int(sensor_id))

        count = filtered_data.count()

        if count > 0:
            print(f"[*] Found {count} records.")
            # Afisarea datelor in format tabelar
            filtered_data.show(60, truncate=False)

            # Statistici
            # filtered_data.select("value").describe().show()
        else:
            print("[!] No matching records found.")

    except Exception as e:
        print(f"[ERROR] Failed to read data: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read data from Delta Lake")
    parser.add_argument("--city", type=str, help="Capital city name", default=None)
    parser.add_argument("--id", type=str, help="Sensor ID", default=None)

    args = parser.parse_args()
    read_data(args.city, args.id)