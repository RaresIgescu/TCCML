import subprocess
import sys
import time
import random
import argparse

# Configuration for the simulation
PYTHON_EXECUTABLE = sys.executable
WRITER_SCRIPT = "src/workload/writer.py"


def run_simulation(city, sensor_id, num_writers, latency_max):

    print(f"--- [SIMULATION START] Targeting {city} (ID: {sensor_id}) with {num_writers} concurrent writers ---")
    processes = []

    # Pornesc procese writer
    for i in range(num_writers):

        new_val = round(random.uniform(400.0, 485.0), 2)
        # Generez o latenta random pentru a simula conflicte OCC. Fara latenta, toate scrierile ar fi aproape simultane.
        sleep_time = random.uniform(0, latency_max)

        cmd = [
            PYTHON_EXECUTABLE, WRITER_SCRIPT,
            "--city", city,
            "--id", str(sensor_id),
            "--value", str(new_val),
            "--sleep", str(sleep_time)
        ]

        print(f"[*] Spawning Writer {i + 1} (Delay: {sleep_time:.2f}s, Value: {new_val})...")

        # Popen starts the process in the background immediately
        p = subprocess.Popen(cmd)
        processes.append(p)

    print(f"--- All {num_writers} processes launched. Waiting for completion... ---")

    success_count = 0
    failure_count = 0

    for p in processes:
        exit_code = p.wait()
        if exit_code == 0:
            success_count += 1
        else:
            failure_count += 1

    # Print statistics
    print("\n--- [SIMULATION END] Summary ---")
    print(f"Total Writers: {num_writers}")
    print(f"Successful Commits: {success_count}")
    print(f"Failed (OCC Conflicts): {failure_count}")

    # Calculate Conflict Rate (Rc) for this session
    if num_writers > 0:
        rc = failure_count / num_writers
        print(f"Conflict Rate (Rc): {rc:.2%}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orchestrator for Delta Lake Resilience Testing")
    parser.add_argument("--city", type=str, required=True, help="Target City")
    parser.add_argument("--id", type=int, required=True, help="Sensor ID")
    parser.add_argument("--writers", type=int, default=5, help="Number of concurrent writers")
    parser.add_argument("--latency", type=float, default=2.0, help="Max random latency (seconds)")

    args = parser.parse_args()

    run_simulation(args.city, args.id, args.writers, args.latency)