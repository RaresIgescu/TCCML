import sys
import os
import random
import argparse
import subprocess
import threading
import time

# Configuration for the simulation
PYTHON_EXECUTABLE = sys.executable
base_dir = os.path.dirname(os.path.abspath(__file__))
WRITER_SCRIPT = os.path.join(base_dir, "writer.py")
# WRITER_SCRIPT = "src/workload/writer.py"

VALID_TAGS = [
    "[SUCCESS]", "[CONFLICT]", "[FAULT]", "[*]", 
    "[LATENCY]", "[ERROR]", "[DEBUG]", "Timeout", "SocketException",
    "Traceback", "File \"", "Error:", "Exception"
]

# Phrases to explicitly silence (Spark Windows file deletion noise)
IGNORED_PHRASES = [
    "ShutdownHookManager: Exception while deleting Spark temp dir",
    "java.io.IOException: Failed to delete",
    "java.nio.file.NoSuchFileException",
    "sun.nio.fs.WindowsException",
    "org.apache.spark.util.Utils$.logUncaughtExceptions",
    "Cannot invoke \"org.apache.spark.storage.BlockManagerId.executorId()\"",
    "org.apache.spark.SparkException: Exception thrown in awaitResult:",
    "at org.apache.spark.rpc.RpcTimeout.awaitResult"
]


def output_filter(process, prefix):
    """
    Only prints lines that contain specific 'Thesis Tags'.
    Silences everything else (Ivy, Spark internals, Windows errors).
    """
    # We use 'errors="replace"' to avoid crashing on weird Windows encoding characters
    for line in iter(process.stdout.readline, ''):
        line = line.strip()
        if not line:
            continue

        # print(f"{prefix} {line}")

        # 1. Skip if the line contains any ignored phrases (Windows cleanup noise)
        if any(ignored in line for ignored in IGNORED_PHRASES):
            continue

        # 2. If the line contains ANY valid tag, print it.
        if any(tag in line for tag in VALID_TAGS):
            print(f"{prefix} {line}")


def run_simulation(city, sensor_id, num_writers, latency_max):

    print(f"--- [SIMULATION START] Targeting {city} (ID: {sensor_id}) with {num_writers} concurrent writers ---")
    processes = []
    threads = []  # vor fi folosite pentru a filtra ouputul de la procese

    # Pornesc procese writer
    for i in range(num_writers):

        new_val = round(random.uniform(400.0, 485.0), 2)
        # Generez o latenta random pentru a simula conflicte OCC. Fara latenta, toate scrierile ar fi aproape simultane.
        sleep_time = random.uniform(0, latency_max)

        # 1. DEFINE the command list FIRST
        cmd = [
            PYTHON_EXECUTABLE, WRITER_SCRIPT,
            "--city", city,
            "--id", str(sensor_id),
            "--value", str(new_val),
            "--sleep", str(sleep_time)
        ]

        print(f"[*] Spawning Writer {i + 1} (Delay: {sleep_time:.2f}s, Value: {new_val})...")

        # 2. Prepare the environment with the correct PYTHONPATH
        current_env = os.environ.copy()
        # Ensure FAULT_MODE is passed through (it comes from run_benchmark.py)

        # 3. NOW call Popen using the defined 'cmd'
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,   # Merge stderr into stdout so we can see crashes
            text=True,
            bufsize=1,
            env=current_env
        )

        processes.append(p)
        t = threading.Thread(target=output_filter, args=(p, f"[Writer {i + 1}]"))
        t.start()
        threads.append(t)

    print(f"--- All {num_writers} processes launched. Waiting for completion... ---")

    # Replaces: for t in threads: t.join()
    # Wait for completion with timeout
    start_wait = time.time()
    TIMEOUT_SECONDS = 120  # 2 minutes max per scenario (generous for Spark startup)

    while True:
        alive_procs = [p for p in processes if p.poll() is None]
        if not alive_procs:
            break

        if time.time() - start_wait > TIMEOUT_SECONDS:
            print(f"\n[TIMEOUT] Simulation timed out after {TIMEOUT_SECONDS}s. Killing remaining processes...")
            for p in alive_procs:
                p.terminate()  # Try nice termination first
                time.sleep(0.5)
                if p.poll() is None:
                    p.kill()   # Force kill
            break

        time.sleep(1)

    # Ensure threads finish (they should since pipes are closed/broken by kill)
    for t in threads:
        t.join(timeout=1.0)

    success_count = sum(1 for p in processes if p.poll() == 0)
    failure_count = num_writers - success_count

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