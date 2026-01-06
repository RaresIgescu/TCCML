import sys
import subprocess
import os
import csv
import time
from datetime import datetime

# Configuration
PYTHON_EXE = sys.executable
ORCHESTRATOR = "src/workload/orchestrator.py"
OUTPUT_FILE = "results/benchmark_results.csv"

# Global Variables
project_root = os.path.dirname(os.path.abspath(__file__))

def run_scenario(writers, fault_mode, city="BenchmarkCity"):
    """Runs a single simulation and returns the parsed results."""
    print(f"\n>>> RUNNING SCENARIO: Writers={writers}, Fault={fault_mode}")
    
    # Set environment for the child processes
    env = os.environ.copy()
    env["FAULT_MODE"] = fault_mode
    sep = ";"
    env[
        "PYTHONPATH"] = f"{project_root}{sep}{os.path.join(project_root, 'src')}{sep}{os.path.join(project_root, 'src/workload')}"

    orch_path = os.path.abspath(ORCHESTRATOR)
    cmd = [
        PYTHON_EXE, orch_path,
        "--city", city,
        "--id", "999",
        "--writers", str(writers),
        "--latency", "0.5" # Low staggering to maximize collision chance
    ]
    
    start_time = time.time()
    # Replace subprocess.run with Popen to stream output
    # result = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=project_root)

    print("  -> Output streaming:")
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=env,
        cwd=project_root,
        bufsize=1
    )

    full_stdout = []

    # Read output line by line as it is generated
    for line in iter(process.stdout.readline, ''):
        line = line.strip()
        if line:
            print(f"     | {line}")
            full_stdout.append(line)

    return_code = process.wait()
    stdout_str = "\n".join(full_stdout)

    # ADD THIS: If the orchestrator didn't output a Summary, it means it CRASHED.
    # We need to see why.
    if "--- [SIMULATION END] Summary ---" not in stdout_str:
        print(f"[ERROR] Orchestrator crashed or failed to complete for {fault_mode}")
        # print("--- STDOUT ---") # Already printed above
        # print(stdout_str)
    duration = round(time.time() - start_time, 2)

    # Simple parsing of the Summary output from concurent_write.py
    success = 0
    failed = 0
    rc = "0%"

    for line in full_stdout:
        if "Successful Commits:" in line:
            success = int(line.split(":")[1].strip())
        if "Failed (OCC Conflicts):" in line:
            failed = int(line.split(":")[1].strip())
        if "Conflict Rate (Rc):" in line:
            rc = line.split(":")[1].strip()

    return {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "writers": writers,
        "fault_mode": fault_mode,
        "success": success,
        "failed": failed,
        "conflict_rate": rc,
        "duration_sec": duration
    }

def main():
    
    # Real benchmark parameters
    # writer_counts = [1, 5, 10, 15]
    # fault_modes = ["none", "latency", "region_failure", "network_partition"]

    # Test parameters (quick run)
    writer_counts = [3]
    fault_modes = ["none"]

    fieldnames = ["timestamp", "writers", "fault_mode", "success", "failed", "conflict_rate", "duration_sec"]
    
    # Open CSV in append mode
    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if f.tell() == 0: writer.writeheader()

        for mode in fault_modes:
            for count in writer_counts:
                # Run each scenario 3 times for statistical significance
                for trial in range(1, 4):
                    print(f"Trial {trial}/3...")
                    data = run_scenario(count, mode)
                    writer.writerow(data)
                    f.flush() # Save to disk immediately

    print(f"\n[DONE] Benchmark complete. Results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()