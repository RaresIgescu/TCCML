# Delta Lake Resilience & OCC Benchmark

### Empirical Analysis of Optimistic Concurrency Control (OCC) in Distributed Systems

![Scala](https://img.shields.io/badge/Language-Scala-red)
![Spark](https://img.shields.io/badge/Engine-Apache%20Spark-orange)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue)
![Azure](https://img.shields.io/badge/Cloud-Azure%20ADLS%20Gen2-0078D4)

---

## 📖 Abstract

This project is a rigorous benchmark suite designed to evaluate the resilience, consistency, and performance of **Delta Lake's Optimistic Concurrency Control (OCC)** protocol under high-concurrency scenarios and simulated distributed system failures.

The system orchestrates concurrent writers using **Scala Futures** and **Apache Spark**, interacting directly with **Azure Data Lake Storage (ADLS) Gen2**. It tests whether ACID guarantees hold when the underlying infrastructure suffers from latency, network partitions, or regional outages.

---

## 🏗️ Architecture & Evolution

### From Python to Scala
The project initially started as a Python prototype (archived in `legacy_python_prototype/`). However, due to the **Global Interpreter Lock (GIL)** in Python, true parallelism was difficult to achieve for high-concurrency benchmarks. 

The core engine was **re-engineered in Scala** to leverage the JVM's native multi-threading capabilities, allowing for accurate simulation of 50+ concurrent writers fighting for the same Delta Lake transaction log.

### Workflow
1.  **Initialization:** A single Spark Driver initializes the connection to Azure.
2.  **Orchestration:** A thread pool spawns concurrent writers.
3.  **Fault Injection:** Before writing, threads are subjected to random latency, timeouts, or connection drops based on the selected scenario.
4.  **Transaction:** Writers attempt to update a specific row in the Delta Table.
5.  **OCC Check:** Delta Lake verifies protocol versions.
    * **Success:** Write is committed.
    * **Conflict:** A `ConcurrentModificationException` is caught and logged.
    * **Failure:** Infrastructure errors (e.g., timeouts) are logged.

---

## 🚀 Key Features

* **Windows Support (with winutils):** Spark/Hadoop on Windows requires `winutils.exe`. This repo expects you to either set `HADOOP_HOME` / `hadoop.home.dir` or provide `./hadoop/bin/winutils.exe` for portability.
* **Scientific Fault Injection:**
    * **Latency Injection:** Simulates variable network lag (200ms - 1000ms).
    * **Network Partition:** Simulates a "Split-Brain" scenario where 50% of nodes lose connectivity (Timeouts).
    * **Region Failure:** Simulates total unavailability of the storage endpoint.
* **Automated Analytics:** Python scripts (`results/`) automatically generate conflict curves and failure rate graphs from the benchmark logs.

---

## 🛠️ Setup & Configuration

### 1. Prerequisites
* **Java JDK 17** installed (matches the current sbt output / Spark 3.5).
* **SBT** (Scala Build Tool) installed.
* **Python 3.x** (only for generating plots).

### 2. Windows only: configure Hadoop winutils

On Windows, Hadoop will error with: `HADOOP_HOME and hadoop.home.dir are unset` unless it can find `winutils.exe`.

Additionally, Spark/Hadoop can require `hadoop.dll` to satisfy JNI calls such as `NativeIO$Windows.access0`.

Choose one:

1) **Portable (recommended):** create `hadoop/bin/winutils.exe` under the project root.

    Also place `hadoop/bin/hadoop.dll` next to it.

2) **System-wide:** set `HADOOP_HOME` to a folder that contains `bin/winutils.exe`.

    Ensure it also contains `bin/hadoop.dll`.

Note: this project depends on Hadoop **3.3.4** (see `build.sbt`), so use a matching winutils build.

### 3. Environment Variables
This project requires access to an Azure Data Lake Gen2 Storage Account.
Create a file named `.env` in the root directory (copy from `.env.template`):

```ini
# .env file
STORAGE_ACCOUNT_NAME=your_storage_account_name
AZURE_ACCESS_KEY=your_azure_access_key
CONTAINER_NAME=your_container_name
```

## ⚡ How to Run

### 1. Run the Benchmark (Scala)
Open a terminal in the project root and run:

```bash
sbt run
```

* This will compile the Scala code.

* It will automatically detect the bundled hadoop.dll for Windows support.

* It will execute 4 scenarios (none, latency, region_failure, network_partition) across 5 concurrency levels (5, 10, 15, 25, 50 writers).

* Results are saved to results/benchmark_results_scala.csv.

---

## 📊 Expected Results

The benchmark produces a CSV file analyzing the **Conflict Rate (Rc)**.

| Scenario | Expected Behavior |
| :--- | :--- |
| **None (Baseline)** | High Conflict Rate (~90%+). High contention for a single resource. 0 Failures. |
| **Latency** | Similar Conflict Rate to Baseline, but higher execution duration. 0 Failures. |
| **Network Partition** | ~50% Failures (Timeouts). The remaining 50% compete for resources (Success/Conflict). |
| **Region Failure** | 100% Failures. 0 Success. 0 Conflicts (System is unreachable). |