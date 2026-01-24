import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_plots(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Run the benchmark first!")
        return

    # 1. Load the data
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # 2. Data Cleaning
    # The new Scala output already has conflict_rate as a float (e.g., 0.96)
    # We ensure it's treated as numeric
    df['conflict_rate'] = pd.to_numeric(df['conflict_rate'])
    
    # Create output directory if not exists
    os.makedirs("results", exist_ok=True)

    # Set the visual style
    sns.set_theme(style="whitegrid")

    # ==========================================
    # PLOT 1: Conflict Rate (The Logic Check)
    # ==========================================
    plt.figure(figsize=(12, 6))
    
    line_plot = sns.lineplot(
        data=df, 
        x="writers", 
        y="conflict_rate", 
        hue="fault_mode", 
        style="fault_mode",
        markers=True,
        dashes=False,
        linewidth=2.5,
        markersize=9
    )

    plt.title("Delta Lake Resilience: Conflict Rate vs. Concurrency", fontsize=16, fontweight='bold')
    plt.xlabel("Number of Concurrent Writers", fontsize=12)
    plt.ylabel("Conflict Rate (0.0 - 1.0)", fontsize=12)
    plt.ylim(-0.05, 1.05) # Keep limits clean
    plt.legend(title="Fault Scenario", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()

    # Save the plot
    output_path_rc = "results/conflict_curve_scala.png"
    plt.savefig(output_path_rc, dpi=300)
    print(f"[SUCCESS] Conflict plot saved to {output_path_rc}")
    plt.close()

    # ==========================================
    # PLOT 2: Duration Analysis (The Performance Check)
    # ==========================================
    plt.figure(figsize=(12, 6))

    bar_plot = sns.barplot(
        data=df,
        x="writers",
        y="duration_ms",
        hue="fault_mode",
        palette="viridis"
    )

    plt.title("System Latency: Execution Time by Fault Mode", fontsize=16, fontweight='bold')
    plt.xlabel("Number of Concurrent Writers", fontsize=12)
    plt.ylabel("Total Execution Time (ms)", fontsize=12)
    plt.legend(title="Fault Scenario", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()

    # Save the plot
    output_path_dur = "results/duration_bar_scala.png"
    plt.savefig(output_path_dur, dpi=300)
    print(f"[SUCCESS] Duration plot saved to {output_path_dur}")
    plt.close()
    
    # ==========================================
    # PLOT 3: Failure Rate Analysis (The Reliability Check)
    # ==========================================
    # We calculate failure rate as Failures / Writers
    df['failure_rate'] = df['failures'] / df['writers']
    
    plt.figure(figsize=(12, 6))
    fail_plot = sns.lineplot(
        data=df, 
        x="writers", 
        y="failure_rate", 
        hue="fault_mode", 
        marker="X",
        linewidth=2.5,
        markersize=9
    )
    
    plt.title("System Reliability: Failure Rate vs. Concurrency", fontsize=16, fontweight='bold')
    plt.xlabel("Number of Concurrent Writers", fontsize=12)
    plt.ylabel("Failure Rate (0.0 - 1.0)", fontsize=12)
    plt.ylim(-0.05, 1.05)
    plt.legend(title="Fault Scenario", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    
    output_path_fail = "results/failure_curve_scala.png"
    plt.savefig(output_path_fail, dpi=300)
    print(f"[SUCCESS] Failure plot saved to {output_path_fail}")
    plt.close()

if __name__ == "__main__":
    # Ensure this matches the actual file name Scala generated
    generate_plots("results/benchmark_results_scala.csv")