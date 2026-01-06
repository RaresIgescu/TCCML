import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def generate_plots(csv_path):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found. Run the benchmark first!")
        return

    # Load the data
    df = pd.read_csv(csv_path)

    # Convert "Conflict Rate (Rc)" string "80.00%" to float 0.8
    df['rc_float'] = df['conflict_rate'].str.rstrip('%').astype('float') / 100.0

    # Set the visual style
    sns.set_theme(style="whitegrid")
    plt.figure(figsize=(10, 6))

    # Create the Conflict Curve
    plot = sns.lineplot(
        data=df, 
        x="writers", 
        y="rc_float", 
        hue="fault_mode", 
        marker="o",
        err_style="band" # Shows the variance between the 3 trials
    )

    plt.title("Delta Lake Resilience: Conflict Rate vs. Concurrency", fontsize=15)
    plt.xlabel("Number of Concurrent Writers", fontsize=12)
    plt.ylabel("Conflict Rate (Rc)", fontsize=12)
    plt.ylim(0, 1.1)
    plt.legend(title="Fault Scenario")

    # Save the plot
    output_path = "results/conflict_curve.png"
    plt.savefig(output_path)
    print(f"[SUCCESS] Plot saved to {output_path}")
    plt.show()

if __name__ == "__main__":
    generate_plots("results/benchmark_results.csv")