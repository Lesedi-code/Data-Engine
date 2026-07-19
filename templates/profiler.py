import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

class DiscoveryProfiler:
    """Computes heavy mathematical profiling metrics and exports visual plots."""
    
    def __init__(self, output_dir="Docs/plots"):
        self.output_dir = output_dir
        # Ensure the directory exists locally on your workstation
        os.makedirs(self.output_dir, exist_ok=True)

    def analyze_raw_shape(self, df, dataset_key):
        """Logs basic table dimensions and prints data type distributions."""
        print(f"\n📊 [EDA] Analyzing Basic Shape for Matrix: {dataset_key}...")
        rows, cols = df.shape
        print(f"   📐 Dimensions: {rows} total records | {cols} distinct features")
        print("\n   🗂️  Feature Data Type Distribution:")
        # Fixed: Iterate safely through the counts instead of using prefix
        for dtype, count in df.dtypes.value_counts().items():
            print(f"      - {dtype}: {count}")

    def analyze_sparsity_and_distribution(self, df, dataset_key):
        """Calculates precise null densities and saves a basic missing-value layout map."""
        print(f"\n📝 [EDA] Checking Missing Values for: {dataset_key}...")
        null_counts = df.isnull().sum()
        
        if null_counts.sum() == 0:
            print("   ✨ Data Density: 100% complete across all features (Zero nulls detected).")
        else:
            print("   ⚠️  Sparsity Alerts (Missing Data Density):")
            for col, count in null_counts[null_counts > 0].items():
                pct = (count / len(df)) * 100
                print(f"      - Feature '{col}': {pct:.2f}% missing values ({count} rows)")
            
        # Light, simple distribution/missingness visual plot
        plt.figure(figsize=(10, 3))
        df.notnull().sum().plot(kind="bar", color="skyblue", edgecolor="black")
        plt.title(f"Data Distribution Density (Non-Null Counts): {dataset_key}")
        plt.ylabel("Available Records")
        plt.xlabel("Features")
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        
        plot_path = f"{self.output_dir}/{dataset_key}_density_map.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"   📉 Summary plot saved to disk: {plot_path}")