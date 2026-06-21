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

    def analyze_sparsity(self, df, dataset_key):
        """Calculates precise null densities and saves a sparsity heatmap."""
        print(f"\n📊 [EDA] Analyzing Sparsity Matrix for: {dataset_key}...")
        null_counts = df.isnull().sum()
        
        if null_counts.sum() == 0:
            print("   ✨ Data Density: 100% complete across all features (Zero nulls detected).")
            return
        
        print("   ⚠️  Sparsity Alerts (Missing Data Density):")
        for col, count in null_counts[null_counts > 0].items():
            pct = (count / len(df)) * 100
            print(f"      - Feature '{col}': {pct:.2f}% missing values ({count} rows)")
            
        # Chart generation
        plt.figure(figsize=(10, 4))
        sns.heatmap(df.isnull(), cbar=False, cmap="viridis", yticklabels=False)
        plt.title(f"Sparsity Matrix: {dataset_key}")
        plt.tight_layout()
        
        plot_path = f"{self.output_dir}/{dataset_key}_sparsity.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"   📉 Plot saved to disk: {plot_path}")

    def analyze_correlations(self, df, dataset_key):
        """Computes Pearson's r correlation matrix for continuous features."""
        print(f"\n📊 [EDA] Analyzing Feature Correlations for: {dataset_key}...")
        numeric_df = df.select_dtypes(include=[np.number])
        
        if numeric_df.shape[1] < 2:
            print("   ℹ️  Correlation Skip: Insufficient numeric variables to cross-correlate.")
            return
            
        corr_matrix = numeric_df.corr()
        print("   🔢 Strong Feature Correlations (Pearson's |r| > 0.7):")
        
        has_high_corr = False
        for i in range(len(corr_matrix.columns)):
            for j in range(i):
                if abs(corr_matrix.iloc[i, j]) > 0.7:
                    has_high_corr = True
                    print(f"      - High Link: {corr_matrix.columns[i]} ── {corr_matrix.columns[j]} = {corr_matrix.iloc[i, j]:.2f}")
        
        if not has_high_corr:
            print("      - All numeric features are structurally independent (|r| <= 0.7).")

        # Heatmap generation
        plt.figure(figsize=(8, 6))
        sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f", square=True)
        plt.title(f"Feature Correlation Heatmap: {dataset_key}")
        plt.tight_layout()
        
        plot_path = f"{self.output_dir}/{dataset_key}_correlations.png"
        plt.savefig(plot_path)
        plt.close()
        print(f"   📉 Plot saved to disk: {plot_path}")