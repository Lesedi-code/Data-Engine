import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlalchemy as sa
import os

def generate_visualizations():
    print("===================================================")
    print("📈 WM-03: DATA VISUALIZATION ENGINE")
    print("===================================================\n")

    # Ensure output directory exists
    os.makedirs("plots", exist_ok=True)

    # 1. Connect to PostgreSQL
    engine = sa.create_engine('postgresql://localhost/postgres')
    
    # 2. Extract bronze landing data
    query = "SELECT * FROM bronze_excel_supplier_catalog;"
    df = pd.read_sql(query, engine)

    # Set visualization style
    sns.set_theme(style="whitegrid")

    # ---------------------------------------------------
    # Plot 1: Price Distribution & Boxplot (WM-03-WE01)
    # ---------------------------------------------------
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    # Histogram / KDE
    sns.histplot(df['price'].dropna(), kde=True, ax=axes[0], color='skyblue')
    axes[0].set_title('Price Distribution Density')
    axes[0].set_xlabel('Price')
    axes[0].set_ylabel('Frequency')

    # Boxplot for Outliers/IQR
    sns.boxplot(y=df['price'], ax=axes[1], color='lightgreen')
    axes[1].set_title('Price Boxplot & Outlier Detection')
    axes[1].set_ylabel('Price Range')

    plt.tight_layout()
    plot1_path = "plots/price_distribution.png"
    plt.savefig(plot1_path)
    plt.close()
    print(f"✅ Saved plot: {plot1_path}")

    # ---------------------------------------------------
    # Plot 2: Supplier Tier Distribution
    # ---------------------------------------------------
    plt.figure(figsize=(8, 5))
    
    # Standardize casing for visualization
    df['supplier_tier_clean'] = df['supplier_tier'].astype(str).str.upper()
    
    sns.countplot(data=df, x='supplier_tier_clean', hue='supplier_tier_clean', palette='viridis', legend=False)
    plt.title('Supplier Tier Frequency')
    plt.xlabel('Supplier Tier')
    plt.ylabel('Count')

    plt.tight_layout()
    plot2_path = "plots/supplier_tier_count.png"
    plt.savefig(plot2_path)
    plt.close()
    print(f"✅ Saved plot: {plot2_path}")

    print("\nVisualizations generated successfully!")

if __name__ == "__main__":
    generate_visualizations()