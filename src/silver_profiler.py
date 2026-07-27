import pandas as pd
import numpy as np
import sqlalchemy as sa

# 1. DEFINE THE FUNCTION FIRST
def profile_silver_layer():
    # Establish Postgres connection
    engine = sa.create_engine('postgresql://localhost/postgres')

    # Fetch dataset into pandas matrix
    df = pd.read_sql("SELECT * FROM bronze_excel_supplier_catalog;", engine)

    # 1. Summary Statistics (WM-02-WE01)
    summary_stats = df[['demand_score', 'price']].describe().T
    summary_stats['median'] = df[['demand_score', 'price']].median()
    summary_stats['iqr'] = df[['demand_score', 'price']].quantile(0.75) - df[['demand_score', 'price']].quantile(0.25)

    print("--- DESCRIPTIVE STATISTICAL PROFILE ---")
    print(summary_stats[['mean', 'std', 'median', 'min', 'max', 'iqr']])

    # 2. Categorical & Missingness Audit (WM-02-WE02)
    print("\n--- MISSINGNESS & FREQUENCY ANALYSIS ---")
    print("Null count (demand_score):", df['demand_score'].isnull().sum())
    print("Null count (price):", df['price'].isnull().sum())
    print("Supplier Tier Distribution:\n", df['supplier_tier'].str.upper().value_counts())

# 2. CALL IT AT THE BOTTOM
if __name__ == "__main__":
    profile_silver_layer()