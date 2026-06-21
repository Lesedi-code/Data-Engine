import pandas as pd
import numpy as np
import os

# Ensure the targeted storage folder exists cleanly
os.makedirs("Data/raw", exist_ok=True)

# Define an expanded structural matrix with mixed data types and intentional anomalies
mock_data = {
    "product_id": ["P001", "P002", "P003", "P004", "P005", "P005"], # ⚠️ Intentional exact duplicate row
    "product_name": ["Wireless Mouse", "Mechanical Keyboard", "Ergonomic Chair", "Type-C Hub", "Desk Mat", "Desk Mat"],
    "category": ["Electronics", "Electronics", "Office", "Electronics", "Office", "Office"],
    "demand_score": [85, 92, np.nan, 61, 45, 45],                  # ⚠️ Intentional NaN value for Sparsity plots
    "price": [250.00, 899.00, 1200.00, 450.00, np.nan, np.nan],     # 📊 Second numeric column with NaN for Correlation/Sparsity
    "supplier_tier": ["Tier 1", "tier 1", "Tier 2", "Tier 3", "Tier 2", "Tier 2"] # 🔠 Case mismatch ('tier 1' vs 'Tier 1')
}

df = pd.DataFrame(mock_data)

print("==================================================")
print("[GENERATING] Manufacturing Corrupted Mock Datasets for Testing...")
print("==================================================")

# 1. Generate JSON Dataset
json_path = "Data/raw/api_products.json"
df.to_json(json_path, orient="records", indent=4)
print(f"[SUCCESS] Created JSON Stream: {json_path}")

# 2. Generate Excel Dataset
excel_path = "Data/raw/supplier_catalog.xlsx"
df.to_excel(excel_path, index=False)
print(f"[SUCCESS] Created Excel Sheets: {excel_path}")

# 3. Generate Parquet Dataset
parquet_path = "Data/raw/historical_demand.parquet"
df.to_parquet(parquet_path, index=False)
print(f"[SUCCESS] Created Parquet Matrix: {parquet_path}")
print("==================================================\n")