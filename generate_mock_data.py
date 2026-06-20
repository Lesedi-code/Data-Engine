import pandas as pd
import os

# Ensure the targeted storage folder exists cleanly
os.makedirs("Data/raw", exist_ok=True)

# Define our base structural data matrix
mock_data = {
    "product_id": ["P001", "P002", "P003", "P004", "P005"],
    "product_name": ["Wireless Mouse", "Mechanical Keyboard", "Ergonomic Chair", "Type-C Hub", "Desk Mat"],
    "demand_score": [85, 92, 74, 61, 45],
    "supplier_tier": ["Tier 1", "Tier 1", "Tier 2", "Tier 3", "Tier 2"]
}

df = pd.DataFrame(mock_data)

print("==================================================")
print("[GENERATING] Manufacturing Mock Datasets...")
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