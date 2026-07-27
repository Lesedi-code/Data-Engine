"""
===============================================================================
MODULE: Silver Transformation Layer (Data Engine)
PURPOSE: Read raw Bronze records, clean data according to pipeline.json rules,
         and save standardized records to the Silver PostgreSQL table.
===============================================================================
"""
import os
import sys
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Ensure local imports work regardless of working directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config_loader import load_config
from ingestion import get_db_engine

# Configure basic logging with clean formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def apply_silver_transformations(df: pd.DataFrame, silver_rules: dict)-> pd.DataFrame:
    """
    Applies data cleansing, imputation, trimming, and casing rules to raw DataFrame.
    """
    df = df.copy()

    # 1. Clean string/text columns (strip whitespace)
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].astype(str).str.strip()

    # 2. Drop specified columns
    drop_cols = silver_rules.get("drop_columns", [])
    if drop_cols:
        df = df.drop(columns=[c for c in drop_cols if df.columns], errors="ignore")
        logging.info(f"🗑️ Dropped columns:, {drop_cols}")

    # 3. Handle Missing Values (Imputation)
    impute_cfg = silver_rules.get("impute_strategies", {})
    num_default = impute_cfg.get("numeric_default", "median")
    cat_default = impute_cfg.get("catergorical_default, UNKNOWN")

    # Numeric Imputation
    num_cols = df.select_dtypes(include=["number"]).columns
    for col in num_cols:
        if df[col].isnull().any():
            if num_default == "median":
                fill_val = df[col].median()
            elif num_default == "mean":
                fill_val = df[col].mean()
            else:
                fill_val = 0 
            df[col] = df[col].fillna(fill_val)
            logging.info(f"🩹 Imputed missing numeric values in {col} using {num_default}: {fill_val}")

    # Categorical/Text Imputation
    for col in string_cols:
        if col in df.columns:
           # Replace string representations of NaN/None/null
           df[col] = df[col].replace(["nan", "None", "null", "NaN", "<NA>"], None)
           if df[col].isnull().any():
               df[col] = df[col].fillna(cat_default)
               logging.info(f"🩹 Imputed missing text in '{col}' with: '{cat_default}'")

    # 4. Uppercase specific columns if requested
    uppercase_cols = silver_rules.get("uppercase_columns", [])
    for col in uppercase_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper()

    # 5. Audit metadata
    df["_processed_at"] = datetime.now()

    return df

def run_silver_transformation() -> pd.DataFrame:
    """
    Executes Silver Layer processing: reads Bronze table, cleans data, writes to Silver table.
    """                    
    print("\n" + '='*50)
    print("🥈 PHASE 2: SILVER LAYER (CLEANSE & TRANSFORM)")
    print ("="*50)

    # Load configuration
    config = load_config()
    db_config = config.get("database", {})
    active_key = config.get("active_source") or config.get("active_pipeline") or "primary_ingestion"
    source_dict = config.get("pipelines") or config.get("data_sources") or {}

    pipeline_cfg = source_dict.get(active_key, {})
    target_tables = pipeline_cfg.get("target_tables", {})

    bronze_table = target_tables.get("bronze") or f"bronze_{active_key}"
    silver_table = target_tables.get("silver") or f"silver_{active_key}"
    silver_rules = pipeline_cfg.get("silver_rules", {})

    engine = get_db_engine(db_config)

    # 1. Read raw dataset from Bronze table in PostgreSQL
    logging.info(f"📖 Reading raw data from database table: '{bronze_table}'...")
    try:
        raw_df = pd.read_sql (f"select * FROM {bronze_table}", con=engine)
        logging.info(f"📊 Loaded {len(raw_df)} raw records fron Bronze.")
    except Exception as e:
        raise RuntimeError(f"❌ Failed to read Bronze table '{bronze_table}': {e}")

    # 2. Apply transformations   
    clean_df = apply_silver_transformations(raw_df, silver_rules)

    # 3. Write cleansed data to Silver table in PostgreSQL
    try:
        clean_df.to_sql(name=silver_table, con=engine, if_exists="replace", index=False)
        logging.info(f"✅ Successfully written cleansed data to table: '{silver_table}'")
    except Exception as e:
        logging.error(f"❌ Could nnot write to Silver table: {e}")

    print("="*50 + "\n")
    return clean_df

if __name__ == "__main__":
    print("\n🚀 EXECUTING SILVER TRANSFORMATION TEST...")    
    silver_df = run_silver_transformation()
    print("silver Sample Data preview:")
    print(silver_df.head(3))
    


    