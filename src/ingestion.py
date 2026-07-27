"""
===============================================================================
MODULE: Bronze Ingestion Layer (Agnostic Data Engine)
PURPOSE: Extract raw source datasets and land them unmodified in PostgreSQL.
UNDER THE HOOD:
  - Uses `config_loader` to fetch source file paths and target database parameters.
  - Handles dynamic file formats (CSV, Excel, JSON).
  - Appends audit metadata (`_ingested_at` timestamp).
  - Writes to PostgreSQL `bronze_*` raw tables using SQLAlchemy.
===============================================================================
"""

import os
import sys
import logging
from datetime import datetime 
import pandas as pd
from sqlalchemy import create_engine

# Ensure local imports work regardless of current working directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config_loader import load_config

# Configure basic logging with clean formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_db_engine(db_config: dict):
    """
    Creates and returns a SQLAlchemy PostgreSQL engine.
    """
    user = db_config.get("user", "postgres")
    password = db_config.get("password", "")
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 5432)
    dbname = db_config.get("dbname", "postgres")

    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_url)

def run_bronze_ingestion() -> pd.DataFrame:
    """
    Executes the Bronze Layer raw data ingestion based on pipeline configs.
    """
    print("\n" + "="*50)
    print("🥉 PHASE 1: BRONZE LAYER (RAW INGESTION)")
    print("="*50)

    # 1. Load pipeline configuration
    config = load_config()
    active_key = config.get("active_source") or config.get("active_pipeline") or "primary_ingestion"
    sources_dict = config.get("pipelines") or config.get("data_sources") or {}

    if active_key not in sources_dict:
        available_keys = list(sources_dict.keys())
        raise KeyError(f"❌ Active key '{active_key}' not found in configuration! Available keys: {available_keys}")
        
    pipeline_cfg = sources_dict[active_key]
    source_format = (pipeline_cfg.get("source_format") or pipeline_cfg.get("format") or "EXCEL").upper()
    source_path = pipeline_cfg.get("source_path") or pipeline_cfg.get("path")

    target_tables = pipeline_cfg.get("target_tables", {})
    target_table = target_tables.get("bronze") or pipeline_cfg.get("bronze_table") or f"bronze_{active_key}"

    logging.info(f"📥 Loading raw file from: {source_path} (Format: {source_format})")

    if not source_path or not os.path.exists(source_path):
        raise FileNotFoundError(f"❌ Source file not found at path: '{source_path}'")

    # 2. Extract raw data dynamically based on format
    if source_format == "CSV":
        df = pd.read_csv(source_path)
    elif source_format in ["EXCEL", "XLSX"]:
        df = pd.read_excel(source_path)
    elif source_format == "JSON":
        df = pd.read_json(source_path)
    else:
        raise ValueError(f"❌ Unsupported format '{source_format}'")

    # 3. Add audit timestamp metadata
    df["_ingested_at"] = datetime.now()

    logging.info(f"📊 Extracted {len(df)} rows and {len(df.columns)} columns.")

    # 4. Land raw data in PostgreSQL Bronze table
    try:
        engine = get_db_engine(config.get("database", {}))
        df.to_sql(name=target_table, con=engine, if_exists="replace", index=False)
        logging.info(f"✅ Successfully written to database table: '{target_table}'")
    except Exception as e:
        logging.warning(f"⚠️ Could not write to Postgres database ({e}). Returning DataFrame in memory.")

    print("="*50 + "\n")
    return df

if __name__ == "__main__":
    # Test execution when script is run directly
    print("\n🚀 EXECUTING INGESTION TEST...")
    raw_df = run_bronze_ingestion()
    print("Bronze Sample Data preview:")
    print(raw_df.head(3))
