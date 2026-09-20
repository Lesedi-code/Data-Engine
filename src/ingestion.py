"""
===============================================================================
MODULE: Bronze Ingestion Layer (Agnostic Data Engine)
PURPOSE: Extract raw source datasets (CSV, Excel, JSON, Parquet, SQL DBs) 
         and land them unmodified in PostgreSQL Staging.
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
    Creates and returns a SQLAlchemy engine for target or source databases.
    """
    dialect = db_config.get("dialect", "postgresql")
    user = db_config.get("user", "postgres")
    password = db_config.get("password", "")
    host = db_config.get("host", "localhost")
    port = db_config.get("port", 5432)
    dbname = db_config.get("dbname", "postgres")

    # Supports mysql+pymysql, postgresql, mssql+pyodbc, etc.
    if "postgresql" in dialect and not dialect.startswith("postgresql"):
        dialect = "postgresql"

    connection_url = f"{dialect}://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(connection_url)

def run_bronze_ingestion() -> pd.DataFrame:
    """
    Executes the Bronze Layer raw data ingestion dynamically based on pipeline configs.
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
    source_format = str(pipeline_cfg.get("source_format") or pipeline_cfg.get("format") or "CSV").upper()
    source_path = pipeline_cfg.get("source_path") or pipeline_cfg.get("path")
    
    target_tables = pipeline_cfg.get("target_tables", {})
    target_table = target_tables.get("bronze") or pipeline_cfg.get("bronze_table") or f"bronze_{active_key}"

    # 2. Extract raw data dynamically based on format
    if source_format in ["MYSQL", "POSTGRES", "POSTGRESQL", "SQLSERVER", "DATABASE"]:
        query = pipeline_cfg.get("source_query", f"SELECT * FROM {active_key};")
        logging.info(f"📥 Querying live source database table/query: '{query}'")
        try:
            source_engine = get_db_engine(pipeline_cfg.get("source_database", {}))
            df = pd.read_sql(query, con=source_engine)
        except Exception as e:
            logging.warning(f"⚠️ Live database connection failed ({e}). Attempting fallback to local path if available...")
            if source_path and os.path.exists(source_path):
                df = pd.read_csv(source_path)
            else:
                raise e

    else:
        # File-based extraction
        logging.info(f"📥 Loading raw file from: {source_path} (Format: {source_format})")
        if not source_path or not os.path.exists(source_path):
            raise FileNotFoundError(f"❌ Source file not found at path: '{source_path}'")

        if source_format == "CSV":
            df = pd.read_csv(source_path)
            
        elif source_format in ["EXCEL", "XLSX", "XLS"]:
            sheet_name = pipeline_cfg.get("sheet_name", 0)
            df = pd.read_excel(source_path, sheet_name=sheet_name)
            
        elif source_format == "JSON":
            try:
                df = pd.read_json(source_path, lines=True)  # Line-delimited JSON (NDJSON)
            except ValueError:
                df = pd.read_json(source_path)             # Standard JSON Array
                
        elif source_format == "PARQUET":
            df = pd.read_parquet(source_path)
            
        else:
            raise ValueError(f"❌ Unsupported format '{source_format}' configured in JSON/YAML!")

    # 3. Add audit timestamp metadata
    df["_ingested_at"] = datetime.now()
    logging.info(f"📊 Extracted {len(df)} rows and {len(df.columns)} columns.")

    # 4. Land raw data in PostgreSQL Bronze table
    try:
        target_engine = get_db_engine(config.get("database", {}))
        df.to_sql(name=target_table, con=target_engine, if_exists="replace", index=False)
        logging.info(f"✅ Successfully written to database table: '{target_table}'")
    except Exception as e:
        logging.warning(f"⚠️ Could not write to target Postgres database ({e}). Returning DataFrame in memory.")

    print("="*50 + "\n")
    return df

if __name__ == "__main__":
    print("\n🚀 EXECUTING INGESTION TEST...")
    raw_df = run_bronze_ingestion()
    print("Bronze Sample Data preview:")
    print(raw_df.head(3))
