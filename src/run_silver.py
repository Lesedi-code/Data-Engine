import json
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

with open("configs/pipeline.json", "r") as f:
    config = json.load(f)

pipeline_cfg = config["pipelines"][config.get("active_source")]
db_cfg = config["database"]
engine = create_engine(f"postgresql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host']}:{db_cfg['port']}/{db_cfg['dbname']}")

def run_bronze_and_silver():
    targets = pipeline_cfg["target_tables"]
    
    # --- BRONZE PHASE (Ingestion logic goes here) ---
    logging.info("📥 Executing Bronze Ingestion...")
    # (Your code that reads the raw file into targets['bronze'])

    # --- SILVER PHASE (Cleansing & Rules) ---
    logging.info(f"✨ Executing Silver Transformations for: {targets['silver']}")
    silver_rules = pipeline_cfg.get("silver_rules", {})
    
    df = pd.read_sql(f"SELECT * FROM {targets['bronze']}", con=engine)
    
    # Apply rules from config
    if silver_rules.get("drop_columns"):
        df = df.drop(columns=silver_rules["drop_columns"], errors="ignore")
    if silver_rules.get("rename_columns"):
        df = df.rename(columns=silver_rules["rename_columns"])
    if silver_rules.get("uppercase_columns"):
        for col in silver_rules["uppercase_columns"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.upper()
                
    df.to_sql(targets["silver"], con=engine, if_exists="replace", index=False)
    logging.info(f"✅ Silver table '{targets['silver']}' successfully built and ready for inspection!")

if __name__ == "__main__":
    run_bronze_and_silver()