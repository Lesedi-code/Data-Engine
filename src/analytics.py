"""
===============================================================================
MODULE: Gold Aggregation Layer (Data Engine)
PURPOSE: Read cleansed Silver records, compute aggregated business metrics 
         based on gold_rules in pipeline.json, and store in PostgreSQL.
===============================================================================
"""

import os
import sys
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

# Ensure local imports work
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config_loader import load_config
from ingestion import get_db_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def run_gold_aggregation() -> pd.DataFrame:
    """
    Executes Gold Layer processing: reads Silver table, performs aggregations, writes to Gold table.
    """
    print("\n" + "="*50)
    print("🥇 PHASE 3: GOLD LAYER (ANALYTICS & METRICS)")
    print("="*50)

    # Load configuration
    config = load_config()
    db_config = config.get("database", {})
    active_key = config.get("active_source") or config.get("active_pipeline") or "primary_ingestion"
    sources_dict = config.get("pipelines") or config.get("data_sources") or {}

    pipeline_cfg = sources_dict.get(active_key, {})
    target_tables = pipeline_cfg.get("target_tables", {})

    silver_table = target_tables.get("silver") or f"silver_{active_key}"
    gold_table = target_tables.get("gold") or f"gold_{active_key}"
    gold_rules = pipeline_cfg.get ("gold_rules", {})

    engine = get_db_engine(db_config)

    # 1. Read cleansed dataset from Silver table
    logging.info(f"📖 Reading cleansed data from database table: '{silver_table}'...") 
    try:
        silver_df = pd.read_sql(f"SELECT * FROM {silver_table};", con=engine)
        logging.info(f"📊 Loaded {len(silver_df)} records from silver.")
    except Exception as e:
        raise RuntimeError(F"❌ Failed to read Silver table '{silver_table}': {e}")

    # 2. Perform GroupBy Aggregations according to rules
    group_cols = gold_rules.get("group_by_columns")

    if group_cols and any(col in silver_df.columns for col in group_cols):
        valid_groups = [c for c in group_cols if c in silver_df.columns]
        logging.info(f"📈 Grouping records by: {valid_groups}")

        num_cols = silver_df.select_dtypes(includ=['number']).columns.tolist()
        if "id" in num_cols:
            num_cols.remove("id") # Do not aggregate IDs

        if num_cols:
            gold_df = silver_df.groupby(valid_groups)[num_cols].agg(["mean", "count"]).reset_index()
            # Flatten multi-level columns
            gold_df.columns = ['_'.join(col).strip('_') for col in gold_df.column.values]
        else:
            gold_df = silver_df.groupby(valid_groups).size().reset_index(name="record_count")
    else:
        logging.info("ℹ️ No group_by_columns defines or found. Generating global summary statistics...")
        num_cols = silver_df.select_dtypes(include=['number']).columns.tolist()
        if "id" in num_cols:
            num_cols.remove("id")

        summary_dict = {"total_records": [len(silver_df)]}
        for c in num_cols:
            summary_dict[f"{c}_mean"] = [silver_df[c].mean()]
            summary_dict[f"{c}_min"] = [silver_df[c].min()]
            summary_dict[f"{c}_max"] = [silver_df[c].max()]

        gold_df =pd.DataFrame(summary_dict)

    # 3. Add audit timestamp
    gold_df["_aggregated_"] = datetime.now()   

    # 4. Write aggregated metrics to Gold table in PostgreSQL
    try:
        gold_df.to_sql (name=gold_table, con=engine, if_exists="replace", index=False)
        logging.info(f"✅ Successfully written gold metrics to table: '{gold_table}'")
    except Exception as e:
        logging.error(f"❌ Could not write to Gold table {e}")

    print("="*50 + "\n")
    return gold_df

if __name__ == "__main__":
    print("\n🚀 EXECUTING GOLD AGGREGATION TEST...")
    gold_df = run_gold_aggregation()
    print("Gold Sample Data preview:")
    print(gold_df.head())

    
