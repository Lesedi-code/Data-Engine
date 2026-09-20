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

def run_gold():
    targets = pipeline_cfg["target_tables"]
    gold_rules = pipeline_cfg.get("gold_rules", {})
    
    logging.info(f"🏆 Building Gold Aggregations from: {targets['silver']}")
    df = pd.read_sql(f"SELECT * FROM {targets['silver']}", con=engine)
    
    group_cols = gold_rules.get("group_by_columns", [])
    if group_cols and all(col in df.columns for col in group_cols):
        # Simple aggregation example based on config
        df_gold = df.groupby(group_cols).size().reset_index(name="record_count")
    else:
        df_gold = df # Fallback if no specific grouping is set

    df_gold.to_sql(targets["gold"], con=engine, if_exists="replace", index=False)
    logging.info(f"✅ Gold table '{targets['gold']}' successfully created!")

if __name__ == "__main__":
    run_gold()