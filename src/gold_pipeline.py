import os
import sys
import pandas as pd
import yaml
from sqlalchemy import create_engine

# Ensure project root is in Python path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def load_config():
    """Loads configuration settings from config.yaml"""
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError("🛑 'config.yaml' not found in project root.")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def run_gold_pipeline():
    print("\n===================================================")
    print("🏆 GOLD PHASE: AUTOMATED DATABASE FETCH & AGGREGATION")
    print("===================================================")

    config = load_config()
    db_uri = config.get("connection_uri")
    silver_dir = "Data/silver"
    silver_csv_path = os.path.join(silver_dir, "clean_client_test.csv")
    gold_dir = config.get("directories", {}).get("gold", "Data/gold")

    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)

    # 1. Direct Pull from Postgres Silver Table
    print("🔌 Querying Postgres table: 'silver_dirty_client_test'...")
    engine = create_engine(db_uri)
    
    query = "SELECT * FROM silver_dirty_client_test ORDER BY id;"
    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️ 'silver_dirty_client_test' is empty. Gold phase skipped.")
        return

    # 2. Export Clean Silver CSV (Includes proper headers automatically)
    df.to_csv(silver_csv_path, index=False)
    print(f"💾 Synced Silver CSV with headers to: {silver_csv_path}")

    # 3. Gold Aggregations
    print("⚡ Computing Business Aggregations...")
    tier_summary = (
        df.groupby("target_tier_clean")
        .agg(
            total_clients=("id", "count"),
            latest_join_date=("join_date_clean", "max")
        )
        .reset_index()
    )

    # 4. Export Gold Artifacts
    output_gold_csv = os.path.join(gold_dir, "gold_client_tier_summary.csv")
    tier_summary.to_csv(output_gold_csv, index=False)
    print(f"💾 Gold Summary exported locally to: {output_gold_csv}")

    # 5. Stream Gold Table back to Postgres
    tier_summary.to_sql("gold_client_tier_summary", engine, if_exists="replace", index=False)
    print("✨ Gold aggregation streamed to Postgres table: 'gold_client_tier_summary'")

    print("===================================================")
    print("🏁 Full Silver-to-Gold flow completed seamlessly!\n")


if __name__ == "__main__":
    run_gold_pipeline()