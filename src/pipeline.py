import os
import sys
import glob
import re
import pandas as pd
from sqlalchemy import create_engine
import yaml

import os
import sys

# Get the absolute path to the root 'Data Factory' folder
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Now Python can find 'templates' at the root level
from templates.profiler import DiscoveryProfiler
# Import your profiler from templates
from templates.profiler import DiscoveryProfiler


def load_config():
    """Loads configuration settings from config.yaml"""
    config_path = "config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError("🛑 'config.yaml' not found in project root.")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def sanitize_table_name(filename):
    """Converts a raw filename into a clean PostgreSQL table name."""
    name_without_ext = os.path.splitext(filename)[0]
    # Keep alphanumeric characters and underscores, lowercased
    clean_name = re.sub(r"\W+", "_", name_without_ext).strip("_").lower()
    return f"bronze_{clean_name}"


def run_pipeline():
    print("\n===================================================")
    print("🚀 DATA ENGINE (Agnostic Dynamic Ingestion)")
    print("===================================================")

    config = load_config()
    db_uri = config.get("connection_uri")
    raw_dir = config.get("directories", {}).get("raw", "Data/raw")

    if not os.path.exists(raw_dir):
        print(f"⚠️ Raw directory '{raw_dir}' does not exist.")
        return

    # Find all files in raw directory
    all_files = glob.glob(os.path.join(raw_dir, "*"))
    files_to_process = [
        f for f in all_files if not os.path.basename(f).startswith("~$")
    ]

    if not files_to_process:
        print(f"📂 No valid files found in '{raw_dir}'. Pipeline complete.\n")
        return

    print(
        f"📂 Found {len(files_to_process)} file(s) ready for ingestion in '{raw_dir}'\n"
    )

    engine = create_engine(db_uri)

    for file_path in files_to_process:
        file_name = os.path.basename(file_path)
        ext = os.path.splitext(file_name)[1].lower()

        print(f"📦 INGESTING FILE: {file_name}")

        try:
            # 1. Load Data
            if ext == ".csv":
                df = pd.read_csv(file_path)
            elif ext in [".xlsx", ".xls"]:
                df = pd.read_excel(file_path, engine="openpyxl")
            elif ext == ".parquet":
                df = pd.read_parquet(file_path)
            elif ext == ".json":
                try:
                    df = pd.read_json(file_path)
                except ValueError:
                    df = pd.read_json(file_path, lines=True)
            else:
                print(f"⏩ Skipping unsupported file type: {ext}\n")
                continue

            # 2. Derive Table Name & Load to Postgres
            table_name = sanitize_table_name(file_name)
            print(f"🎯 Target Table: '{table_name}'")

            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(
                f"   ✨ SUCCESS: Clean memory matrix streamed into '{table_name}'."
            )

            # 3. Profiler Methods (Matched to profiler.py)
            profiler = DiscoveryProfiler(output_dir="reports")
            profiler.analyze_raw_shape(df, table_name)
            profiler.analyze_sparsity_and_distribution(df, table_name)
            print("---------------------------------------------------\n")

        except Exception as e:
            print(f"❌ Failed to process '{file_name}': {e}\n")

    print("===================================================")
    print("🔌 Postgres connection pool closed cleanly.")
    print("🏁 Framework execution terminated safely.\n")


if __name__ == "__main__":
    run_pipeline()