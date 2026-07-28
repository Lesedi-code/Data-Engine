import os
import sys
import yaml
import pandas as pd
from sqlalchemy import create_engine, text

def ensure_directories(dirs):
    """Ensure all required pipeline directories exist."""
    for d in dirs.values():
        if not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
            print(f"📁 Created directory: {d}")

def load_config(config_path="config.yaml"):
    """Load and parse the master pipeline configuration matrix."""
    if not os.path.exists(config_path):
        print(f"❌ Configuration file '{config_path}' not found.")
        sys.exit(1)
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def run_pipeline():
    print("===================================================")
    print("🚀 DATA ENGINE (Medallion Ingestion Pipeline)")
    print("===================================================")
    
    config = load_config()
    
    # 1. Self-healing directory check
    directories = config.get("directories", {
        "raw": "Data/raw",
        "silver": "Data/silver",
        "gold": "Data/gold"
    })
    ensure_directories(directories)
    
    # Ensure reports directory exists for EDA profiling
    if not os.path.exists("reports"):
        os.makedirs("reports", exist_ok=True)

    # 2. Extract sources and targets
    sources = config.get("sources", {})
    targets = config.get("targets", {})
    ingestion_rules = config.get("ingestion", [])
    
    # Target Postgres Connection
    pg_uri = targets.get("local_postgres", {}).get("connection_uri", config.get("connection_uri", "postgresql://localhost/postgres"))
    
    try:
        pg_engine = create_engine(pg_uri)
        print(f"✅ Connected to Target PostgreSQL: {pg_uri}")
    except Exception as e:
        print(f"❌ Failed to connect to Target PostgreSQL: {e}")
        sys.exit(1)

    # 3. Process Config-driven Ingestion Tasks (e.g. MySQL to Postgres)
    if ingestion_rules:
        for task in ingestion_rules:
            src_id = task.get("source_id")
            query = task.get("source_query")
            target_table = task.get("target_table")
            
            src_config = sources.get(src_id, {})
            if not src_config:
                print(f"⚠️ Source config '{src_id}' not found. Skipping task.")
                continue

            dialect = src_config.get("dialect", "mysql+pymysql")
            user = src_config.get("username", "")
            pwd = src_config.get("password", "")
            host = src_config.get("host", "localhost")
            port = src_config.get("port", 3306)
            db = src_config.get("database", "")

            mysql_uri = f"{dialect}://{user}:{pwd}@{host}:{port}/{db}"
            
            print(f"\n🔄 Executing Ingestion Task: {src_id} ➡️ {target_table}")
            try:
                src_engine = create_engine(mysql_uri)
                df = pd.read_sql_query(query, src_engine)
                print(f"📊 Pulled {len(df)} rows from source database.")
                
                # Stream to Postgres Bronze Staging
                df.to_sql(target_table, pg_engine, if_exists="replace", index=False)
                print(f"🎉 Successfully streamed matrix into Postgres table: '{target_table}'")
                
            except Exception as e:
                print(f"❌ Error during ingestion task '{target_table}': {e}")

    # 4. Process local flat files inside Data/raw if any exist
    raw_dir = directories.get("raw", "Data/raw")
    raw_files = [f for f in os.listdir(raw_dir) if not f.startswith(".")]
    
    if raw_files:
        print(f"\n📁 Found {len(raw_files)} local files in '{raw_dir}' for flat-file ingestion.")
        for file in raw_files:
            file_path = os.path.join(raw_dir, file)
            table_name = f"bronze_{os.path.splitext(file)[0].lower()}"
            try:
                if file.endswith(".csv"):
                    df = pd.read_csv(file_path)
                elif file.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(file_path)
                elif file.endswith(".json"):
                    df = pd.read_json(file_path)
                else:
                    continue
                
                df.to_sql(table_name, pg_engine, if_exists="replace", index=False)
                print(f"🎉 Streamed local file '{file}' into Postgres table: '{table_name}'")
            except Exception as e:
                print(f"⚠️ Failed to ingest local file '{file}': {e}")

    print("\n✅ Pipeline Execution Completed Successfully!")

if __name__ == "__main__":
    run_pipeline()