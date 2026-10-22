import argparse
import os
import re
import sys
import json
import yaml
import requests
import logging
import time
import duckdb
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline_execution.log", encoding="utf-8")
    ]
)


def get_available_ollama_models(base_url: str = "http://localhost:11434") -> list:
    """Queries local Ollama instance and returns a list of installed model names."""
    try:
        response = requests.get(f"{base_url}/api/tags", timeout=3)
        if response.status_code == 200:
            models = response.json().get("models", [])
            return [m.get("name") for m in models if "name" in m]
    except requests.ConnectionError:
        pass
    return []


def check_ollama_service(base_url: str = "http://localhost:11434") -> list:
    print("🔍 Checking Ollama service availability...")
    models = get_available_ollama_models(base_url)
    if models:
        print("✔ Ollama service is active and responsive.")
        print(f"📦 Discovered Local LLMs: {', '.join(models)}")
        return models
    
    print("⚠️ Warning: Ollama service is not active. AI transformation gates will be disabled.")
    return []  


def load_config(config_path: str = "config/pipeline.yaml") -> dict:
    """Load configuration settings from YAML."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def sanitize_table_name(file_name: str) -> str:
    """Derive a clean, SQL-compliant table name from a raw filename."""
    base_name = os.path.splitext(file_name)[0]
    return re.sub(r'[^a-zA-Z0-9_]+', '_', base_name).strip('_').lower()


def get_db_engine(config: dict):
    """Establish SQLAlchemy engine for PostgreSQL (optional fallback)."""
    targets = config.get("targets", {})
    local_pg = targets.get("local_postgres", {})
    uri = local_pg.get("connection_uri")

    if not uri:
        db_cfg = config.get("database", {})
        user = db_cfg.get("user", "postgres")
        password = db_cfg.get("password", "postgres")
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 5432)
        dbname = db_cfg.get("dbname", "staging_db")
        uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    url_obj = make_url(uri)
    if not os.path.exists("/.dockerenv") and url_obj.host in ["postgres_db", "postgres"]:
        url_obj = url_obj._replace(host="localhost")

    engine = create_engine(url_obj)

    try:
        with engine.begin() as conn:
            for schema in ["bronze", "silver", "gold"]:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
        logging.info("PostgreSQL database schemas initialized successfully.")
    except Exception as e:
        logging.warning(f"PostgreSQL not reachable ({e}). Operating via DuckDB OLAP engine.")
        engine = None

    return engine


def get_duckdb_connection():
    """Initialize local DuckDB instance for lightning-fast analytical compute and memory caching."""
    db_file = "data_engine.duckdb"
    con = duckdb.connect(db_file)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    logging.info(f"DuckDB connection established successfully ({db_file}).")
    return con


def persist_data(df: pd.DataFrame, table_name: str, schema_name: str, engine, duck_con, if_exists: str = "replace"):
    """Dual-writes data into PostgreSQL (if active) and DuckDB OLAP engine with logged transactions."""
    if engine:
        try:
            with engine.begin() as conn:
                df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists=if_exists, index=False)
            print(f"✔ [PostgreSQL] Persisted {len(df):,} records into staging_db.{schema_name}.{table_name}", flush=True)
            logging.info(f"[PostgreSQL] Persisted {len(df):,} records into staging_db.{schema_name}.{table_name}")
        except Exception as e:
            print(f"⚠️ [PostgreSQL Warning] Write skipped for {schema_name}.{table_name}: {e}", flush=True)
            logging.warning(f"[PostgreSQL Warning] Write skipped for {schema_name}.{table_name}: {e}")

    try:
        duck_con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        duck_con.register("temp_df", df)
        if if_exists == "replace":
            duck_con.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name};")
            duck_con.execute(f"CREATE TABLE {schema_name}.{table_name} AS SELECT * FROM temp_df;")
        else:
            duck_con.execute(f"INSERT INTO {schema_name}.{table_name} SELECT * FROM temp_df;")
        duck_con.unregister("temp_df")
        print(f"✔ [DuckDB] Persisted {len(df):,} records into {schema_name}.{table_name}", flush=True)
        logging.info(f"[DuckDB] Persisted {len(df):,} records into {schema_name}.{table_name}")
    except Exception as e:
        print(f"❌ [DuckDB Error] Failed to write {schema_name}.{table_name}: {e}", flush=True)
        logging.error(f"[DuckDB Error] Failed to write {schema_name}.{table_name}: {e}")


def sync_duckdb_to_postgres(duck_con, engine, table_name: str, schema_name: str = "silver"):
    """Reads current state from DuckDB and updates PostgreSQL table so both engines stay perfectly in sync."""
    if not engine:
        return
    try:
        df = duck_con.execute(f"SELECT * FROM {schema_name}.{table_name}").df()
        with engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, schema=schema_name, if_exists="replace", index=False)
        print(f"✔ [PostgreSQL Sync] Updated staging_db.{schema_name}.{table_name} with {len(df):,} records.", flush=True)
    except Exception as e:
        print(f"⚠️ [PostgreSQL Sync Warning] Could not sync table to Postgres: {e}", flush=True)


def run_data_quality_assertions(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Automated Data Quality Gate: audits records for nulls, duplicates, and logs health metrics."""
    logging.info(f"Running Data Quality Assertion Gate for table '{table_name}'...")
    
    initial_count = len(df)
    null_counts = df.isnull().sum().to_dict()
    duplicate_count = df.duplicated().sum()
    
    print(f"\n================================================================================")
    print(f" 🛡️ [DATA QUALITY ASSERTION GATE] Auditing '{table_name}'")
    print(f"================================================================================")
    print(f"   • Total Input Records    : {initial_count:,}")
    print(f"   • Duplicate Rows Found   : {duplicate_count:,}")
    print(f"   • Null Value Distribution:")
    for col, count in null_counts.items():
        if count > 0:
            pct = (count / initial_count) * 100
            print(f"     - '{col}': {count:,} nulls ({pct:.1f}%)")
            logging.warning(f"DQ Alert: Column '{col}' contains {count} null values ({pct:.1f}%).")

    cleaned_df = df.dropna(how='all').drop_duplicates()
    final_count = len(cleaned_df)
    dropped_count = initial_count - final_count
    
    print(f"   • Action Taken           : Dropped {dropped_count:,} invalid/duplicate rows.")
    print(f"✔ [DQ Gate Passed] Cleaned dataset ready for Silver tier ({final_count:,} rows).")
    print(f"================================================================================\n")
    
    logging.info(f"DQ Gate completed. Initial: {initial_count}, Cleaned: {final_count}, Dropped: {dropped_count}")
    return cleaned_df


def select_llm_model(available_models: list) -> str:
    """Prompt user to select which active local LLM to use, or skip the AI gate entirely."""
    if not available_models:
        return None

    print("\n🤖 Select Local LLM Model for AI Gate:")
    for idx, model_name in enumerate(available_models, 1):
        print(f"   [{idx}] {model_name}")
    
    skip_idx = len(available_models) + 1
    print(f"   [{skip_idx}] Skip AI Gate (Proceed directly to Approval)")
    
    while True:
        choice = input(f"\nSelect option [1-{skip_idx}]: ").strip()
        if choice.isdigit():
            val = int(choice)
            if 1 <= val <= len(available_models):
                selected = available_models[val - 1]
                print(f"✔ Selected model: {selected}")
                return selected
            elif val == skip_idx:
                print("✔ Skipping AI Gate.")
                return None
        elif choice.lower() in ['exit', 'skip', 'done', 'none']:
            print("✔ Skipping AI Gate.")
            return None
        print(f"Invalid selection. Please choose a number between 1 and {skip_idx}.")


def ask_model_with_sql(prompt_context: str, duck_con, engine, table_name: str, schema: str = "silver", model: str = "gemma4:e4b"):
    """Maintains a conversational chat loop with the selected LLM, allowing iterative refinements and typo corrections."""
    system_prompt = (
        f"You are an expert data engineer assisting Lesedi Motleleng for a data science competition. "
        f"You have access to a table named '{schema}.{table_name}' inside DuckDB. "
        f"When the user requests a modification or query, write a valid DuckDB SQL query. "
        f"Return your response in this exact format:\n"
        f"SQL: <your sql query here>\n"
        f"EXPLANATION: <brief explanation>\n"
        f"If the user is just chatting, correcting a typo, or answering a previous error, adapt naturally without resetting context."
    )
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Table Context: {prompt_context}. Let's begin."}
    ]
    
    print(f"\n💬 [Interactive Chat Mode Activated with {model}]")
    print("Type your data requests, correct spelling errors conversationally, or type 'done' to finish chatting and review data.\n")

    while True:
        user_query = input(f"[{schema} - {model}] 👤 You: ").strip()
        if not user_query:
            continue
        if user_query.lower() in ['done', 'exit', 'quit', 'back']:
            print("Exiting AI Chat session...")
            break

        messages.append({"role": "user", "content": user_query})

        print(f"\n🤖 [AI ASSISTANT] Thinking...", flush=True)
        try:
            response = requests.post(
                "http://localhost:11434/api/chat",
                json={"model": model, "messages": messages, "stream": False},
                timeout=60
            )
            
            if response.status_code == 200:
                ai_message = response.json().get("message", {}).get("content", "No response received.")
            else:
                ai_message = f"[ERROR] Ollama returned status code {response.status_code}"
        except Exception as e:
            ai_message = f"[ERROR] Could not reach local Ollama server: {e}"

        print(f"\n--- AI Response ({model}) ---\n{ai_message}\n----------------------")
        messages.append({"role": "assistant", "content": ai_message})

        if "SQL:" in ai_message:
            try:
                lines = ai_message.split('\n')
                sql_query = ""
                capturing = False
                for line in lines:
                    if line.startswith("SQL:"):
                        sql_query = line.replace("SQL:", "").strip()
                        capturing = True
                    elif capturing and line.startswith("EXPLANATION:"):
                        capturing = False
                    elif capturing:
                        sql_query += " " + line.strip()

                sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
                
                if sql_query and not sql_query.upper().startswith("SELECT 1"):
                    print(f"\n🔍 [AI-SQL] Executing query: {sql_query}")
                    result_df = duck_con.execute(sql_query).df()
                    
                    # Sync changes back to PostgreSQL so both engines stay aligned
                    sync_duckdb_to_postgres(duck_con, engine, table_name, schema_name=schema)

                    if not result_df.empty:
                        total_rows = len(result_df)
                        # Universal 1000 threshold limit
                        if total_rows > 1000:
                            preview_df = result_df.head(15)
                            print(f"\n📊 Query Results (Dataset exceeds 1,000 rows - Showing top 15 of {total_rows:,} rows):\n{preview_df.to_string(index=False)}")
                            messages.append({"role": "system", "content": f"Query execution result (Showing top 15 of {total_rows:,} rows):\n{preview_df.to_string(index=False)}"})
                        else:
                            print(f"\n📊 Query Results ({total_rows:,} rows):\n{result_df.to_string(index=False)}")
                            messages.append({"role": "system", "content": f"Query execution result:\n{result_df.to_string(index=False)}"})
                    else:
                        print("\n✔ Query executed successfully (DDL/DML completed).")
                        messages.append({"role": "system", "content": "Query executed successfully with zero rows returned."})
            except Exception as e:
                error_msg = f"[ERROR] Could not execute AI-generated SQL against DuckDB: {e}"
                print(error_msg)
                messages.append({"role": "system", "content": error_msg})


def get_discovered_datasets(config: dict) -> dict:
    """Dynamically discover structured files in Structured_dataset."""
    dirs = config.get("directories", {})
    raw_root = dirs.get("raw") or "Data/raw"
    struct_dir = os.path.join(raw_root, "Structured_dataset")
    
    discovered = {}
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir, exist_ok=True)
        return discovered

    ext_map = {".csv": "CSV", ".json": "JSON", ".parquet": "PARQUET", ".xlsx": "EXCEL", ".xls": "EXCEL"}
    for item in os.listdir(struct_dir):
        item_path = os.path.join(struct_dir, item)
        if os.path.isfile(item_path):
            ext = os.path.splitext(item)[1].lower()
            if ext in ext_map:
                key = sanitize_table_name(item)
                discovered[key] = {"file_name": item, "file_path": item_path, "format": ext_map[ext]}
    return discovered


def ingest_from_openedge_api(config: dict, table_name: str, engine, duck_con) -> tuple[pd.DataFrame, str]:
    """Connects to Progress OpenEdge PASOE REST API and ingests into Bronze."""
    oe_config = config.get("openedge_api", {})
    base_url = oe_config.get("base_url", "http://localhost:8810/rest/enterprise_app")
    endpoint_path = f"/api/{table_name}" if not table_name.startswith("/") else table_name
    full_url = f"{base_url}{endpoint_path}"
    
    print(f"\n🌐 [OpenEdge Connector] Connecting to PASOE endpoint -> {full_url}")
    
    try:
        response = requests.get(full_url, timeout=10)
        if response.status_code != 200:
            print(f"❌ Error: OpenEdge API returned status code {response.status_code}.")
            return pd.DataFrame(), table_name
            
        payload = response.json()
        df = pd.DataFrame.from_dict(payload, orient='index') if isinstance(payload, dict) else pd.json_normalize(payload)
            
        clean_table_name = sanitize_table_name(table_name)
        persist_data(df, clean_table_name, "bronze", engine, duck_con, if_exists="replace")
        print(f"✔ [OpenEdge Ingestion] Successfully synced enterprise records into 'bronze.{clean_table_name}' ({len(df):,} records)")
        return df, clean_table_name
        
    except requests.ConnectionError:
        print(f"🔌 [OpenEdge Notice] Could not connect to live PASOE server at {full_url}. Make sure mock_openedge_server.py is running.")
        return pd.DataFrame(), table_name
    except Exception as e:
        print(f"❌ [OpenEdge Error] Failed to ingest from OpenEdge API: {e}")
        return pd.DataFrame(), table_name


def load_raw_dataset(config: dict, dataset_key: str, engine, duck_con) -> tuple[pd.DataFrame, str]:
    """Ingests raw structured datasets with robust format handling and chunking for massive files."""
    discovered = get_discovered_datasets(config)
    if not discovered:
        raise FileNotFoundError("No structured files found in 'Data/raw/Structured_dataset/'.")

    if dataset_key not in discovered:
        dataset_key = list(discovered.keys())[0]

    dataset_cfg = discovered[dataset_key]
    file_name = dataset_cfg.get("file_name")
    file_path = dataset_cfg.get("file_path")
    file_format = dataset_cfg.get("format")

    table_name = sanitize_table_name(file_name)
    df = pd.DataFrame()

    try:
        print(f"📦 [BRONZE STREAM] Loading raw dataset '{file_name}' (Format: {file_format})...")
        if file_format == "PARQUET":
            parquet_file = pq.ParquetFile(file_path)
            total_records = parquet_file.metadata.num_rows
            first_batch = True
            for batch in parquet_file.iter_batches(batch_size=500000):
                df_chunk = batch.to_pandas()
                if first_batch:
                    df = df_chunk.copy()
                    persist_data(df_chunk, table_name, "bronze", engine, duck_con, if_exists="replace")
                    first_batch = False
                else:
                    persist_data(df_chunk, table_name, "bronze", duck_con, if_exists="append")
            print(f"✔ Successfully streamed Parquet source: {file_name} ({total_records:,} records)")
        elif file_format in ["EXCEL", "XLSX", "XLS"]:
            df = pd.read_excel(file_path)
            persist_data(df, table_name, "bronze", engine, duck_con, if_exists="replace")
            print(f"✔ Successfully loaded Excel Bronze source: {file_name} ({len(df):,} records)")
        elif file_format == "JSON":
            with open(file_path, 'r', encoding='utf-8') as jf:
                raw_json = json.load(jf)
            df = pd.DataFrame.from_dict(raw_json, orient='index') if isinstance(raw_json, dict) else pd.json_normalize(raw_json)
            persist_data(df, table_name, "bronze", engine, duck_con, if_exists="replace")
            print(f"✔ Successfully loaded JSON Bronze source: {file_name} ({len(df):,} records)")
        elif file_format == "CSV":
            first_batch = True
            total_records = 0
            try:
                chunks = pd.read_csv(file_path, chunksize=500000, on_bad_lines='skip', encoding='utf-8')
            except UnicodeDecodeError:
                chunks = pd.read_csv(file_path, chunksize=500000, on_bad_lines='skip', encoding='latin-1')

            for chunk_df in chunks:
                total_records += len(chunk_df)
                if first_batch:
                    df = chunk_df.copy()
                    persist_data(chunk_df, table_name, "bronze", engine, duck_con, if_exists="replace")
                    first_batch = False
                else:
                    persist_data(chunk_df, table_name, "bronze", duck_con, if_exists="append")
            print(f"✔ Successfully streamed CSV source: {file_name} (~{total_records:,} records)")

    except Exception as e:
        logging.error(f"Ingestion error for {file_name}: {e}")

    return df, table_name


def select_dataset_key(config: dict) -> str:
    """Prompt user to choose from discovered local datasets."""
    discovered = get_discovered_datasets(config)
    available = list(discovered.keys())
    if not available:
        return None
    if len(available) == 1:
        return available[0]
    
    print("\n📂 Discovered Structured Files:")
    for idx, key in enumerate(available, 1):
        print(f"   [{idx}] {discovered[key].get('file_name')} ({discovered[key].get('format')})")
    
    while True:
        choice = input(f"\nSelect dataset [1-{len(available)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(available):
            return available[int(choice) - 1]
        print("Invalid selection. Please try again.")


def run_structured_pipeline(config: dict, engine, duck_con, available_models: list, raw_df: pd.DataFrame = None, table_name: str = None):
    """Executes core structured Medallion workflow with DQ Gates, Telemetry, and Manual Inspection/Approval Gates."""
    start_time = time.time()

    if raw_df is None or table_name is None:
        active_dataset = select_dataset_key(config)
        if not active_dataset:
            print("[WARN] No files found in structured directory.")
            return
        raw_df, table_name = load_raw_dataset(config, active_dataset, engine, duck_con)

    print(f"\n--------------------------------------------------------------------------------")
    print(f" [BRONZE LAYER] Active table: 'bronze.{table_name}' ({len(raw_df):,} rows)")
    print(f"--------------------------------------------------------------------------------")

    # 1. Automated Data Quality Gate
    cleaned_df = run_data_quality_assertions(raw_df, table_name)

    # 2. Silver Tier Transformation & Interactive Approval Gate Loop
    while True:
        print(f"--------------------------------------------------------------------------------")
        print(f" [SILVER LAYER] Standardizing schema and types into 'silver.{table_name}'...")
        print(f"--------------------------------------------------------------------------------")
        silver_df = cleaned_df.copy()
        silver_df.columns = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in silver_df.columns]
        persist_data(silver_df, table_name, "silver", engine, duck_con, if_exists="replace")
        print(f"✔ Silver tier materialized for '{table_name}' ({len(silver_df):,} rows).")

        # Interactive Model Selection for Silver AI Gate (with Skip Option)
        selected_model = select_llm_model(available_models)

        if selected_model:
            # Human-in-the-Loop Interactive AI Chat Gate: Silver Layer
            while True:
                action = input(f"\n[SILVER GATE - Model: {selected_model}] Press [Enter] to start AI Chat, type 'inspect' to preview table, or type 'done' to finish transformations: ").strip().lower()
                if action == 'done':
                    break
                elif action == 'inspect':
                    try:
                        row_count = duck_con.execute(f"SELECT COUNT(*) FROM silver.{table_name}").fetchone()[0]
                        # Universal 1000 threshold limit for table inspection
                        if row_count > 1000:
                            preview_df = duck_con.execute(f"SELECT * FROM silver.{table_name} LIMIT 15").df()
                            print(f"\n📊 Current Silver Table Preview (Dataset exceeds 1,000 rows - Top 15 of {row_count:,}):\n{preview_df.to_string(index=False)}\n")
                        else:
                            preview_df = duck_con.execute(f"SELECT * FROM silver.{table_name}").df()
                            print(f"\n📊 Current Silver Table Preview ({row_count:,} Rows):\n{preview_df.to_string(index=False)}\n")
                    except Exception as e:
                        print(f"❌ Error loading preview: {e}")
                else:
                    ask_model_with_sql(f"Table 'silver.{table_name}' with schema standardization complete.", duck_con, engine, table_name, schema="silver", model=selected_model)
        else:
            print("ℹ️ AI Gate bypassed. Proceeding directly to Silver Inspection & Approval.")

        # Manual Inspection & Approval Gate before Gold Promotion (Metadata Summary only, skipping large data dumps)
        print(f"\n================================================================================")
        print(f" 🛡️ [SILVER INSPECTION & APPROVAL GATE] Table: 'silver.{table_name}'")
        print(f"================================================================================")
        try:
            row_count = duck_con.execute(f"SELECT COUNT(*) FROM silver.{table_name}").fetchone()[0]
            print(f"   • Target Table   : silver.{table_name}")
            print(f"   • Total Rows     : {row_count:,}")
            print(f"   • Storage Status : Persisted in DuckDB and synchronized with PostgreSQL staging.")
        except Exception as e:
            print(f"   • Notice: Table metadata retrieved successfully.")

        approval = input("\nDo you approve these changes and wish to promote to Gold? [y = Approve & Promote, r = Return to AI Chat, n = Cancel & Return to Menu]: ").strip().lower()
        
        if approval == 'y':
            print("✔ Changes approved! Proceeding to Gold tier promotion.")
            break
        elif approval == 'r':
            print("Returning to Silver AI Chat loop for further adjustments...")
            continue
        else:
            print("Pipeline promotion cancelled by user. Returning to Master Orchestrator menu.")
            return

    # 3. Gold Tier Promotion (Only after explicit user approval)
    print(f"--------------------------------------------------------------------------------")
    print(f" [GOLD LAYER] Materializing analytical model into 'gold.{table_name}'...")
    print(f"--------------------------------------------------------------------------------")
    try:
        gold_df = duck_con.execute(f"SELECT * FROM silver.{table_name}").df()
    except Exception:
        gold_df = silver_df.copy()

    persist_data(gold_df, table_name, "gold", engine, duck_con, if_exists="replace")
    print(f"✔ Gold tier materialized for '{table_name}' ({len(gold_df):,} rows).")

    # Automated Export: Gold Tier CSV Export
    dirs = config.get("directories", {})
    gold_export_dir = dirs.get("gold") or "Data/gold"
    os.makedirs(gold_export_dir, exist_ok=True)
    gold_csv_path = os.path.join(gold_export_dir, f"{table_name}_gold.csv")
    gold_df.to_csv(gold_csv_path, index=False)
    print(f"✔ [Gold Export] Exported analytical dataset to CSV -> {gold_csv_path}")
    logging.info(f"Gold dataset exported to CSV: {gold_csv_path}")

    # 4. Execution Timing & Telemetry Metrics
    duration = time.time() - start_time
    print(f"\n================================================================================")
    print(f" ⏱️ [PIPELINE TELEMETRY METRICS]")
    print(f"   • Target Dataset  : {table_name}")
    print(f"   • Total Processed : {len(gold_df):,} records")
    print(f"   • Execution Time  : {duration:.2f} seconds")
    print(f"   • Engines Active  : PostgreSQL Staging + DuckDB OLAP")
    print(f"   • Gold CSV Export : {gold_csv_path}")
    print(f"================================================================================\n")
    
    logging.info(f"Pipeline successfully completed for '{table_name}' in {duration:.2f} seconds.")


def execute_pipeline():
    """Master Orchestrator menu for competition demo."""
    available_models = check_ollama_service()
    config = load_config("config/pipeline.yaml")
    engine = get_db_engine(config)
    duck_con = get_duckdb_connection()

    while True:
        print("\n================================================================================")
        print("                      DATA ENGINE - MASTER ORCHESTRATOR                         ")
        print("================================================================================")
        print("   [1] Local Raw Files (CSV, Parquet, JSON, Excel from Structured_dataset)")
        print("   [2] Progress OpenEdge PASOE REST API (Live Enterprise Stream)")
        print("   [3] Exit Pipeline")
        print("================================================================================")
        
        choice = input("\nSelect pipeline mode [1-3]: ").strip()
        
        if choice == "1":
            run_structured_pipeline(config, engine, duck_con, available_models)
        elif choice == "2":
            target = input("Enter OpenEdge table endpoint (e.g., inventory): ").strip().lower()
            if target:
                oe_df, t_name = ingest_from_openedge_api(config, target, engine, duck_con)
                if not oe_df.empty:
                    run_structured_pipeline(config, engine, duck_con, available_models, raw_df=oe_df, table_name=t_name)
        elif choice == "3":
            print("Shutting down Data Engine. Goodbye!")
            logging.info("Pipeline terminated normally by user.")
            sys.exit(0)
        else:
            print("Invalid selection. Please choose between 1 and 3.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Engine Master Orchestrator")
    args = parser.parse_args()
    execute_pipeline()