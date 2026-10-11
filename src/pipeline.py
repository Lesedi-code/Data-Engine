import argparse
import os
import re
import sys
import json
import traceback
import pandas as pd
import pyarrow.parquet as pq
import yaml
import requests
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

# Configure logging for robust trace tracking
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Import the multimodal unstructured engine if available
try:
    from unstructured_engine import process_unstructured_medallion
except ImportError:
    process_unstructured_medallion = None


def check_ollama_service(base_url: str = "http://localhost:11434") -> None:
    """Verifies that the local Ollama instance is up and running before pipeline execution."""
    print("🔍 Checking Ollama service availability...")
    try:
        response = requests.get(f"{base_url}/api/tags", timeout=3)
        if response.status_code == 200:
            print("✔ Ollama service is active and responsive.")
            return
    except requests.ConnectionError:
        pass
    
    print("❌ Error: Ollama service is not running. Please start it using 'ollama serve' in a separate terminal.")
    sys.exit(1)


def load_config(config_path: str = "config/pipeline.yaml") -> dict:
    """Load configuration settings from YAML."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def sanitize_table_name(file_name: str) -> str:
    """Derive a clean, SQL-compliant PostgreSQL table name from a raw filename."""
    base_name = os.path.splitext(file_name)[0]
    return re.sub(r'[^a-zA-Z0-9_]+', '_', base_name).strip('_').lower()


def get_db_engine(config: dict):
    """Establish SQLAlchemy engine and ensure core structured schemas exist in PostgreSQL."""
    targets = config.get("targets", {})
    local_pg = targets.get("local_postgres", {})
    uri = local_pg.get("connection_uri")

    if not uri:
        db_cfg = config.get("database", {})
        user = db_cfg.get("user", "engine_user")
        password = db_cfg.get("password", "engine_password_123")
        host = db_cfg.get("host", "postgres_db")
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
    except Exception as e:
        print(f"[ERROR] Failed to initialize database schemas: {e}")
        sys.exit(1)

    return engine


def persist_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str, engine, if_exists: str = "replace"):
    """Write DataFrame directly into PostgreSQL schema with explicit transaction safety."""
    if not engine:
        return
    try:
        with engine.begin() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema_name,
                if_exists=if_exists,
                index=False
            )
        print(f"✔ PostgreSQL Persisted -> staging_db.{schema_name}.{table_name}", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to write to PostgreSQL schema '{schema_name}.{table_name}': {e}", flush=True)
        sys.exit(1)


def execute_ai_transformation(cursor_or_conn, sql_code: str, engine=None) -> bool:
    """
    Executes AI-generated SQL with transaction safety, automatic rollbacks on error,
    and an interactive recovery prompt for the developer/tester.
    """
    try:
        print(f"\n🤖 [AI Transformation Gate] Executing SQL:\n{sql_code}\n")
        
        if engine:
            with engine.begin() as conn:
                for stmt in sql_code.split(";"):
                    if stmt.strip():
                        conn.execute(text(stmt))
        else:
            for stmt in sql_code.split(";"):
                if stmt.strip():
                    cursor_or_conn.execute(text(stmt))
            cursor_or_conn.commit()
            
        print("✔ AI-driven transformation successfully committed to database.")
        return True
        
    except Exception as e:
        print(f"\n❌ Database Execution Error: {e}")
        print("🔒 Transaction rolled back successfully. No corrupted state saved.")
        
        while True:
            choice = input("\nChoose an option - (r)etry with custom SQL, (s)kip step, or (q)uit: ").strip().lower()
            if choice == 'r':
                manual_sql = input("Enter corrected SQL query: ")
                return execute_ai_transformation(cursor_or_conn, manual_sql, engine)
            elif choice == 's':
                print("⚠️ Skipping this AI transformation gate.")
                return False
            elif choice == 'q':
                print("Shutting down pipeline.")
                sys.exit(1)
            else:
                print("Invalid choice. Please select r, s, or q.")


def call_gemma_assistant(prompt: str, model: str = "gemma4:e4b") -> str:
    """Query local Ollama instance running Gemma for data engineering assistance."""
    print(f"\n🤖 [GEMMA AI] Querying local model ({model})...", flush=True)
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False
            },
            timeout=60
        )
        if response.status_code == 200:
            return response.json().get("response", "No response received.")
        else:
            return f"[ERROR] Ollama returned status code {response.status_code}"
    except Exception as e:
        return f"[ERROR] Could not reach local Ollama server: {e}"


def ask_gemma_with_sql(prompt_context: str, user_query: str, engine, table_name: str, schema: str = "bronze"):
    """Allows Gemma to write SQL, execute it against PostgreSQL, and answer the user."""
    system_prompt = f"""
You are an expert PostgreSQL data engineer assisting Lesedi Motleleng.
You have access to a PostgreSQL table named '{schema}.{table_name}'.
The user will ask a question. If the question requires inspecting the data (like finding nulls, counts, or values), write a valid SQL query to find the answer.
Return your response in this exact format:
SQL: <your sql query here>
EXPLANATION: <brief explanation of what the query does>

If no SQL is needed, leave the SQL: line blank or write SELECT 1; and provide your answer in the explanation.
"""
    full_prompt = f"{system_prompt}\nTable Context: {prompt_context}\nUser Question: {user_query}\nResponse:"
    
    ai_response = call_gemma_assistant(full_prompt)
    print(f"\n--- Gemma Reasoning ---\n{ai_response}\n----------------------")

    if "SQL:" in ai_response:
        try:
            lines = ai_response.split('\n')
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
                with engine.begin() as conn:
                    result_df = pd.read_sql(text(sql_query), conn)
                print(f"\n📊 Query Results:\n{result_df.to_string(index=False)}")
                
                followup_prompt = f"The user asked: '{user_query}'. I ran the SQL query and got these results:\n{result_df.to_string(index=False)}\nProvide a concise final answer."
                final_answer = call_gemma_assistant(followup_prompt)
                print(f"\n🤖 [GEMMA FINAL ANSWER]\n{final_answer}")
        except Exception as e:
            print(f"[ERROR] Could not execute AI-generated SQL against PostgreSQL: {e}")


def get_discovered_datasets(config: dict) -> dict:
    """Dynamically discover all files in the Structured_dataset directory with exact robust paths."""
    dirs = config.get("directories", {})
    raw_root = dirs.get("raw") or dirs.get("raw_data", "Data/raw")
    struct_dir = os.path.join(raw_root, "Structured_dataset")
    
    discovered = {}
    if not os.path.exists(struct_dir):
        os.makedirs(struct_dir, exist_ok=True)
        return discovered

    ext_map = {
        ".csv": "CSV",
        ".json": "JSON",
        ".parquet": "PARQUET",
        ".xlsx": "EXCEL",
        ".xls": "EXCEL"
    }

    for item in os.listdir(struct_dir):
        item_path = os.path.join(struct_dir, item)
        if os.path.isfile(item_path):
            ext = os.path.splitext(item)[1].lower()
            if ext in ext_map:
                key = sanitize_table_name(item)
                discovered[key] = {
                    "file_name": item,
                    "file_path": item_path,
                    "format": ext_map[ext],
                    "primary_key": "id" if "id" in item else None
                }
    return discovered


def load_raw_dataset(config: dict, dataset_key: str, engine, batch_size: int = 500000) -> tuple[pd.DataFrame, str]:
    """Ingests raw structured datasets using robust direct paths and threshold-gated format recommendations."""
    datasets = config.get("datasets", {})
    discovered = get_discovered_datasets(config)
    
    merged_datasets = {**discovered, **datasets}
    if not merged_datasets:
        raise FileNotFoundError("No structured files found in 'Data/raw/Structured_dataset/'.")

    if dataset_key not in merged_datasets:
        dataset_key = list(merged_datasets.keys())[0]

    dataset_cfg = merged_datasets[dataset_key]
    file_name = dataset_cfg.get("file_name") or f"{dataset_key}.csv"
    file_path = dataset_cfg.get("file_path")
    file_format = str(dataset_cfg.get("format", "CSV")).upper()
    sheet_name = dataset_cfg.get("sheet_name", 0)

    dirs = config.get("directories", {})
    raw_root = dirs.get("raw") or dirs.get("raw_data", "Data/raw")
    struct_dir = os.path.join(raw_root, "Structured_dataset")

    if not file_path or not os.path.exists(file_path):
        file_path = os.path.join(struct_dir, file_name)

    # Robust case-insensitive fallback search in Structured_dataset directory
    if not os.path.exists(file_path) and os.path.exists(struct_dir):
        files_in_dir = os.listdir(struct_dir)
        matching = [f for f in files_in_dir if f.lower() == file_name.lower() or sanitize_table_name(f) == dataset_key]
        if matching:
            file_name = matching[0]
            file_path = os.path.join(struct_dir, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Source file not found at path: {file_path}. Directory contents: {os.listdir(struct_dir) if os.path.exists(struct_dir) else 'Directory missing'}")

    table_name = sanitize_table_name(file_name)
    df = pd.DataFrame()
    total_records = 0
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

    try:
        if file_format == "PARQUET":
            print(f"📦 [BRONZE STREAM] Opening Parquet file with batched streaming: {file_path} ({file_size_mb:.2f} MB)")
            parquet_file = pq.ParquetFile(file_path)
            total_records = parquet_file.metadata.num_rows
            
            first_batch = True
            for batch in parquet_file.iter_batches(batch_size=batch_size):
                df_chunk = batch.to_pandas()
                if first_batch:
                    df = df_chunk.copy()
                    persist_to_postgres(df_chunk, table_name, "bronze", engine, if_exists="replace")
                    first_batch = False
                else:
                    persist_to_postgres(df_chunk, table_name, "bronze", engine, if_exists="append")
            print(f"✔ Successfully streamed & persisted Parquet source: {file_path} ({total_records} records)")

        elif file_format in ["EXCEL", "XLSX", "XLS"]:
            # Only trigger format recommendation and prompt if Excel file exceeds size limit (> 25 MB)
            if file_size_mb > 25:
                print("\n💡 [PIPELINE FORMAT RECOMMENDATION] -----------------------------------------")
                print(f"   Target file '{file_name}' is in Excel format (~{file_size_mb:.2f} MB).")
                print(f"   RECOMMENDATION: Convert this workbook into a **CSV** or **Parquet** file")
                print(f"   to unlock chunked streaming and eliminate XML parsing overhead.")
                print("--------------------------------------------------------------------------------\n")
                
                proceed = input("Proceed with local Excel parsing anyway? (y/n): ").strip().lower()
                if proceed != 'y':
                    print("🛑 Aborted. Please save your sheet as a CSV or Parquet and retry.")
                    sys.exit(0)

            df = pd.read_excel(file_path, sheet_name=sheet_name)
            total_records = len(df)
            persist_to_postgres(df, table_name, "bronze", engine, if_exists="replace")
            print(f"✔ Successfully loaded Excel Bronze source: {file_path} ({total_records} records)")

        elif file_format == "JSON":
            if file_size_mb > 100:
                print("\n💡 [PIPELINE FORMAT RECOMMENDATION] -----------------------------------------")
                print(f"   JSON file size is ~{file_size_mb:.2f} MB. For optimal performance with large")
                print(f"   datasets, consider converting JSON payloads into **Parquet** format.")
                print("--------------------------------------------------------------------------------\n")

            print(f"📦 [BRONZE STREAM] Opening JSON file: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as jf:
                raw_json_data = json.load(jf)
            
            if isinstance(raw_json_data, dict):
                df = pd.DataFrame.from_dict(raw_json_data, orient='index')
            else:
                df = pd.json_normalize(raw_json_data)
                
            total_records = len(df)
            persist_to_postgres(df, table_name, "bronze", engine, if_exists="replace")
            print(f"✔ Successfully loaded & persisted JSON source: {file_path} ({total_records} records)")

        elif file_format == "CSV":
            if file_size_mb > 500:
                print("\n💡 [PIPELINE FORMAT RECOMMENDATION] -----------------------------------------")
                print(f"   CSV file size is ~{file_size_mb:.2f} MB. Converting this large flat file")
                print(f"   directly into compressed **Parquet** will drastically reduce storage size")
                print(f"   and accelerate downstream query speeds.")
                print("--------------------------------------------------------------------------------\n")

            print(f"📦 [BRONZE STREAM] Opening CSV file with chunked iteration: {file_path}")
            first_batch = True
            for chunk_df in pd.read_csv(file_path, chunksize=batch_size, on_bad_lines='skip'):
                total_records += len(chunk_df)
                if first_batch:
                    df = chunk_df.copy()
                    persist_to_postgres(chunk_df, table_name, "bronze", engine, if_exists="replace")
                    first_batch = False
                else:
                    persist_to_postgres(chunk_df, table_name, "bronze", engine, if_exists="append")
            print(f"✔ Successfully streamed & persisted CSV source: {file_path} (~{total_records} records)")

        else:
            raise ValueError(f"Unsupported format '{file_format}' defined for dataset '{dataset_key}'")

    except Exception as e:
        logging.error(f"Schema ingestion failure for {file_path}: {e}")
        print(f"⚠️ Warning: Failed to parse {file_path} cleanly. Error: {e}")
        df = pd.DataFrame()

    return df, table_name


def select_dataset_key(config: dict, target_key: str = None) -> str:
    """Dynamically select active dataset key, prompting the user if multiple files are found."""
    datasets = config.get("datasets", {})
    discovered = get_discovered_datasets(config)
    merged = {**discovered, **datasets}

    available = list(merged.keys())
    if not available:
        return None
    
    if target_key and target_key in available:
        return target_key
    
    if len(available) == 1:
        return available[0]
    
    print("\n📂 Multiple structured datasets discovered:")
    for idx, key in enumerate(available, 1):
        file_info = merged[key]
        print(f"   [{idx}] {file_info.get('file_name')} (Format: {file_info.get('format')})")
    
    while True:
        choice = input(f"\nSelect dataset [1-{len(available)}]: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(available):
            selected_key = available[int(choice) - 1]
            print(f"✔ Selected dataset: {selected_key}")
            return selected_key
        print("Invalid selection. Please try again.")


def reset_layers(config: dict, layer: str, table_name: str = None, engine=None):
    """Granular reset for specific medallion layers (bronze, silver, gold, or all)."""
    target_layers = ["bronze", "silver", "gold"] if layer in ["all", "full"] else [layer.lower()]
    dirs = config.get("directories", {})

    print(f"\n================================================================================")
    print(f"                      DATA ENGINE - RESET: {layer.upper()}                      ")
    print(f"================================================================================")

    for l in target_layers:
        if engine:
            try:
                with engine.begin() as conn:
                    if table_name and l != "all":
                        conn.execute(text(f'DROP TABLE IF EXISTS "{l}"."{table_name}" CASCADE;'))
                        print(f"✔ PostgreSQL: Dropped table -> {l}.{table_name}")
                    else:
                        conn.execute(text(f'DROP SCHEMA IF EXISTS "{l}" CASCADE;'))
                        conn.execute(text(f'CREATE SCHEMA "{l}";'))
                        print(f"✔ PostgreSQL: Reset schema -> {l}")
            except Exception as e:
                print(f"[ERROR] Could not reset PostgreSQL layer '{l}': {e}")

        if l == "silver":
            silver_dir = dirs.get("silver_staging", "Data/silver_staging")
            if os.path.exists(silver_dir):
                if table_name:
                    file_path = os.path.join(silver_dir, f"{table_name}_silver.csv")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        print(f"✔ Local File Removed: {file_path}")
                else:
                    for item in os.listdir(silver_dir):
                        item_path = os.path.join(silver_dir, item)
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                    print(f"✔ Cleared local directory: {silver_dir}")

        elif l == "gold":
            gold_dir = dirs.get("gold_exports", "Data/gold_exports")
            if os.path.exists(gold_dir):
                if table_name:
                    file_path = os.path.join(gold_dir, f"{table_name}_gold.csv")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        print(f"✔ Local File Removed: {file_path}")
                else:
                    for item in os.listdir(gold_dir):
                        item_path = os.path.join(gold_dir, item)
                        if os.path.isfile(item_path):
                            os.remove(item_path)
                    print(f"✔ Cleared local directory: {gold_dir}")

    print("================================================================================\n")


def run_gold_promotion(config: dict, table_name: str, engine, interactive: bool = False):
    """Fetches current live state from silver.{table_name} and promotes directly to Gold."""
    print(f"\n--------------------------------------------------------------------------------")
    print(f" [GOLD LAYER] Promoting Live 'silver.{table_name}' -> 'gold.{table_name}'")
    print(f"--------------------------------------------------------------------------------")

    if engine:
        try:
            gold_df = pd.read_sql(f'SELECT * FROM "silver"."{table_name}"', engine)
            print(f"✔ Successfully fetched {len(gold_df)} records directly from PostgreSQL 'silver.{table_name}'.")
        except Exception as e:
            print(f"[WARN] Could not query 'silver.{table_name}' ({e}). Falling back to local Silver CSV.")
            silver_path = os.path.join(config.get("directories", {}).get("silver_staging", "Data/silver_staging"), f"{table_name}_silver.csv")
            gold_df = pd.read_csv(silver_path) if os.path.exists(silver_path) else pd.DataFrame()
    else:
        silver_path = os.path.join(config.get("directories", {}).get("silver_staging", "Data/silver_staging"), f"{table_name}_silver.csv")
        gold_df = pd.read_csv(silver_path) if os.path.exists(silver_path) else pd.DataFrame()

    if interactive:
        while True:
            action = input("\n[GOLD GATE] Press [Enter] to continue, type 'ai' to ask Gemma, or type 'exit' to stop: ").strip().lower()
            if action == 'ai':
                user_query = input("🤖 Enter your question for Gemma (Gold): ").strip()
                if user_query:
                    sample_cols = list(gold_df.columns)
                    context_desc = f"Curated table 'gold.{table_name}' has columns {sample_cols} with {len(gold_df)} total rows."
                    ask_gemma_with_sql(context_desc, user_query, engine, table_name, schema="gold")
            elif action == 'exit':
                print("Exiting pipeline at Gold Gate.")
                sys.exit(0)
            else:
                break

    gold_dir = config.get("directories", {}).get("gold_exports", "Data/gold_exports")
    os.makedirs(gold_dir, exist_ok=True)
    gold_path = os.path.join(gold_dir, f"{table_name}_gold.csv")
    gold_df.to_csv(gold_path, index=False)

    persist_to_postgres(gold_df, table_name, "gold", engine, if_exists="replace")

    print(f"✔ Local Gold export CSV saved: {gold_path}")
    print(f"\n--- Previewing Gold Curated Data ('gold.{table_name}') ---")
    print(gold_df.head(5))

    print("\n================================================================================")
    print(f" [SUCCESS] Pipeline completed. Gold schema curated & exported successfully.")
    print(f"   • Database: staging_db.gold.{table_name}")
    print(f"   • Export:   {gold_path}")
    print("================================================================================\n")


def run_structured_pipeline(config: dict, dataset_key: str, interactive: bool, engine):
    """Executes the core structured Medallion workflow with AI gates at each layer."""
    active_dataset = select_dataset_key(config, dataset_key)
    if not active_dataset:
        print("[WARN] No structured raw files found in 'Data/raw/Structured_dataset/'. Skipping structured run.")
        return

    raw_df, table_name = load_raw_dataset(config, active_dataset, engine)

    print(f"\n--------------------------------------------------------------------------------")
    print(f" [BRONZE LAYER] Ingesting Raw File -> PostgreSQL Table: 'bronze.{table_name}'")
    print(f"--------------------------------------------------------------------------------")
    print(f"✔ Read {len(raw_df)} records across {len(raw_df.columns)} columns.")

    if interactive:
        while True:
            action = input("\n[BRONZE GATE] Press [Enter] to continue, type 'ai' to ask Gemma, or type 'exit' to stop: ").strip().lower()
            if action == 'ai':
                user_query = input("🤖 Enter your question for Gemma (Bronze): ").strip()
                if user_query:
                    columns_list = list(raw_df.columns)
                    context_desc = f"Table 'bronze.{table_name}' has columns {columns_list} with {len(raw_df)} total records."
                    ask_gemma_with_sql(context_desc, user_query, engine, table_name, schema="bronze")
            elif action == 'exit':
                print("Exiting pipeline at Bronze Gate.")
                sys.exit(0)
            else:
                break

    print(f"\n--------------------------------------------------------------------------------")
    print(f" [SILVER LAYER] Staging Cleaned Data -> PostgreSQL Table: 'silver.{table_name}'")
    print(f"--------------------------------------------------------------------------------")

    silver_df = raw_df.copy()
    silver_dir = config.get("directories", {}).get("silver_staging", "Data/silver_staging")
    os.makedirs(silver_dir, exist_ok=True)
    silver_path = os.path.join(silver_dir, f"{table_name}_silver.csv")
    silver_df.to_csv(silver_path, index=False)

    persist_to_postgres(silver_df, table_name, "silver", engine, if_exists="replace")
    print(f"✔ Base Silver table populated in PostgreSQL.")

    if interactive:
        while True:
            print(f"\n================================================================================")
            print(f" 🛠️  SILVER TRANSFORMATION GATE: 'silver.{table_name}'")
            print(f"================================================================================")
            print(f" [1] Continue to Gold promotion")
            print(f" [2] Ask Gemma AI to write a custom SQL transformation")
            print(f" [3] Ask Gemma a custom question (with live DB queries)")
            print(f" [4] Exit pipeline")
            print(f"================================================================================")
            choice = input("Select option [1-4], press [Enter] to continue, or type 'exit': ").strip().lower()

            if choice in ["4", "exit"]:
                print("Exiting pipeline at Silver Gate.")
                sys.exit(0)
            elif choice == "2":
                columns_list = list(silver_df.columns)
                user_transformation = input("🛠️ Enter transformation instruction for Gemma: ").strip()
                if user_transformation:
                    prompt = f"""
You are an expert PostgreSQL data engineer.
The table 'silver.{table_name}' has columns: {columns_list}.
Write a clean PostgreSQL DML or DDL statement (such as DELETE, UPDATE, or ALTER TABLE) to execute the following transformation requested by the engineer: '{user_transformation}'.
Return ONLY valid SQL code inside a markdown block. No conversational text.
"""
                    ai_response = call_gemma_assistant(prompt)
                    print(f"\n--- Gemma Generated SQL ---\n{ai_response}\n--------------------------")
                    
                    run_sql = input("Execute this AI-generated SQL against 'silver.{table_name}'? (y/n): ").strip().lower()
                    if run_sql == 'y':
                        if "```sql" in ai_response:
                            sql_code = ai_response.split("```sql")[1].split("```")[0].strip()
                        elif "```" in ai_response:
                            sql_code = ai_response.split("```")[1].split("```")[0].strip()
                        else:
                            sql_code = ai_response

                        execute_ai_transformation(None, sql_code, engine=engine)
            elif choice == "3" or choice == "ai":
                user_query = input("🤖 Enter your custom question for Gemma (Silver): ").strip()
                if user_query:
                    columns_list = list(silver_df.columns)
                    context_desc = f"Table 'silver.{table_name}' has columns {columns_list} with {len(silver_df)} total records."
                    ask_gemma_with_sql(context_desc, user_query, engine, table_name, schema="silver")
            else:
                break

    run_gold_promotion(config, table_name, engine, interactive=interactive)


def execute_pipeline(interactive: bool = False, dataset_key: str = None, reset: bool = False, reset_layer_name: str = None, promote_gold_only: bool = False):
    """Unified Master Orchestrator supporting Structured, Unstructured, or Dual workflows with diagnostics."""
    
    if reset or reset_layer_name:
        try:
            config = load_config("config/pipeline.yaml")
            engine = get_db_engine(config)
        except Exception:
            engine = None
            config = {"directories": {"raw": "Data/raw", "silver_staging": "Data/silver_staging", "gold_exports": "Data/gold_exports"}}

        if reset:
            reset_layers(config, "all", table_name=None, engine=engine)
        elif reset_layer_name:
            target_ds = select_dataset_key(config, dataset_key) if dataset_key and not reset_layer_name else None
            reset_layers(config, reset_layer_name, table_name=target_ds, engine=engine)
        return

    check_ollama_service()

    try:
        config = load_config("config/pipeline.yaml")
        
        raw_root = config.get("directories", {}).get("raw", "Data/raw")
        os.makedirs(os.path.join(raw_root, "Structured_dataset"), exist_ok=True)
        os.makedirs(os.path.join(raw_root, "Unstructured_dataset"), exist_ok=True)

        try:
            engine = get_db_engine(config)
        except Exception as e:
            print(f"\n[DATABASE ERROR] Could not connect to PostgreSQL: {e}", flush=True)
            engine = None

        if promote_gold_only:
            active_dataset = select_dataset_key(config, dataset_key)
            table_name = active_dataset if active_dataset else "default_table"
            run_gold_promotion(config, table_name, engine, interactive=interactive)
            return

        pipeline_mode = "1"
        if interactive:
            print("\n================================================================================")
            print("                      DATA ENGINE - UNIFIED MEDALLION ORCHESTRATOR")
            print("================================================================================")
            print(" Select Pipeline Mode:")
            print("   [1] Structured Data Only (Tabular CSV/JSON/Parquet -> Bronze/Silver/Gold)")
            print("   [2] Unstructured Data Only (PDFs/Text/Docs/Images/Audio -> Multimodal Chunks)")
            print("   [3] Both (Run Structured Pipeline & Unstructured Ingestion Concurrently)")
            print("   [4] Reset Layer / Full Wipe")
            print("   [5] Exit")
            print("================================================================================")

            choice = input("\nSelect an option [1-5]: ").strip()

            if choice == "5":
                print("Exiting Data Engine CLI.")
                return
            elif choice == "4":
                print("\nSelect reset scope:")
                print(" [1] Bronze Layer")
                print(" [2] Silver Layer")
                print(" [3] Gold Layer")
                print(" [4] All Layers (Full Reset)")
                layer_map = {"1": "bronze", "2": "silver", "3": "gold", "4": "all"}
                selected_layer = layer_map.get(input("\nSelect scope [1-4]: ").strip())
                if selected_layer:
                    reset_layers(config, selected_layer, table_name=None, engine=engine)
                return
            elif choice in ["1", "2", "3"]:
                pipeline_mode = choice
            else:
                print("Invalid selection. Defaulting to Structured Data Only [1].")
                pipeline_mode = "1"

        if pipeline_mode in ["1", "3"]:
            run_structured_pipeline(config, dataset_key, interactive, engine)

        if pipeline_mode in ["2", "3"]:
            if process_unstructured_medallion:
                print("\n================================================================================")
                print(" [WORKFLOW] Executing Unstructured Multimodal Ingestion Engine...")
                print("================================================================================")
                process_unstructured_medallion(config, engine, interactive=interactive)
            else:
                print("\n[WARN] 'unstructured_engine.py' not found or failed to import. Skipping unstructured pipeline.")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] Exception caught in execute_pipeline: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified Data Factory Medallion Engine")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive CLI mode")
    parser.add_argument("--dataset", type=str, default=None, help="Target dataset key from config/pipeline.yaml")
    parser.add_argument("--reset", action="store_true", help="Full reset of all database schemas and local exports")
    parser.add_argument("--reset-layer", type=str, choices=["bronze", "silver", "gold", "all"], help="Granular reset scope")
    parser.add_argument("--promote-gold", action="store_true", help="Promote current silver table directly to gold")
    args = parser.parse_args()

    execute_pipeline(
        interactive=args.interactive,
        dataset_key=args.dataset,
        reset=args.reset,
        reset_layer_name=args.reset_layer,
        promote_gold_only=args.promote_gold
    )