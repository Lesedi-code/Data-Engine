import os
import hashlib
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
import yaml
import sys

def load_config(config_path: str = "config.yaml") -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_db_engine(config: dict):
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
    with engine.begin() as conn:
        for schema in ["bronze", "silver", "gold"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
    return engine

def compute_file_hash(filepath: str) -> str:
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()

def call_gemma_assistant_unstructured(prompt: str, model: str = "gemma4:e4b") -> str:
    """Query local Ollama instance running Gemma for unstructured data insights."""
    print(f"\n🤖 [GEMMA AI - UNSTRUCTURED] Querying local model ({model})...", flush=True)
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

def ask_gemma_unstructured_with_sql(prompt_context: str, user_query: str, engine, schema: str = "silver", table_name: str = "unstructured_chunks"):
    """Allows Gemma to write SQL, execute it against unstructured PostgreSQL tables, and answer the user."""
    system_prompt = f"""
You are an expert PostgreSQL data engineer assisting Lesedi Motleleng.
You have access to a PostgreSQL table named '{schema}.{table_name}'.
The user will ask a question about the unstructured dataset or text chunks. If the question requires inspecting the data, write a valid SQL query to find the answer.
Return your response in this exact format:
SQL: <your sql query here>
EXPLANATION: <brief explanation of what the query does>

If no SQL is needed, leave the SQL: line blank or write SELECT 1; and provide your answer in the explanation.
"""
    full_prompt = f"{system_prompt}\nTable Context: {prompt_context}\nUser Question: {user_query}\nResponse:"
    
    ai_response = call_gemma_assistant_unstructured(full_prompt)
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
                final_answer = call_gemma_assistant_unstructured(followup_prompt)
                print(f"\n🤖 [GEMMA FINAL ANSWER]\n{final_answer}")
        except Exception as e:
            print(f"[ERROR] Could not execute AI-generated SQL against PostgreSQL: {e}")

def ingest_realtime_apis(config: dict) -> list[dict]:
    api_configs = config.get("api_sources", [])
    api_records = []
    
    for api in api_configs:
        name = api.get("name", "external_api")
        url = api.get("url")
        if not url:
            continue
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                payload = response.text
                api_records.append({
                    "file_hash": hashlib.md5(payload.encode()).hexdigest(),
                    "file_name": f"api_stream_{name}.json",
                    "media_type": "API_JSON",
                    "file_path": url,
                    "file_size_bytes": len(payload.encode())
                })
                os.makedirs("Data/raw/unstructured/api_dumps", exist_ok=True)
                with open(f"Data/raw/unstructured/api_dumps/{name}.json", "w", encoding="utf-8") as f:
                    f.write(payload)
        except Exception as e:
            print(f"[WARN] Failed to fetch API source '{name}': {e}")
    return api_records

def process_unstructured_medallion(config: dict, engine, interactive: bool = True):
    raw_dir = "Data/raw/Unstructured_dataset"
    if not os.path.exists(raw_dir):
        raw_dir = "Data/raw/unstructured"
    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir, exist_ok=True)

    print("\n--------------------------------------------------------------------------------")
    print(" [UNSTRUCTURED BRONZE] Ingesting Multimodal File Registry & API Streams")
    print("--------------------------------------------------------------------------------")

    bronze_records = []
    
    for root, _, files in os.walk(raw_dir):
        for file in files:
            if "api_dumps" in root:
                continue
            filepath = os.path.join(root, file)
            file_hash = compute_file_hash(filepath)
            ext = os.path.splitext(file)[1].lower()
            file_size = os.path.getsize(filepath)

            bronze_records.append({
                "file_hash": file_hash,
                "file_name": file,
                "media_type": ext.replace(".", "").upper(),
                "file_path": filepath,
                "file_size_bytes": file_size
            })

    api_records = ingest_realtime_apis(config)
    bronze_records.extend(api_records)

    if not bronze_records:
        print("ℹ No unstructured assets or API sources found.")
        return

    bronze_df = pd.DataFrame(bronze_records)
    with engine.begin() as conn:
        bronze_df.to_sql(
            name="unstructured_raw",
            con=conn,
            schema="bronze",
            if_exists="replace",
            index=False
        )
    print(f"✔ Bronze Ingested: {len(bronze_records)} raw asset(s) -> 'bronze.unstructured_raw'.")

    if interactive:
        while True:
            action = input("\n[UNSTRUCTURED BRONZE GATE] Press [Enter] to continue, type 'ai' to ask Gemma, or type 'exit' to stop: ").strip().lower()
            if action == 'ai':
                user_query = input("🤖 Enter your question for Gemma (Unstructured Bronze): ").strip()
                if user_query:
                    file_summary = [r['file_name'] for r in bronze_records]
                    context_desc = f"Table 'bronze.unstructured_raw' tracks raw unstructured files: {file_summary}"
                    ask_gemma_unstructured_with_sql(context_desc, user_query, engine, schema="bronze", table_name="unstructured_raw")
            elif action == 'exit':
                print("Exiting pipeline at Unstructured Bronze Gate.")
                sys.exit(0)
            else:
                break

    # =========================================================================
    # SILVER LAYER: Advanced Parsing (OCR, Whisper, PDF, and API Payloads)
    # =========================================================================
    print("\n--------------------------------------------------------------------------------")
    print(" [UNSTRUCTURED SILVER] Executing Local OCR, Transcription & Chunking")
    print("--------------------------------------------------------------------------------")

    silver_records = []
    
    for record in bronze_records:
        filepath = record["file_path"]
        ext = record["media_type"]
        file_name = record["file_name"]

        if ext in ["TXT", "MD", "LOG", "JSON", "API_JSON"]:
            target_path = filepath if os.path.exists(filepath) else f"Data/raw/unstructured/api_dumps/{file_name.replace('api_stream_', '')}"
            if os.path.exists(target_path):
                with open(target_path, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    chunks = [content[i:i+1000] for i in range(0, len(content), 1000)]
                    for idx, chunk in enumerate(chunks):
                        silver_records.append({
                            "doc_hash": record["file_hash"],
                            "file_name": file_name,
                            "chunk_index": idx + 1,
                            "page_number": 1,
                            "chunk_text": chunk,
                            "citation_source": f"Source: {file_name} | Chunk {idx+1}"
                        })

        elif ext == "PDF":
            try:
                from pypdf import PdfReader
                reader = PdfReader(filepath)
                for page_num, page in enumerate(reader.pages):
                    page_text = page.extract_text() or ""
                    if page_text.strip():
                        silver_records.append({
                            "doc_hash": record["file_hash"],
                            "file_name": file_name,
                            "chunk_index": page_num + 1,
                            "page_number": page_num + 1,
                            "chunk_text": page_text,
                            "citation_source": f"PDF: {file_name} | Page {page_num+1}"
                        })
            except Exception as e:
                print(f"[WARN] Error reading PDF '{file_name}': {e}")

        elif ext in ["PNG", "JPG", "JPEG"]:
            try:
                import pytesseract
                from PIL import Image
                ocr_text = pytesseract.image_to_string(Image.open(filepath))
                silver_records.append({
                    "doc_hash": record["file_hash"],
                    "file_name": file_name,
                    "chunk_index": 1,
                    "page_number": 1,
                    "chunk_text": ocr_text if ocr_text.strip() else "[OCR detected no readable text]",
                    "citation_source": f"Image OCR: {file_name}"
                })
            except Exception as e:
                silver_records.append({
                    "doc_hash": record["file_hash"],
                    "file_name": file_name,
                    "chunk_index": 1,
                    "page_number": 1,
                    "chunk_text": f"[Image registered. OCR processing skipped: {e}]",
                    "citation_source": f"Image: {file_name}"
                })

        elif ext in ["MP3", "WAV", "M4A"]:
            try:
                import whisper
                model = whisper.load_model("base")
                result = model.transcribe(filepath)
                transcript = result.get("text", "")
                silver_records.append({
                    "doc_hash": record["file_hash"],
                    "file_name": file_name,
                    "chunk_index": 1,
                    "page_number": 1,
                    "chunk_text": transcript,
                    "citation_source": f"Audio Transcript: {file_name}"
                })
            except Exception as e:
                silver_records.append({
                    "doc_hash": record["file_hash"],
                    "file_name": file_name,
                    "chunk_index": 1,
                    "page_number": 1,
                    "chunk_text": f"[Audio file logged. Local Whisper transcription unavailable: {e}]",
                    "citation_source": f"Audio: {file_name}"
                })

    silver_df = pd.DataFrame(silver_records)
    with engine.begin() as conn:
        silver_df.to_sql(
            name="unstructured_chunks",
            con=conn,
            schema="silver",
            if_exists="replace",
            index=False
        )
    print(f"✔ Silver Structured: Generated {len(silver_records)} parsed chunk(s) -> 'silver.unstructured_chunks'.")

    # Unstructured Silver Gate Loop with Dynamic DML Transformation Option [2]
    if interactive:
        while True:
            print(f"\n================================================================================")
            print(f" 🛠️  UNSTRUCTURED SILVER TRANSFORMATION GATE: 'silver.unstructured_chunks'")
            print(f"================================================================================")
            print(f" [1] Continue to Gold promotion")
            print(f" [2] Ask Gemma AI to write a custom SQL transformation")
            print(f" [3] Ask Gemma a custom question (with live DB queries)")
            print(f" [4] Exit pipeline")
            print(f"================================================================================")
            choice = input("Select option [1-4], press [Enter] to continue, or type 'exit': ").strip().lower()

            if choice in ["4", "exit"]:
                print("Exiting pipeline at Unstructured Silver Gate.")
                sys.exit(0)
            elif choice == "2":
                columns_list = list(silver_df.columns)
                user_transformation = input("🛠️ Enter transformation instruction for Gemma (e.g., 'Delete chunks where page_number > 5', 'Update text'): ").strip()
                if user_transformation:
                    prompt = f"""
You are an expert PostgreSQL data engineer.
The table 'silver.unstructured_chunks' has columns: {columns_list}.
Write a clean PostgreSQL DML statement (such as DELETE, UPDATE, or ALTER TABLE) to execute the following transformation requested by the engineer: '{user_transformation}'.
IMPORTANT: If the user wants to filter, keep, or remove rows, write a DELETE statement (e.g., DELETE FROM silver.unstructured_chunks WHERE ...) rather than a SELECT statement, because this statement must mutate the table directly in PostgreSQL.
Return ONLY valid SQL code inside a markdown block. No conversational text.
"""
                    ai_response = call_gemma_assistant_unstructured(prompt)
                    print(f"\n--- Gemma Generated SQL ---\n{ai_response}\n--------------------------")
                    
                    run_sql = input("Execute this AI-generated SQL against 'silver.unstructured_chunks'? (y/n): ").strip().lower()
                    if run_sql == 'y':
                        try:
                            if "```sql" in ai_response:
                                sql_code = ai_response.split("```sql")[1].split("```")[0].strip()
                            elif "```" in ai_response:
                                sql_code = ai_response.split("```")[1].split("```")[0].strip()
                            else:
                                sql_code = ai_response

                            with engine.begin() as conn:
                                for stmt in sql_code.split(";"):
                                    if stmt.strip():
                                        conn.execute(text(stmt))
                            print("✔ AI SQL executed successfully against PostgreSQL!")
                        except Exception as e:
                            print(f"[ERROR] Failed to execute SQL: {e}")
            elif choice == "3" or choice == "ai":
                user_query = input("🤖 Enter your custom question for Gemma (Unstructured Silver): ").strip()
                if user_query:
                    context_desc = f"Table 'silver.unstructured_chunks' has {len(silver_records)} text chunks parsed from files."
                    ask_gemma_unstructured_with_sql(context_desc, user_query, engine, schema="silver", table_name="unstructured_chunks")
            else:
                break

    # =========================================================================
    # GOLD LAYER: Curated Agentic Search Index
    # =========================================================================
    print("\n--------------------------------------------------------------------------------")
    print(" [UNSTRUCTURED GOLD] Exporting Curated Agentic Search Index")
    print("--------------------------------------------------------------------------------")

    if engine:
        try:
            gold_df = pd.read_sql('SELECT * FROM "silver"."unstructured_chunks"', engine)
            print(f"✔ Successfully fetched {len(gold_df)} records directly from PostgreSQL 'silver.unstructured_chunks'.")
        except Exception as e:
            print(f"[WARN] Could not query silver table ({e}), using local dataframe.")
            gold_df = silver_df[["file_name", "chunk_index", "page_number", "chunk_text", "citation_source"]].copy()
    else:
        gold_df = silver_df[["file_name", "chunk_index", "page_number", "chunk_text", "citation_source"]].copy()

    with engine.begin() as conn:
        gold_df.to_sql(
            name="unstructured_knowledge_gold",
            con=conn,
            schema="gold",
            if_exists="replace",
            index=False
        )
        
    os.makedirs("Data/gold_exports", exist_ok=True)
    gold_df.to_csv("Data/gold_exports/unstructured_knowledge_gold.csv", index=False)
    print("✔ Gold Curated: Knowledge index persisted -> 'gold.unstructured_knowledge_gold'.\n")

    if interactive:
        while True:
            action = input("\n[UNSTRUCTURED GOLD GATE] Press [Enter] to complete, type 'ai' to ask Gemma, or type 'exit' to stop: ").strip().lower()
            if action == 'ai':
                user_query = input("🤖 Enter your question for Gemma (Unstructured Gold Index): ").strip()
                if user_query:
                    context_desc = f"Table 'gold.unstructured_knowledge_gold' has {len(gold_df)} curated knowledge index rows."
                    ask_gemma_unstructured_with_sql(context_desc, user_query, engine, schema="gold", table_name="unstructured_knowledge_gold")
            elif action == 'exit':
                print("Exiting pipeline at Unstructured Gold Gate.")
                sys.exit(0)
            else:
                break

if __name__ == "__main__":
    config = load_config("config.yaml")
    engine = get_db_engine(config)
    process_unstructured_medallion(config, engine)