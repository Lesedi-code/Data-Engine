import os
import duckdb
import ollama
import re

class AgenticRAG:
    def __init__(self, db_path="data_engine.duckdb", model=None):
        self.db_path = db_path
        self.model = model or self._detect_local_model()

    def _detect_local_model(self):
        """Detect available models from Ollama and let the user choose interactively."""
        try:
            response = ollama.list()
            models = response.get('models', []) if isinstance(response, dict) else getattr(response, 'models', [])
            
            if not models:
                print("⚠️ No local models found in Ollama. Falling back to 'deepseek-r1:14b'.")
                return "deepseek-r1:14b"
            
            model_names = [m.get('model') if isinstance(m, dict) else getattr(m, 'model', str(m)) for m in models]
            
            if len(model_names) == 1:
                print(f"⚙️ Auto-selected single available model: {model_names[0]}")
                return model_names[0]
            
            print("\n⚙️ Available local Ollama models detected:")
            for idx, name in enumerate(model_names, 1):
                print(f"  [{idx}] {name}")
            print("  (Type 'exit' or press Ctrl+C to quit the session)")
            
            choice = input("\nSelect model number (or press Enter for default): ").strip()
            if choice.isdigit() and 1 <= int(choice) <= len(model_names):
                selected = model_names[int(choice) - 1]
                print(f"⚙️ Selected model: {selected}\n")
                return selected
            
            for name in model_names:
                if 'deepseek' in name.lower():
                    print(f"⚙️ Defaulting to DeepSeek model: {name}\n")
                    return name
            
            print(f"⚙️ Defaulting to: {model_names[0]}\n")
            return model_names[0]
            
        except Exception as e:
            print(f"⚠️ Could not detect local models ({e}). Falling back to 'deepseek-r1:14b'.")
            return "deepseek-r1:14b"

    def _get_workspace_schema(self):
        """Automatically inspect CSV files in the workspace to extract table schemas for the prompt."""
        schema_desc = ""
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            tables = conn.execute("SHOW TABLES;").fetchdf()
            if not tables.empty:
                schema_desc += f"DuckDB Tables:\n{tables.to_string(index=False)}\n\n"
        except Exception:
            pass

        schema_desc += "Workspace CSV Files & Columns:\n"
        for root, dirs, files in os.walk('.'):
            dirs[:] = [d for d in dirs if d not in {'venv', '.git', '__pycache__'}]
            for file in files:
                if file.endswith('.csv'):
                    csv_path = os.path.join(root, file)
                    try:
                        cols = conn.execute(f"DESCRIBE SELECT * FROM '{csv_path}';").fetchdf()['column_name'].tolist()
                        schema_desc += f"- {file} (path: {csv_path}): columns = {cols}\n"
                    except Exception:
                        pass
        conn.close()
        return schema_desc

    def query_duckdb(self, sql_query: str):
        """Tool: Query structured tables or direct CSV files via DuckDB with auto path resolution and gold priority."""
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            clean_sql = re.sub(r'```[a-zA-Z]*|```', '', sql_query).strip()
            
            for match_file in re.findall(r"['\"]([a-zA-Z0-9_\-\.]+\.csv)['\"]", clean_sql):
                found_path = match_file
                if not os.path.exists(found_path):
                    candidate_paths = []
                    for root, dirs, files in os.walk('.'):
                        dirs[:] = [d for d in dirs if d not in {'venv', '.git', '__pycache__'}]
                        if match_file in files:
                            candidate_paths.append(os.path.join(root, match_file))
                    
                    if candidate_paths:
                        gold_matches = [p for p in candidate_paths if 'gold' in p.lower()]
                        found_path = gold_matches[0] if gold_matches else candidate_paths[0]

                clean_sql = clean_sql.replace(f"'{match_file}'", f"'{found_path}'").replace(f'"{match_file}"', f'"{found_path}"')

            result = conn.execute(clean_sql).fetchdf()
            return result.to_markdown(index=False)
        except Exception as e:
            return f"Error executing query: {e}"
        finally:
            conn.close()

    def run_agent(self, user_prompt: str):
        """Chatbot execution loop with schema awareness and think tag cleaning."""
        workspace_schema = self._get_workspace_schema()

        # FIXED SYSTEM PROMPT: Allows both SQL queries and general Python/ML strategy discussions
        system_prompt = (
            "You are an enterprise data assistant and machine learning engineer with access to a local DuckDB data warehouse "
            "and local project CSV files.\n\n"
            f"{workspace_schema}\n"
            "INSTRUCTIONS:\n"
            "1. If the user asks to query, inspect, or aggregate data from tables/CSV files, write a standard DuckDB query inside a ```sql ``` block.\n"
            "2. If the user asks about machine learning models, python code, strategy, or general architecture, provide clear explanations or Python code blocks (using ```python ```). Do NOT force everything into SQL.\n"
            "3. ALWAYS use exact column names when referencing workspace files."
        )
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        response = ollama.chat(model=self.model, messages=messages)
        assistant_message = response['message']['content']
        
        # Strip out DeepSeek reasoning tags
        assistant_message = re.sub(r'<think>.*?</think>', '', assistant_message, flags=re.DOTALL).strip()

        # FIXED EXECUTION GUARD: ONLY look for explicitly tagged ```sql ... ``` blocks to run against DuckDB
        sql_match = re.search(r'```sql\s+(.*?)```', assistant_message, re.DOTALL | re.IGNORECASE)
        sql_query = sql_match.group(1).strip() if sql_match else None

        if sql_query and len(sql_query) > 5:
            print(f"\n⚙️ [Running Query]: {sql_query}")
            query_result = self.query_duckdb(sql_query)
            
            messages.append({"role": "assistant", "content": assistant_message})
            messages.append({"role": "user", "content": f"Here are the query results from the database:\n{query_result}\n\nNow give me a direct, friendly chatbot answer based on this."})
            
            final_response = ollama.chat(model=self.model, messages=messages)
            final_content = final_response['message']['content']
            return re.sub(r'<think>.*?</think>', '', final_content, flags=re.DOTALL).strip()
        
        # If it's Python code, strategy advice, or normal text, return it directly without trying to query DuckDB!
        return assistant_message

if __name__ == "__main__":
    rag = AgenticRAG()
    print("=== 🤖 Data Factory Chatbot Initialized ===")
    print("Ask me anything about your data, models, or pipeline (type 'exit' or press Ctrl+C to quit):\n")
    
    while True:
        try:
            user_input = input("👤 User > ")
        except (KeyboardInterrupt, EOFError):
            print("\n\n👋 Interrupted. Chat session closed. Goodbye!")
            break
            
        if user_input.lower() in ["exit", "quit"]:
            print("Chatbot exiting. Goodbye!")
            break
        if not user_input.strip():
            continue
            
        print("⚙️ Thinking...")
        response = rag.run_agent(user_input)
        print(f"\n⚙️ Agent > {response}\n" + "-"*50 + "\n")