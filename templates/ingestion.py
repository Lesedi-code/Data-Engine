import duckdb
import pandas as pd
import os

def load_data_source(source_config: dict):
    """
    Ingestion Core: Detects file format dynamically 
    and handles extraction using the optimal backend engine.
    """
    file_format = source_config.get("format", "").upper()
    file_path = source_config.get("path")
    
    # Defensive Check: Ensure the file actually exists before processing
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Target data file missing at destination path: {file_path}")
        
    # Open a local high-speed memory bridge
    connection = duckdb.connect(database=":memory:")
    
    # Format Routing Matrix (The Factory Design)
    if file_format == "CSV":
        query = f"SELECT * FROM read_csv_auto('{file_path}')"
        return connection.execute(query).fetch_df()
        
    elif file_format == "PARQUET":
        query = f"SELECT * FROM read_parquet('{file_path}')"
        return connection.execute(query).fetch_df()
        
    elif file_format == "JSON":
        # Pandas handles complex nested JSON files smoothly
        return pd.read_json(file_path)
        
    elif file_format == "EXCEL":
        # Pulls the active sheets into a structural grid
        return pd.read_excel(file_path)
        
    else:
        raise ValueError(f"Unsupported operational format framework: {file_format}")