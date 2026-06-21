import duckdb
import pandas as pd
import os

class IngestionFactory:
    def __init__(self):
        # Initialize the high-speed memory bridge once
        self.connection = duckdb.connect(database=":memory:")

    def load_data_source(self, source_config: dict):
        """
        Ingestion Core: Detects file format dynamically and extracts data.
        """
        file_format = source_config.get("format", "").upper()
        file_path = source_config.get("path")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Target data file missing: {file_path}")

        # Factory Logic
        if file_format == "CSV":
            query = f"SELECT * FROM read_csv_auto('{file_path}')"
            return self.connection.execute(query).fetch_df()
            
        elif file_format == "PARQUET":
            query = f"SELECT * FROM read_parquet('{file_path}')"
            return self.connection.execute(query).fetch_df()
            
        elif file_format == "JSON":
            return pd.read_json(file_path)
            
        elif file_format == "EXCEL":
            return pd.read_excel(file_path)
            
        else:
            raise ValueError(f"Unsupported operational format: {file_format}")