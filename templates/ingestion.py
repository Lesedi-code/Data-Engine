import pandas as pd
from sqlalchemy import create_engine
import io
import os

class IngestionFactory:
    def __init__ (self, connection_uri="postgresql://localhost/postgres"):

        self.engine = create_engine(connection_uri)
        print("🐘 Postgres Engine Pool Intitialized Successfully.")

    def load_data_to_bronze(self, source_config: dict, table_name: str):
        """
        Ingest flat data files dynamically and streams them directly 
        into a dedicated raw Postgres Bronze staging table.
        """
        file_format = source_config.get("format", "").upper()
        file_path = source_config.get("path")

        # Keep your defensive guardrail to check if the file exists on your drive
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Target data file missing: {file_path}")
        print(f"📥 Extracting {file_format} file layout: {file_path}")

        # Extract data into a temporary Pandas matrix based on configuration format
        if file_format == "PARQUET":
            df =pd.read_parquet(file_path)
        elif file_format == "CSV":
            df = pd.read_csv(file_path)
        elif file_format == "JSON":
            df = pd.read_json(file_path)
        elif file_format == "EXCEL":
            df = pd.read_excel(file_path)
        else:
            raise ValueError(f"❌ Unsupported operational format: {file_format}")
        
        # 🚀 Metatable Generation: Build the empty structure in Postgres instantly
        # head(0) sends just the header metadata, ensuring no rows are inserted slowly
        df.head(0).to_sql(f"bronze_{table_name}", self.engine, if_exists="replace", index=False)

        # 🚀 The Production Optimization: Trigger High-Speed Postgres COPY Stream
        raw_conn = self.engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                # Create a virtual text stream file directly inside your RAM
                output = io.StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0) # Rewind the virtual file pointer to the start

                # Use native COPY protocol to pipe the entire block into the DB instantly
                sql_copy = f"COPY bronze _{table_name} FROM STDIN WITH CSV DELIITER '\t' NULL AS '' ;" 
                cur.copy_expert(sql_copy, output)

                raw_conn.commit()
                print(f"   ✨ SUCCESS: Streamed records into Postgres table 'bronze_{table_name}' . ")
        finally:
            raw_conn.close()
        
    def close(self):
        """Safely clean up and drop the database engine connection pool."""
        self.engine.dispose()
        print("🔌  Postgres connection pool closed cleanly.")
        



