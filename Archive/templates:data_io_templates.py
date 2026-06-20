import pandas as pd
from sqlalchemy import create_engine

def load_data(source_path, source_type='csv', db_url=None):
    """
    Standardized Ingestion: Handles CSV, Excel, or SQL.
    Returns: DataFrame
    """
    if source_type == 'csv':
        return pd.read_csv(source_path)
    elif source_type == 'excel':
        return pd.read_excel(source_path)
    elif source_type == 'sql':
        engine = create_engine(db_url)
        return pd.read_sql(source_path, engine) # source_path is the SQL query here
    else:
        raise ValueError("Unsupported source_type. Use 'csv', 'excel', or 'sql'.")