import pandas as pd

class DataValidator:
    def __init__(self):
        # We can store validation reports here
        self.last_report = None

    def validate_schema(self, df: pd.DataFrame, required_columns: list):
        """Checks if the dataframe contains all necessary columns."""
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Schema Validation Failed. Missing columns: {missing}")
        return True

    def check_nulls(self, df: pd.DataFrame, threshold=0.05):
        """Flags columns where null values exceed a certain percentage."""
        null_ratios = df.isnull().mean()
        high_null_cols = null_ratios[null_ratios > threshold]
        
        if not high_null_cols.empty:
            print(f"Warning: High null count in: {high_null_cols.to_dict()}")
        return True

    def validate_types(self, df: pd.DataFrame, type_map: dict):
        """Verifies that columns match expected data types."""
        for col, expected_type in type_map.items():
            if col in df.columns and df[col].dtype != expected_type:
                raise TypeError(f"Type Mismatch: {col} expected {expected_type}, got {df[col].dtype}")
        return True
        
          
                       
            
    
                            