class DataWrangler:
    """Executes programmatic data wrangling based on yaml configuration rules."""
    
    def transform(self, df, wrangle_config):
        if not wrangle_config:
            return df
            
        # 1. Deduplicate
        if wrangle_config.get("deduplicate"):
            df = df.drop_duplicates()
        
        # 2. Impute (Fill missing)
        for col, val in wrangle_config.get("impute_missing", {}).items():
            if col in df.columns:
                df[col] = df[col].fillna(val)
        
        # 3. Standardize Casing
        for col in wrangle_config.get("standardize_casing", []):
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()
                
        return df