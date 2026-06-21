import json
import os
import yaml

# Pulling our custom tool directly out of our project toolbox!
from templates.ingestion import IngestionFactory
from templates.validation import DataValidator

def run_pipeline():
    #We must open and load our YAML config right here inside the function!
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        
    print("\n==============================================")
    print("[INITIALIZING] The Forge: The Data Factory Engine")
    print("==============================================")
    
    # Instantiate our professional toolboxes
    ingestor = IngestionFactory()
    validator = DataValidator()
    
    # Process every data path specified in the configuration matrix
    for dataset_key, dataset_meta in config["datasets"].items():
        print(f"\n🚀 [START] Processing stream: {dataset_key}")
        print("-" * 46)
        
        try:
            # PHASE 1: Dynamic Ingestion
            df = ingestor.load_data_source(dataset_meta)
            
            if df is None:
                print(f"❌ [FAILED] Ingestion engine returned empty data for: {dataset_key}")
                continue
                
            # PHASE 2: Dynamic Validation rules pulled straight from YAML
            validation_rules = dataset_meta.get("validation", {})
            required_cols = validation_rules.get("required_columns", [])
            
            # Execute checks dynamically
            validator.validate_schema(df, required_cols)
            validator.check_nulls(df)
            
            # If it passes everything:
            print(f"✅ [SUCCESS] Schema verification flawless for: {dataset_key}")
            print(f"📊 Rows Processed: {len(df)}")
            print(df.head(2)) # Show a tiny glance of clean data
            
        except FileNotFoundError as e:
            print(f"⚠️  [SKIP] Skipping '{dataset_key}' due to missing file structure:\n   {e}")
        except Exception as e:
            # Catch validation errors or parsing failures here without killing the loop!
            print(f"🛑 [ALERT] Validation halted for '{dataset_key}': {e}")
            
        print(f"🏁 [FINISHED] Data stream workflow complete for: {dataset_key}")
        print("==============================================")


if __name__ == "__main__":
    run_pipeline()