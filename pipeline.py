import json
import os
# Pulling our custom tool directly out of our project toolbox!
from templates.ingestion import load_data_source

print("\n============================================")
print("[INTIALIZING] The Forge: The Data Factory Engine")
print("===========================================")

# 1. Read the Control Panel Settings
CONFIG_PATH = "configs/pipeline.json"

with open(CONFIG_PATH, "r") as file_stream:
    configuration = json.load(file_stream)

# 2. Extract runtime details dynamically
active_key = configuration.get("active_source")
active_source_settings = configuration["data_sources"][active_key]

print(f"[CONFIG RUNTIME] Environment: {configuration['environment'].upper()}")
print(f"[ROUTING ENGINE] Active key: {active_key}")
print(f"[ROUTING ENGINE] Parsing Path: {active_source_settings['path']}")

# 3. Hand off execution control to our specialized toolbox module
try:
    processed_dataframe = load_data_source(active_source_settings)
    print("\n[SUCCESS] Extraction Pipeline Executed Flawlessly:\n")
    print(processed_dataframe)

except Exception as engine_error:
    print(f"\n[CRITICAL ERROR] The Forge execution halted!")
    print(f"Diagnostics: {str(engine_error)}")

print("===================================================")