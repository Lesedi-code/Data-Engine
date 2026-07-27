"""
===============================================================================
MODULE: Config Loader (Agnostic Data Engine)
PURPOSE: Load and parse pipeline configuration files dynamically for any dataset.
UNDER THE HOOD:
  - Reads settings from JSON without relying on hardcoded table or column names.
  - Exposes connection credentials and pipeline parameters to downstream layers.
===============================================================================
"""


import json
import os
import logging

logging.basicConfig(level=logging.INFO, format="%(ascttime)s [%(levelname)s] %(message)s")

def load_config(config_path: str = "configs/pipeline.json") -> dict:
    """Reads and validates the generic pipeline configuratiion dictionary."""
    if not os.path.exists(config_path):
        if os.path.exists("configs/configs/pipeline.json"):
            config_path = "configs/configs/pipeline.json"
        else:
            raise FileExistsError(f"❌ config file not found: {config_path}")

    with open(config_path, "r") as file:
        try:
            config = json.load(file)
            logging.info(f"✅ Configuration loaded successfully from '{config_path}'")
            return config
        except json.JSONDecodeERROR as e:
            raise ValueError(f"❌ Error parsing JSON file '{config_path}': {e}")

if __name__ =="__main__":
    print("\n" +"="*50)
    print("🧪 TESTING BLOCK 1: CONFIG LOADER")
    print("=*50")

    cfg = load_config()

    print("\nParsed Config Keys:")
    for key in cfg.keys():
        print(f" - {key}")

    print("="*50 + "\n")
    
          