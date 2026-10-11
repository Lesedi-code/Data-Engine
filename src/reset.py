import argparse
import os
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

def load_config(config_path: str = "config/pipeline.yaml") -> dict:
    """Load configuration settings from YAML."""
    if not os.path.exists(config_path):
        config_path = "config.yaml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found.")
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def get_db_engine(config: dict):
    """Establish SQLAlchemy engine."""
    targets = config.get("targets", {})
    local_pg = targets.get("local_postgres", {})
    uri = local_pg.get("connection_uri")

    if not uri:
        db_cfg = config.get("database", {})
        user = db_cfg.get("user", "engine_user")
        password = db_cfg.get("password", "engine_password_123")
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 5432)
        dbname = db_cfg.get("dbname", "staging_db")
        uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    url_obj = make_url(uri)
    if not os.path.exists("/.dockerenv") and url_obj.host in ["postgres_db", "postgres"]:
        url_obj = url_obj._replace(host="localhost")

    return create_engine(url_obj)

def reset_medallion_environment(layer: str = "all"):
    """Reset specified Medallion layer(s) across PostgreSQL and local directories."""
    try:
        config = load_config()
    except Exception:
        config = {}

    dirs = config.get("directories", {})
    target_layers = ["bronze", "silver", "gold"] if layer in ["all", "full"] else [layer.lower()]

    print(f"\n================================================================================")
    print(f"               DATA ENGINE - GRANULAR RESET: {layer.upper()}                    ")
    print(f"================================================================================")

    try:
        engine = get_db_engine(config)
    except Exception as e:
        print(f"[WARN] Could not connect to PostgreSQL for database reset: {e}")
        engine = None

    for l in target_layers:
        if engine:
            try:
                with engine.begin() as conn:
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{l}" CASCADE;'))
                    conn.execute(text(f'CREATE SCHEMA "{l}";'))
                    print(f"✔ PostgreSQL: Reset schema -> {l}")
            except Exception as e:
                print(f"[ERROR] Could not reset PostgreSQL schema '{l}': {e}")

        if l == "silver":
            silver_dir = dirs.get("silver_staging", "Data/silver_staging")
            if os.path.exists(silver_dir):
                for item in os.listdir(silver_dir):
                    item_path = os.path.join(silver_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                print(f"✔ Cleared local directory: {silver_dir}")

        elif l == "gold":
            gold_dir = dirs.get("gold_exports", "Data/gold_exports")
            if os.path.exists(gold_dir):
                for item in os.listdir(gold_dir):
                    item_path = os.path.join(gold_dir, item)
                    if os.path.isfile(item_path):
                        os.remove(item_path)
                print(f"✔ Cleared local directory: {gold_dir}")

    print("================================================================================\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Standalone Granular Reset Utility for Data Factory")
    parser.add_argument("--layer", type=str, default="all", choices=["bronze", "silver", "gold", "all"], help="Target layer to reset")
    args = parser.parse_args()

    reset_medallion_environment(layer=args.layer)