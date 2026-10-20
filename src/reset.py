import argparse
import os
import shutil
import duckdb
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def load_config(config_path: str = "config/pipeline.yaml") -> dict:
    """Load configuration settings from YAML."""
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_db_engine(config: dict):
    """Establish SQLAlchemy engine for PostgreSQL if available."""
    targets = config.get("targets", {})
    local_pg = targets.get("local_postgres", {})
    uri = local_pg.get("connection_uri")

    if not uri:
        db_cfg = config.get("database", {})
        user = db_cfg.get("user", "postgres")
        password = db_cfg.get("password", "postgres")
        host = db_cfg.get("host", "localhost")
        port = db_cfg.get("port", 5432)
        dbname = db_cfg.get("dbname", "staging_db")
        uri = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    try:
        url_obj = make_url(uri)
        if not os.path.exists("/.dockerenv") and url_obj.host in ["postgres_db", "postgres"]:
            url_obj = url_obj._replace(host="localhost")
        return create_engine(url_obj)
    except Exception:
        return None


def reset_medallion_environment(layer: str = "all"):
    """Resets specific or all Medallion layers except Bronze, preserving raw data."""
    # Filter out 'bronze' to ensure raw data schemas are never dropped during a reset
    requested_layers = ["bronze", "silver", "gold"] if layer.lower() in ["all", "full"] else [layer.lower()]
    target_layers = [l for l in requested_layers if l != "bronze"]
    
    if "bronze" in requested_layers:
        print("\nℹ [Notice] Bronze schema is protected and will NOT be wiped.")

    print(f"\n================================================================================")
    print(f"                      DATA ENGINE - RESET (EXCEPT BRONZE)                       ")
    print(f"================================================================================")

    # 1. Reset DuckDB OLAP schemas (excluding bronze)
    db_file = "data_engine.duckdb"
    if os.path.exists(db_file):
        try:
            con = duckdb.connect(db_file)
            for l in target_layers:
                con.execute(f"DROP SCHEMA IF EXISTS {l} CASCADE;")
                con.execute(f"CREATE SCHEMA {l};")
                print(f"✔ [DuckDB] Reset schema -> {l}")
            con.close()
        except Exception as e:
            print(f"[WARN] Could not reset DuckDB schemas: {e}")

    # 2. Reset PostgreSQL schemas (excluding bronze)
    config = load_config("config/pipeline.yaml")
    engine = get_db_engine(config)
    if engine:
        try:
            with engine.begin() as conn:
                for l in target_layers:
                    conn.execute(text(f'DROP SCHEMA IF EXISTS "{l}" CASCADE;'))
                    conn.execute(text(f'CREATE SCHEMA "{l}";'))
                    print(f"✔ [PostgreSQL] Reset schema -> staging_db.{l}")
        except Exception as e:
            print(f"[WARN] PostgreSQL reset skipped or failed: {e}")

    # 3. Clean local staging/export directories if needed
    dirs = config.get("directories", {})
    if layer.lower() in ["all", "full", "silver"]:
        silver_dir = dirs.get("silver_staging", "Data/silver_staging")
        if os.path.exists(silver_dir):
            shutil.rmtree(silver_dir)
            os.makedirs(silver_dir, exist_ok=True)
            print(f"✔ Cleared local directory: {silver_dir}")

    if layer.lower() in ["all", "full", "gold"]:
        gold_dir = dirs.get("gold_exports", "Data/gold_exports")
        if os.path.exists(gold_dir):
            shutil.rmtree(gold_dir)
            os.makedirs(gold_dir, exist_ok=True)
            print(f"✔ Cleared local directory: {gold_dir}")

    print("================================================================================\n")
    print("✔ Environment successfully reset (Bronze raw data preserved).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Engine Layer Reset Utility")
    parser.add_argument("--layer", type=str, default="all", help="Target layer to reset (silver, gold, or all).")
    args = parser.parse_args()

    reset_medallion_environment(layer=args.layer)