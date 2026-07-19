import yaml
import io 
import pandas as pd 
from templates.ingestion import IngestionFactory
from templates.profiler import DiscoveryProfiler
from templates.Wrangler import DataWrangler

def run_pipeline():
    print("\n===================================================")
    print("🚀 THE FORGE: DATA ENGINE")
    print("=====================================================")

    # 1. Load the central configuration mapping rules
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    db_uri = config.get("connection_uri", "postgresql://localhost/postgres")
    datasets_map = config.get("datasets", {})

    # 2. Initialize our specialized modular architectural components
    ingest_factory = IngestionFactory(connection_uri=db_uri)
    profiler = DiscoveryProfiler(output_dir="Docs/plots")
    wrangler = DataWrangler()

    try:
        # 3. Step through our configuration matrix streams sequentially
        for dataset_key, meta in datasets_map.items():
            print(f"\n📦 RUNNING DATASTREAM WORKFLOW: {dataset_key}")
            print("_" * 46)

            file_path = meta.get("path")
            file_format = meta.get("format").upper()

            # --- Phase 1: Ingestion Loading into Pandas memory space ---
            print(f"📥 Loading file template dynamically from: {file_path}")
            if file_format == "PARQUET":
                df = pd.read_parquet(file_path)
            elif file_format == "CSV":
                df = pd.read_csv(file_path)
            elif file_format == "JSON":
                df = pd.read_json(file_path)
            elif file_format == "EXCEL":
                df = pd.read_excel(file_path)
            else:
                print(f"🛑 [SKIP] Unsupported dataset stream format: {file_format}")
                continue
                
            #--- Phase 2: Lightweight Data Science Discovery Profiling ---
            profiler.analyze_raw_shape(df, dataset_key)
            # Plot generation removed here to keep focus entirely on the raw text diagnostics

            # --- Phase 3: Configuration-Driven Automated Data Wrangling ---
            wrangle_rules = meta.get("wrangling", {})
            if wrangle_rules:
                df = wrangler.transform(df, wrangle_rules)

            # --- Phase 4: Stream Clean Data matrix straight to Postgres Bronze schema ---
            df.head(0).to_sql(f"bronze_{dataset_key}", ingest_factory.engine, if_exists="replace", index=False)

            raw_conn = ingest_factory.engine.raw_connection()
            try:
                with raw_conn.cursor() as cur:
                    output = io.StringIO()
                    df.to_csv(output, sep='\t', header=False, index=False)
                    output.seek(0)
                    sql_copy = f"COPY bronze_{dataset_key} FROM STDIN WITH CSV DELIMITER '\t' NULL AS '';"
                    cur.copy_expert(sql_copy, output)
                raw_conn.commit()
                print(f"    ✨ SUCCESS: Clean memory matrix streamed into table 'bronze_{dataset_key}'.")
            finally:
                raw_conn.close()

            print(f"✅ Pipeline loop execution complete for stream: '{dataset_key}'")
            print("==============================================")

    except Exception as e:
        print(f"\n🛑 SYSTEM ANOMALY: Pipeline wrapper failed.\nDetail: {e}")

    finally:
        ingest_factory.close()
        print("\n🏁 [FINISHED] Framework execution loop terminated safely.")

if __name__ == "__main__":
    run_pipeline()


