import yaml
import pandas as pd
from templates.ingestion import IngestionFactory
from templates.validation import DataValidator
from templates.analytics import DiscoveryProfiler

class InteractiveGatekeeper:
    """Manages all human-in-the-loop diagnostic menus, heuristics verification, and overrules."""
    
    @staticmethod
    def prompt_menu(title, options):
        print(f"\n📋 [CHOOSE] {title}")
        for k, v in options.items():
            print(f"   [{k}] {v}")
        while True:
            choice = input("👉 Enter your selection: ").strip()
            if choice in options:
                return choice
            print("❌ Invalid selection. Please choose from the active menu items.")

    def handle_cleaning_phase(self, df, dataset_key):
        """Scans for mixed dates, duplicates, and text casing issues, verifying decisions with the user."""
        print("\n🛠️  [PHASE 3] Running Structural Diagnostics...")
        
        # 1. Deduplication Verification Check
        if df.duplicated().any():
            opts = {"1": "Purge all duplicate rows completely", "2": "Retain duplicates for feature exploration"}
            if self.prompt_menu(f"Heuristics suspect {df.duplicated().sum()} records are redundant duplicates. Action?", opts) == "1":
                df = df.drop_duplicates()
                print("   🧹 Duplicates dropped.")
                
        # 2. Text Casing Anomalies Check
        for col in df.select_dtypes(include=['object', 'string']).columns:
            lowered_counts = df[col].astype(str).str.lower().nunique()
            actual_counts = df[col].nunique()
            if lowered_counts < actual_counts:
                opts = {"1": f"Standardize text in '{col}' to Lowercase", "2": "Leave variations unaltered"}
                if self.prompt_menu(f"Heuristics suspect column '{col}' has text casing typos (e.g. 'Tier 1' vs 'tier 1'). Action?", opts) == "1":
                    df[col] = df[col].astype(str).str.lower()
                    print(f"   🔄 Lowercase normalization applied to '{col}'.")
                    
        # 3. Structural Datatype Guess Verification
        for col in df.columns:
            if 'date' in col.lower() or 'time' in col.lower():
                opts = {"1": "Yes, convert and normalize to datetime format", "2": "No, treat this feature as plain text/categorical"}
                if self.prompt_menu(f"Heuristics guess column '{col}' represents a Date/Time value. Confirm?", opts) == "1":
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    print(f"   📅 Parsed '{col}' as structural datetime vector.")
                    
        return df

    def handle_eda_phase(self, df, profiler, dataset_key):
        """Surfaces analytical statistics and requests permission to render physical plots."""
        print("\n📊 [PHASE 4] Moving to Exploratory Data Analysis...")
        
        # 1. Verify Sparsity Visualization Intent
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            print(f"   ⚠️  Sparsity Detected: {null_counts.sum()} total empty cells across features.")
            opts = {"1": "Generate and save structural Sparsity Matrix plot", "2": "Skip this plot"}
            if self.prompt_menu("Heuristics recommend rendering a Sparsity Map. Confirm?", opts) == "1":
                profiler.analyze_sparsity(df, dataset_key)
        else:
            print("   ✨ Data Density: 100% complete across all features.")

        # 2. Verify Correlation Visualization Intent
        numeric_cols = df.select_dtypes(include=['number']).shape[1]
        if numeric_cols >= 2:
            opts = {"1": "Generate and save Feature Correlation Matrix heatmap", "2": "Skip this plot"}
            if self.prompt_menu(f"Heuristics found {numeric_cols} numeric features capable of co-dependence. Render heatmap?", opts) == "1":
                profiler.analyze_correlations(df, dataset_key)
        else:
            print("   ℹ️  Correlation Skip: Insufficient numeric variables to cross-correlate.")

    def handle_modeling_phase(self, df, target_variable):
        """Surfaces target metrics, presents a mathematical problem hypothesis, and allows an override."""
        print("\n🧠 [PHASE 5 & 6] Initializing Modeling Strategy...")
        
        if target_variable not in df.columns:
            print("   ℹ️  No Target selected. Defaulting to Unsupervised layouts.")
            options = {"1": "K-Means Clustering", "2": "Isolation Forest (Anomaly Detection)", "3": "PCA Dimensionality Reduction"}
            return options, "Unsupervised"
            
        # Collect baseline metrics for our user evaluation menu
        unique_count = df[target_variable].nunique()
        data_type = df[target_variable].dtype
        
        # Engine Formulates its Hypothesis
        if data_type in ['int64', 'float64'] and unique_count > 10:
            guessed_type = "Regression"
        else:
            guessed_type = "Classification"
            
        print(f"   🔬 Heuristic Analysis on Target '{target_variable}':")
        print(f"      - Data Type: {data_type}")
        print(f"      - Unique Element States: {unique_count}")
        print(f"      - System Guess: This looks like a [{guessed_type}] framework layout.")
        
        # The Overrule Junction
        opts = {
            "1": f"Accept Engine Guess ({guessed_type} Modeling Framework)",
            "2": "Overrule: Force Classification instead (Discrete classes/categories)",
            "3": "Overrule: Force Regression instead (Continuous numerical scales)"
        }
        user_frame = self.prompt_menu("Do you want to accept or overrule the system's structural hypothesis?", opts)
        
        # Map out the final chosen structure based on the choice code
        if user_frame == "1":
            final_type = guessed_type
        elif user_frame == "2":
            final_type = "Classification"
        else:
            final_type = "Regression"
            
        # Return the correct targeted modeling algorithm array
        if final_type == "Regression":
            options = {"1": "Linear Regression", "2": "Random Forest Regressor", "3": "LightGBM Regressor"}
        else:
            options = {"1": "Logistic Regression", "2": "XGBoost Classifier", "3": "Random Forest Classifier"}
            
        return options, final_type


def run_pipeline():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        
    print("\n==============================================")
    print("🚀 THE FORGE: DYNAMIC HUMAN-IN-THE-LOOP ENGINE")
    print("==============================================")
    
    ingestor = IngestionFactory()
    validator = DataValidator()
    profiler = DiscoveryProfiler()
    ui = InteractiveGatekeeper()
    
    for dataset_key, dataset_meta in config["datasets"].items():
        print(f"\n📦 RUNNING DATASTREAM WORKFLOW: {dataset_key}")
        print("-" * 46)
        
        try:
            # Phase 2: Ingestion
            df = ingestor.load_data_source(dataset_meta)
            if df is None: continue
            
            # Phase 3: Interactive Cleaning & Structural Approvals
            df = ui.handle_cleaning_phase(df, dataset_key)
            
            # Phase 4: Schema Validation & User-Approved EDA Profiling
            validation_rules = dataset_meta.get("validation", {})
            required_cols = validation_rules.get("required_columns", [])
            validator.validate_schema(df, required_cols)
            
            # Run interactive EDA validations
            ui.handle_eda_phase(df, profiler, dataset_key)
            
            # Target Vector Assignment Configuration Menu
            col_menu = {str(i+1): col for i, col in enumerate(df.columns)}
            col_menu[str(len(df.columns) + 1)] = "No Target / Run Unsupervised Analysis"
            tgt_choice = ui.prompt_menu("Select your Target variable (Y) for modeling:", col_menu)
            target_var = col_menu[tgt_choice]
            
            # Phase 5 & 6: Interactive Hypothesis Approval Matrix
            model_menu, problem_type = ui.handle_modeling_phase(df, target_var)
            algo_choice = ui.prompt_menu(f"Executing under confirmed [{problem_type}] architecture. Select Algorithm:", model_menu)
            
            print(f"\n✅ Baseline pipeline sequence verified for '{dataset_key}' using {model_menu[algo_choice]}!")
            
        except Exception as e:
            print(f"🛑 [ALERT] Framework execution paused for '{dataset_key}': {e}")
            
        print(f"🏁 Workflow complete for dataset stream: {dataset_key}")
        print("==============================================")

if __name__ == "__main__":
    run_pipeline()