# The Forge: The Data Factory Master Manual

## 1. System Vision & Architecture Philosophy
* **Project Name:** The Forge (Data Operating System)
* **Status:** In Development (Building the parachute in the sky)
* **Core Philosophy:** **"Blueprint Over Syntax."** We prioritize absolute modularity, reproducibility, and a "Human-in-the-Loop" architecture. By decoupling logic into an internal toolbox (`/templates/`), we treat our code as a reusable framework rather than a rigid, single-use script.
* **Operational Strategy:** Every line of code added must be documented with the technical *Why* behind the decision to prevent "tutorial paralysis" and build genuine system intuition.

---

## 2. Global Directory Architecture
*Visual layout of the workspace layers:*

```text
data_factory/
├── data/               # Data Storage Layer (Never mix logic and storage)
│   ├── raw/            # [Immutable] Original source files (Never write here)
│   ├── processed/      # [Mutable] Intermediate clean/engineered data
│   └── outputs/        # [Mutable] Final serialized models and business reports
├── templates/          # [The Toolbox] Reusable logic modules (The "Bricks")
│   ├── __init__.py     # Makes the directory an importable Python package
│   ├── ingestion.py    # Data loading and acquisition logic
│   ├── validation.py   # Schema integrity and quality control scripts
│   ├── cleaning.py     # Outlier and noise reduction utilities
│   ├── features.py     # Mathematical feature transformation wrappers
│   ├── modeling.py     # Model training, hyperparameter loops, and evaluation
│   └── deployment.py   # Model serialization and artifact saving
├── configs/            # [Configuration Layer] Central settings files
│   └── pipeline.yaml   # Removes hardcoded variables (paths, thresholds, hyperparameters)
├── main.py             # [The Engine] Central pipeline orchestration script
├── master_pipeline.md  # [The Source of Truth] This manual
└── requirements.txt    # [Dependency Manifest] Environment and reproducibility control