# The Forge: A Configuration-Driven ETL Data Engine

---

## 🚀 Architectural Vision

This engine is built to enforce a **strict separation of infrastructure and configuration**. Instead of hardcoding transformation logic for every new dataset, pipeline behavior is controlled by a central configuration matrix. This keeps execution code lean, fast, and easily scalable across data streams.

- **Data Science & Analytics Focus:** Automated diagnostic profiling provides immediate visual shape and missing-value density maps directly in `reports/`.
- **Medallion Data Architecture:**
  - **Bronze Layer (Ingestion & Profiling):** Raw multi-format imports (`.csv`, `.json`, `.xlsx`) streamed straight into PostgreSQL `bronze_*` staging tables.
  - **Silver Layer (Cleaning & Standardization):** Missing value imputation, date parsing, string normalization (`INITCAP`), and primary key deduplication.
  - **Gold Layer (Aggregation & Delivery):** Automated extraction from Silver into business-ready aggregate tables (`gold_*`) and reporting CSV exports.
- **High-Performance Streaming:** Bypasses slow row-by-row insertions by using optimized SQLAlchemy engines and binary streaming mechanisms straight into PostgreSQL targets.

---

## 💻 Technical Stack & Environment

- **Host Environment:** macOS Core Architecture (Apple MacBook Pro)
- **Primary IDE & Tools:** Visual Studio Code & Terminal-native interfaces
- **Containerization & Database:** Docker Compose running PostgreSQL 15 Engine
- **Data Architecture Pattern:** Full Medallion Staging Concept (Raw Inputs ➡️ Bronze ➡️ Silver ➡️ Gold)
- **Core Dependencies:** Python 3.11, Pandas, PyYAML, SQLAlchemy, Psycopg2-Binary, Matplotlib, Seaborn

---

## 📁 Project Structure

```text
Data Factory/
├── config.yaml               # Master pipeline matrix & declarative wrangling rules
├── requirements.txt        # Production-ready package specifications
└── templates/
    ├── Wrangler.py         # Automated data cleaning implementation classes
    ├── ingestion.py        # PostgreSQL connection pool handling factories
    └── profiler.py         # In-memory shape and sparsity diagnostic utilities