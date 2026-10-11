# Data Engine: A Configuration-Driven ETL Pipeline

## 🚀 Architectural Vision

This engine is built to enforce a **strict separation of infrastructure and configuration**. Instead of hardcoding transformation logic for every new dataset, pipeline behavior is controlled by a central configuration matrix. This keeps execution code lean, fast, and easily scalable across data streams.

###  Core Pillars

- **Data Science & Analytics Focus:** Automated diagnostic profiling provides immediate visual shape and missing-value density maps.
- **Medallion Data Architecture:**
  - **Bronze Layer (Ingestion & Profiling):** Raw multi-format imports (`.csv`, `.json`, `.parquet`, `.xlsx`) streamed straight into PostgreSQL `bronze_*` staging tables with threshold-gated format recommendations.
  - **Silver Layer (Cleaning & Standardization):** Missing value imputation, date parsing, string normalization (`INITCAP`), primary key deduplication, and interactive AI-driven SQL transformation gates powered by local Ollama (`gemma4:e4b`).
  - **Gold Layer (Aggregation & Delivery):** Automated extraction from Silver into business-ready aggregate tables (`gold_*`), reporting CSV exports, and executive visual analytics.
- **High-Performance Streaming:** Bypasses slow row-by-row insertions by using optimized SQLAlchemy engines and binary/chunked streaming mechanisms straight into PostgreSQL targets.

---

## 📐 System Architecture Diagrams

### 1. High-Level Data Flow Topology

```text
  [ Raw Data Sources ]
  ├── .csv (Chunked / Batched Stream >500MB Warning)
  ├── .parquet (Native Arrow Batched Stream)
  ├── .xlsx / .xls (Local Parsing / >25MB Warning)
  └── .json (Standard / Normalized >100MB Warning)
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│              UNIFIED MEDALLION ORCHESTRATOR                     │
│                   (src/pipeline.py CLI)                         │
└────────┬────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL STAGING DB                      │
│                                                                 │
│   ┌────────────────┐     ┌────────────────┐     ┌───────────┐   │
│   │  BRONZE SCHEMA │ ──► │  SILVER SCHEMA │ ──► │GOLD SCHEMA│   │
│   │  (Raw Landing) │     │ (Cleaned/Stg)  │     │(Curated)  │   │
│   └────────────────┘     └───────┬────────┘     └─────┬─────┘   │
└──────────────────────────────────┼────────────────────┼─────────┘
                                   │                    │
                                   ▼                    ▼
                           ┌───────────────┐    ┌───────────────┐
                           │ Gemma AI Gate │    │ Gold CSV      │
                           │(Ollama Local) │    │ Local Exports │
                           └───────────────┘    └───────────────┘

---
```

### 2. Medallion Execution Pipeline & AI Gate Sequence
```text
[ Raw File Input ]
        │
        ▼
 ┌───────────────┐
 │ BRONZE LAYER  │ ──► Stream & Persist raw data to PostgreSQL 'bronze' schema
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  BRONZE GATE  │ ──► Interactive CLI Inspection (Ask Gemma AI / Continue)
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │ SILVER LAYER  │ ──► Apply cleaning, deduplication, and schema standardization
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  SILVER GATE  │ ──► AI Transformation Loop:
 └───────┬───────┘     ├── 1. Engineer requests transformation (e.g. "Drop nulls")
         │             ├── 2. Gemma AI drafts SQL query via local Ollama
         │             ├── 3. Transaction-safe execution with auto-rollback on error
         │             └── 4. Human-in-the-loop interactive review & retry
         ▼
 ┌───────────────┐
 │  GOLD LAYER   │ ──► Promote clean Silver tables to 'gold' schema & export CSV
 └───────────────┘

```
## 🧠 Engineering Design & Architecture Decisions

This section outlines the core engineering philosophy, architectural trade-offs, and design patterns implemented in the Data Engine to ensure scalability, maintainability, and enterprise readiness.

### 1. Configuration-Driven vs. Hardcoded ETL Pipelines

- **The Challenge:** Traditional ETL pipelines hardcode transformation logic, source schemas, and table mappings directly into Python scripts. Adding a new dataset or modifying a column mapping requires rewriting core pipeline execution code, introducing regression risks and high maintenance overhead.
- **The Solution:** We decoupled execution logic from configuration. The engine reads a central declarative matrix that dictates source paths, formats, schemas, and target layers.
- **Engineering Takeaway:** *Demonstrates adherence to the Open-Closed Principle (SOLID) and production-grade engineering principles where code is reusable and business logic changes are handled via configuration rather than redeploying source code.*

### 2. Medallion Data Architecture (Bronze ➔ Silver ➔ Gold)

- **The Challenge:** Raw incoming data (multi-format CSVs, JSONs, Parquet, and Excel workbooks) often contains anomalies, duplicates, and missing values. Pushing raw data directly to reporting tools results in corrupted dashboards and incorrect business metrics.
- **The Solution:** Implemented a strict 3-tier Medallion architecture:
  - **Bronze Layer:** Acts as a faithful, immutable landing zone preserving raw multi-format imports inside PostgreSQL staging tables with automated file discovery and size-gated format warnings.
  - **Silver Layer:** Standardizes data via deterministic cleaning, paired with an interactive AI transformation gate that permits local LLMs (Gemma) to draft, review, and execute safe DML/DDL queries with automatic transaction rollbacks on failure.
  - **Gold Layer:** Pre-computes business-ready aggregate tables (`gold_*`) and metrics designed for direct consumption by downstream BI tools.
- **Engineering Takeaway:** *Shows a deep understanding of modern data engineering best practices, separating data ingestion, cleansing, and serving concerns.*

### 3. Containerized Isolation & Local-First Agility

- **The Challenge:** Dependency drift and environmental inconsistencies disrupt deployments, especially when managing local PostgreSQL database instances alongside Python runtime environments.
- **The Solution:** Containerized the database infrastructure using **Docker Compose** while keeping execution lightweight and flexible via local virtual environments on Apple Silicon (macOS M4 Pro).
- **The Architectural Win:** Balances robust containerized database persistence with lightning-fast local Python runtime execution for local AI assistant loops (Ollama).
- **Engineering Takeaway:** *Proves proficiency in modern infrastructure orchestration, local-first LLM tooling, and creating seamless developer workflows.*

### 4. High-Performance Bulk Streaming & Ingestion

- **The Challenge:** Standard row-by-row database insertions (`INSERT INTO ... VALUES`) scale poorly and bottleneck heavily when processing tens of thousands of records.
- **The Solution:** Bypassed row-by-row bottlenecks by leveraging optimized SQLAlchemy engines coupled with native high-performance binary streaming mechanisms and chunked iteration (e.g., 500k-row batching for large CSV/Parquet files) straight into PostgreSQL target schemas.
- **Engineering Takeaway:** *Highlights optimization-minded coding practices and performance tuning for high-volume data pipelines.*

