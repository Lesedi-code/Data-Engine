# Data Engine: A Configuration-Driven ETL Pipeline

## 🚀 Architectural Vision

This engine is built to enforce a **strict separation of infrastructure and configuration**. Instead of hardcoding transformation logic for every new dataset, pipeline behavior is controlled by a central configuration matrix. This keeps execution code lean, fast, and easily scalable across data streams.

At its core, this project was built to solve a high-friction problem in data science and data engineering: **eliminating the 80% data wrangling tax** (dealing with null value distributions, schema inconsistencies, trailing characters, and manual deduplication) so data scientists and analytics practitioners can spend 100% of their focus on the **20% high-value modeling, predictive features, and business insights** that organizations actually value.

### Target Audience & Use Case
- **Data Scientists & Analytics Practitioners:** Professionals who need clean, standardized data instantly without wasting hours on mechanical data hygiene chores.
- **Data & Analytics Engineers:** Teams managing complex, multi-format data pipelines who require robust Data Quality (DQ) assertion gates and dual-engine persistence.
- **Enterprise Operations & IT:** Organizations running legacy backend infrastructure (such as Progress OpenEdge systems) that require modern, secure, API-driven data extraction routes without risking database locks or production downtime.

### Core Pillars

- **Data Science & Analytics Focus:** Automated diagnostic profiling provides immediate visual shape, missing-value density maps, and frees practitioners from mechanical data chores.
- **Medallion Data Architecture:**
  - **Bronze Layer (Ingestion & Profiling):** Raw multi-format imports (`.csv`, `.json`, `.parquet`, `.xlsx`) or live enterprise REST streams streamed straight into PostgreSQL and DuckDB `bronze_*` staging tables with automated file discovery and size-gated format warnings.
  - **Silver Layer (Cleaning & Standardization):** Missing value imputation, date parsing, string normalization (`INITCAP`), primary key deduplication, and interactive AI-driven SQL transformation gates powered by local Ollama models.
  - **Gold Layer (Delivery & Exports):** Automated extraction from Silver into business-ready aggregate tables (`gold_*`), reporting CSV exports (`Data/gold/`), and executive visual analytics.
- **High-Performance Streaming:** Bypasses slow row-by-row insertions by using optimized SQLAlchemy engines and binary/chunked streaming mechanisms straight into PostgreSQL and DuckDB OLAP targets.

---

## 📐 System Architecture Diagrams

### 1. High-Level Data Flow Topology

```text
  [ Raw Data Sources / OpenEdge Stream ]
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
│                  DUAL-ENGINE PERSISTENCE LAYER                  │
│             (PostgreSQL Staging DB + DuckDB OLAP)               │
│                                                                 │
│   ┌────────────────┐     ┌────────────────┐     ┌───────────┐   │
│   │  BRONZE SCHEMA │ ──► │  SILVER SCHEMA │ ──► │GOLD SCHEMA│   │
│   │  (Raw Landing) │     │ (Cleaned/Stg)  │     │(Curated)  │   │
│   └────────────────┘     └───────┬────────┘     └─────┬─────┘   │
└──────────────────────────────────┼────────────────────┼─────────┘
                                   │                    │
                                   ▼                    ▼
                           ┌───────────────┐    ┌───────────────┐
                           │ Local AI Gate │    │ Gold CSV      │
                           │(Ollama Models)│    │ Local Exports │
                           └───────────────┘    └───────────────┘
```

### 2. Medallion Execution Pipeline & AI Gate Sequence
```text
[ Raw File Input / OpenEdge Stream ]
        │
        ▼
 ┌───────────────┐
 │ BRONZE LAYER  │ ──► Stream & Persist raw data to PostgreSQL & DuckDB 'bronze' schema
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  BRONZE GATE  │ ──► Automated DQ Assertion Gate (Audits nulls & duplicates)
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │ SILVER LAYER  │ ──► Apply cleaning, deduplication, and schema standardization
 └───────┬───────┘
         │
         ▼
 ┌───────────────┐
 │  SILVER GATE  │ ──► Model-Selectable AI Transformation Loop:
 └───────┬───────┘     ├── 1. Choose active local LLM (e.g., Gemma, DeepSeek)
                       ├── 2. Engineer requests transformation (e.g., "Drop column sku")
                       ├── 3. Selected LLM drafts SQL query via local Ollama
                       ├── 4. Transaction-safe execution against DuckDB/PostgreSQL
                       └── 5. Human-in-the-loop interactive review & retry
         ▼
 ┌───────────────┐
 │  GOLD LAYER   │ ──► Promote clean Silver tables to 'gold' schema & export CSV
 └───────────────┘

```
# 🛠️ Step-by-Step Installation & Execution Guide

### Prerequisites
- **Git** (installed on your machine for cloning)
- **Python 3.10+**
- **Ollama** installed locally with desired models pulled (e.g., `ollama pull gemma4:e4b`).
- **Docker & Docker Compose** (optional, for containerized PostgreSQL staging infrastructure).

## Get the Project Files
You can obtain the project files using either of the following methods:

## IN THE TERMINAL RUN THE FOLLOWING COMMANDS:

## Step 1: Clone and Set Up Virtual Environment
Clone via Git(Recommended for developers): git clone https://github.com/Lesedi-code/Data-Engine.git
cd "Data-Engine"
python -m venv venv
source venv/bin/activate

Option B: Download as ZIP (Alternative for non-Git users)
Go to the GitHub Repository.
Click the green Code button at the top right.
Select Download ZIP, extract the folder on your machine, and open your terminal inside that extracted folder (Data-Engine-main).

## Step 2: Install Dependencies
Install all required Python packages using the provided requirements file: pip install -r requirements.txt

## Step 3: Start Infrastructure Services (Docker Compose)
Spin up the containerized PostgreSQL staging database environment: docker-compose up -d

## Step 4: Launch Local Ollama Service
ollama serve

## Step 5: Coonect to the mock/live enterprise server
python src/mock_openedge_server.py

## Step 6: Run the Master Orchestrator Pipeline
python -u src/pipeline.py

From here, you can select whether to ingest local structured files (Data/raw/Structured_dataset/) or pull a live enterprise stream from the mock Progress OpenEdge PASOE REST API (src/mock_openedge_server.py).

"Note: PostgreSQL / Docker staging is optional. If not detected, the pipeline automatically falls back to run entirely through the high-performance local DuckDB OLAP engine (data_engine.duckdb)."

## For Windows (Powershell/Command Prompt)

git clone [https://github.com/Lesedi-code/Data-Engine.git](https://github.com/Lesedi-code/Data-Engine.git)
cd "Data-Engine"
python -m venv venv
#PowerShell activation:
.\venv\Scripts\Activate.ps1
#OR Command Prompt activation:
venv\Scripts\activate.bat

## Step 2: Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

## Step 3: Start Infrastructure Services (Optional)
docker-compose up -d

## Step 4: Launch Local Ollama Service
ollama serve

## Step 5: Connect to the Mock/Live Enterprise Server (Terminal 1)
python src/mock_openedge_server.py

## Step 6: Run the Master Orchestrator Pipeline (Terminal 2)
python -u src/pipeline.py

# 🧠 Engineering Design & Architecture Decisions

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

