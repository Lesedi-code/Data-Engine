# Data Engine: A Configuration-Driven ETL Pipeline

---

## 🚀 Architectural Vision

This engine is built to enforce a **strict separation of infrastructure and configuration**. Instead of hardcoding transformation logic for every new dataset, pipeline behavior is controlled by a central configuration matrix. This keeps execution code lean, fast, and easily scalable across data streams.

* **Data Science & Analytics Focus:** Automated diagnostic profiling provides immediate visual shape and missing-value density maps directly in `reports/`.
* **Medallion Data Architecture:**
  * **Bronze Layer (Ingestion & Profiling):** Raw multi-format imports (`.csv`, `.json`, `.xlsx`) streamed straight into PostgreSQL `bronze_*` staging tables.
  * **Silver Layer (Cleaning & Standardization):** Missing value imputation, date parsing, string normalization (`INITCAP`), and primary key deduplication.
  * **Gold Layer (Aggregation & Delivery):** Automated extraction from Silver into business-ready aggregate tables (`gold_*`), reporting CSV exports, and executive visual analytics.
* **High-Performance Streaming:** Bypasses slow row-by-row insertions by using optimized SQLAlchemy engines and binary streaming mechanisms straight into PostgreSQL targets.

---

## 🧠 Engineering Design & Architecture Decisions

## 🧠 Engineering Design & Architecture Decisions

This section outlines the core engineering philosophy, architectural trade-offs, and design patterns implemented in Version 2 (v2) of the Data Engine to ensure scalability, maintainability, and enterprise readiness.

### 1. Configuration-Driven vs. Hardcoded ETL Pipelines
* **The Challenge:** Traditional ETL pipelines hardcode transformation logic, source schemas, and table mappings directly into Python scripts. Adding a new dataset or modifying a column mapping requires rewriting core pipeline execution code, introducing regression risks and high maintenance overhead.
* **The Solution:** We decoupled execution logic from configuration. The engine reads a central declarative matrix (`configs/pipeline.json`) that dictates source paths, schemas, and target layers.
* **Engineering Takeaway:** *Demonstrates adherence to the Open-Closed Principle (SOLID) and production-grade engineering principles where code is reusable and business logic changes are handled via configuration rather than redeploying source code.*

### 2. Medallion Data Architecture (Bronze ➔ Silver ➔ Gold)
* **The Challenge:** Raw incoming data (multi-format CSVs, JSONs, Excel files) often contains anomalies, duplicates, and missing values. Pushing raw data directly to reporting tools results in corrupted dashboards and incorrect business metrics.
* **The Solution:** Implemented a strict 3-tier Medallion architecture:
  * **Bronze Layer:** Acts as a faithful, immutable landing zone preserving raw multi-format imports inside PostgreSQL staging tables with automated data profiling.
  * **Silver Layer:** Standardizes data via deterministic cleaning—handling missing value imputation, date parsing, string normalization (`INITCAP`), and primary key deduplication.
  * **Gold Layer:** Pre-computes business-ready aggregate tables (`gold_*`) and metrics designed for direct sub-second consumption by BI tools like Tableau.
* **Engineering Takeaway:** *Shows a deep understanding of modern data engineering best practices, separating data ingestion, cleansing, and serving concerns.*

### 3. Containerized Isolation via Docker Compose
* **The Challenge:** Dependency drift and "it works on my machine" syndromes disrupt deployments, especially when managing local PostgreSQL database instances alongside Python runtime environments.
* **The Solution:** Fully containerized the data pipeline and database engine using **Docker Compose**. Python transformation scripts execute inside an isolated container (`etl_engine`) with volume-mounted local code files.
* **The Architectural Win:** Local file-system volume mounting allows developers to instantly edit code and run local transformations without triggering expensive container image rebuilds (`--build`), balancing rapid developer iteration with containerized environment reproducibility.
* **Engineering Takeaway:** *Proves proficiency in modern DevOps, container networking, volume persistence, and creating seamless developer experiences.*

### 4. High-Performance Bulk Streaming & Ingestion
* **The Challenge:** Standard row-by-row database insertions (`INSERT INTO ... VALUES`) scale poorly and bottleneck heavily when processing tens of thousands of records.
* **The Solution:** Bypassed row-by-row bottlenecks by leveraging optimized SQLAlchemy engines coupled with native high-performance binary streaming mechanisms straight into PostgreSQL target schemas.
* **Engineering Takeaway:** *Highlights optimization-minded coding practices and performance tuning for high-volume data pipelines.*





---

## 🐳 Running with Docker Compose

1. **Start the database and container environment:**
   ```bash
   docker compose up -d
