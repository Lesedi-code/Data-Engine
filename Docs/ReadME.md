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

## 📊 Analytics & BI Integration (v0.2)

The engine serves business-ready **Gold Layer** endpoints directly consumed by **Tableau Desktop**:

* **Executive Dashboard:** Live 2-tier dashboard architecture monitoring portfolio performance.
* **Key Visual Endpoints:**
  * **Category Metrics (`gold_category_metrics`):** Top categories ranked by total inventory value (R134M+ working capital).
  * **Price Brackets (`gold_price_brackets`):** Product distribution mapped across Budget, Mid-Range, Premium, and High-End tiers.
  * **Inventory Alerts (`gold_inventory_alerts`):** Operational supply chain health tracking (Healthy Stock vs. Low Stock Warnings).
* **Documentation:** Executive analytics report, observations, and engineering challenge breakdown published in `Docs/`.

---

## 💻 Technical Stack & Environment

* **Host Environment:** macOS Core Architecture (Apple MacBook Pro)
* **Primary IDE & Tools:** Visual Studio Code & Terminal-native interfaces
* **Visualization & Reporting:** Tableau Desktop, Automated Executive DOCX Reporting
* **Containerization & Database:** Docker Compose running PostgreSQL 15 Engine
* **Data Architecture Pattern:** Full Medallion Staging Concept (Raw Inputs ➔ Bronze ➔ Silver ➔ Gold)
* **Core Dependencies:** Python 3.11, Pandas, PyYAML, SQLAlchemy, Psycopg2-Binary, Matplotlib, Seaborn