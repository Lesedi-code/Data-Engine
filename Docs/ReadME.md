# A Configuration-Driven ETL Data Engine

A lightweight, configuration-driven ETL (Extract, Transform, Load) framework designed to ingest raw flat files, execute automated data cleaning based on declarative YAML rules, and stream optimized memory matrices directly into a local PostgreSQL staging environment.

## 🚀 Architectural Vision

This engine is built to enforce a **strict separation of infrastructure and configuration**. Instead of hardcoding transformation logic for every new dataset, pipeline behavior is entirely controlled by a central configuration matrix. This keeps the execution code lean, fast, and easily scalable to new data streams.

- **Data Scientist Focus:** Lean text-based diagnostic profiling provides immediate visual shape and sparsity feedback without the bloat of rendering heavy analytical charts during execution loops.
- **High-Performance Streaming:** Bypasses slow, row-by-row database insertion methods by using optimized binary copying mechanisms to stream internal memory matrices directly into PostgreSQL targets.

---

## 💻 Technical Stack & Environment

- **Host Environment:** macOS Core Architecture (Apple MacBook Pro)
- **Primary IDE & Tools:** Jupyter Lab & Terminal-native interfaces
- **Database Architecture:** Postgres.app server running locally on `localhost`
- **Data Architecture Pattern:** Medallion Staging Concept (Raw files ➡️ In-memory Wrangling ➡️ Postgres Bronze Staging Layer)
- **Core Dependencies:** Python 3, Pandas, PyYAML, SQLAlchemy, PyArrow, OpenPyXL, PyTest

---

## 🛠️ Data Engine Mechanics & Workflow

The architecture is decoupled into distinct processing phases inside the main execution loop:

```text
├── config.yaml             # Master pipeline matrix & declarative wrangling rules
├── pipeline.py             # Automated runtime orchestrator
├── requirements.txt        # Production-ready package specifications
└── templates/
    ├── Wrangler.py         # Automated data cleaning implementation classes
    ├── ingestion.py        # PostgreSQL connection pool handling factories
    └── profiler.py         # In-memory shape and sparsity diagnostic utilities
