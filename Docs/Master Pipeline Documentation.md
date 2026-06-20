# The Forge: The Data Factory Master Manual

## 1. System Vision & Architecture Philosophy
* **Project Name:** The Forge (Data Operating System)
* **Status:** In Development (Building the parachute in the sky)
* **Core Philosophy:** **"Blueprint Over Syntax."** We prioritize absolute modularity, reproducibility, and a "Human-in-the-Loop" architecture. By decoupling logic into an internal toolbox (`/templates/`), we treat our code as a reusable framework rather than a rigid, single-use script.
* **Operational Strategy:** Every line of code added must be documented with the technical *Why* behind the decision to prevent "tutorial paralysis" and build genuine system intuition.

---

## 2. Global Directory Architecture
*Visual layout of the workspace layers:*

# The Forge: Data Factory Engine Documentation v0.1

## 1. Global Directory Architecture
# The Forge: Data Factory Engine Documentation v0.1

## 1. Global Directory Architecture
data_factory/
├── Data/               # Data Storage Layer
│   └── raw/            # [Immutable Raw Inflow] 
│       ├── market_trends.csv          # (Active) Universal Middleman
│       ├── api_products.json          # (Active) Semi-Structured API Payload
│       ├── supplier_catalog.xlsx      # (Active) Enterprise Supplier Sheet
│       └── historical_demand.parquet  # (Active) High-Performance Binary Columnar
├── templates/          # [The Modular Toolbox]
│   ├── __init__.py     # (Initialized) Package Module Mapping
│   ├── ingestion.py    # (Completed) Dynamic Multi-Format Factory Engine
│   └── validation.py   # (Staged) Quality Control Gatekeeper Framework
├── configs/            # [Decoupled Configuration Layer]
│   └── pipeline.json   # (Completed) Central Ingestion Routing Panel
├── Docs/               # [Documentation & Dependencies Vault]
│   ├── requirements.txt # (Updated) Pinned Package Version Manifest
│   └── master_pipeline.md # (Active) Core Project Manual
├── pipeline.py         # [The Core Orchestration Engine] Wired to Configs & Factory
└── generate_mock_data.py # [Testing Utility] Multi-Format Mock Data Generator