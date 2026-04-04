# Pharmaceutical Supply Chain Optimisation

A machine learning pipeline for predicting pharmaceutical shipment delivery risk. The system processes supply chain data, engineers features, and trains models to identify shipments at risk of late delivery — enabling proactive supply chain management.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Running the Airflow Pipeline](#running-the-airflow-pipeline)
  - [Running the Notebook](#running-the-notebook)
- [Data Pipeline](#data-pipeline)
- [Dataset](#dataset)
- [Technologies](#technologies)
- [Development](#development)

---

## Overview

This project builds an end-to-end ML pipeline to optimise pharmaceutical supply chains by predicting the probability that a shipment will arrive late. The pipeline covers:

- **Data ingestion** – loading raw supply chain records from CSV
- **Feature engineering** – parsing dates, encoding categoricals, handling missing values
- **Data splitting** – stratified train/test split with optional class balancing via upsampling
- **Model training** – experiment tracking through MLflow (Databricks integration)
- **Pipeline orchestration** – scheduled Airflow DAG running each step in sequence

---

## Architecture

```
Raw CSV Data
     │
     ▼
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌───────────────┐
│ import_data │ ──▶ │ process_data │ ──▶ │  split_data │ ──▶ │ Model Training│
│  (ingest)   │     │  (engineer)  │     │  (prepare)  │     │   (MLflow)    │
└─────────────┘     └──────────────┘     └─────────────┘     └───────────────┘
         ▲
         │  Orchestrated by Apache Airflow (Docker)
```

---

## Project Structure

```
.
├── config/
│   ├── config.yaml          # Model and dataset configuration
│   └── airflow.cfg          # Airflow settings
├── dags/
│   └── example_dag.py       # Airflow DAG defining the pipeline
├── data/
│   └── supply_chain.csv     # Raw pharmaceutical supply chain dataset
├── notebook/
│   └── Supply_Chain_Optimisation.ipynb  # Exploratory analysis and modelling
├── src/
│   ├── config.py            # YAML configuration loader
│   ├── import_data.py       # CSV data ingestion
│   ├── main.py              # CLI entry point
│   ├── process_data.py      # Data cleaning and feature engineering
│   └── split_data.py        # Train/test splitting with class balancing
├── docker-compose.yaml      # Docker Compose for Airflow services
├── requirements.txt         # Python dependencies
└── README.md
```

---

## Prerequisites

- Python 3.9+
- Docker and Docker Compose (for Airflow orchestration)
- An MLflow tracking server (or Databricks workspace) for experiment logging

---

## Installation

### Python environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Airflow (Docker)

```bash
# 1. Initialise the Airflow metadata database
docker compose up airflow-init

# 2. Start all Airflow services in the background
docker compose up -d
```

The Airflow web UI will be available at `http://localhost:8080`.

---

## Configuration

All pipeline parameters are stored in `config/config.yaml`:

```yaml
system:
  raw_dataset_path: "./data/supply_chain.csv"
  processed_dataset_path: "./data/supply_chain_processed.csv"
  model_save_path: "./models/model.pkl"

model:
  test_size: 0.2       # Fraction of data held out for testing
  random_state: 42

model_params:
  n_estimators: 100
  max_depth: 10
  random_state: 42
```

---

## Usage

### Running the Airflow Pipeline

Once the Docker services are running, trigger the `hello_world_dag` DAG from the Airflow UI or via the CLI:

```bash
docker compose exec airflow-scheduler airflow dags trigger hello_world_dag
```

The DAG executes the following tasks in order:

```
print_hello → fetch_data → preview_data → process_data → split_data → done
```

### Running the Notebook

Open the Jupyter notebook for interactive exploration and model training:

```bash
jupyter notebook notebook/Supply_Chain_Optimisation.ipynb
```

The notebook connects to MLflow for experiment tracking. Set the `MLFLOW_TRACKING_URI` environment variable before launching if you are using a remote tracking server.

### Running the Entry Point Directly

```bash
python src/main.py
```

---

## Data Pipeline

| Step | Module | Description |
|------|--------|-------------|
| **Ingest** | `src/import_data.py` | Reads `supply_chain.csv` (Latin-1 encoding) into a Pandas DataFrame |
| **Process** | `src/process_data.py` | Drops unused columns, parses date fields into year/month/day features, label-encodes categorical variables, handles missing values |
| **Split** | `src/split_data.py` | Stratified train/test split (default 80/20); optional upsampling to balance the minority class |
| **Train** | `notebook/` | Model training experiments tracked with MLflow; supports XGBoost, LightGBM, and scikit-learn estimators with Optuna hyperparameter optimisation |

Intermediate data files (X_train, X_test, y_train, y_test) are written to `/tmp/ml_splits/` during pipeline execution.

---

## Dataset

**File**: `data/supply_chain.csv`  
**Rows**: 10,325 pharmaceutical shipment records  
**Encoding**: Latin-1

Key column groups:

| Group | Columns |
|-------|---------|
| Identifiers | ID, Project Code, PQ #, PO / SO #, ASN/DN # |
| Geography | Country |
| Shipment | Shipment Mode, PQ First Sent to Client Date, PO Sent to Vendor Date, Scheduled Delivery Date, Delivered to Client Date |
| Product | Product Group, Molecule/Test Type, Brand, Dosage, Dosage Form, Manufacturing Site |
| Financials | Freight Cost (USD), Line Item Insurance (USD), Line Item Value, Pack Price, Unit Price |
| Logistics | Weight (Kilograms), Vendor, Fulfill Via |
| Target | First Line Designation (binary: on-time vs. late delivery) |

---

## Technologies

| Category | Tools |
|----------|-------|
| Data processing | Pandas, NumPy |
| Machine learning | scikit-learn, XGBoost, LightGBM |
| Hyperparameter tuning | Optuna, optuna-integration |
| Experiment tracking | MLflow |
| Visualisation | Matplotlib, Seaborn, Plotly |
| Pipeline orchestration | Apache Airflow 3 |
| Containerisation | Docker, Docker Compose |
| Code quality | Ruff |
| Testing | Pytest |

---

## Development

### Linting

```bash
ruff check .
ruff check . --fix   # auto-fix safe issues
```

### Tests

```bash
pytest
```

The CI/CD pipeline (`.github/workflows/run_test.yaml`) runs both Ruff and Pytest on every push.