# 🚀 Scalable Data Engineering with Azure

A production-grade, end-to-end data engineering platform built on Microsoft Azure, featuring batch & streaming ingestion, distributed processing, orchestration, and observability.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                    │
│  REST APIs │ Databases │ Event Hubs │ Blob Storage │ On-Prem Files  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER                                    │
│        Azure Event Hubs │ Azure Data Factory │ ADF Pipelines        │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  PROCESSING LAYER                                   │
│     Azure Databricks (PySpark) │ Azure Stream Analytics │ dbt       │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                  STORAGE LAYER (Medallion Architecture)             │
│   Bronze (Raw) │ Silver (Cleaned) │ Gold (Curated) → Azure Synapse  │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION & MONITORING                             │
│         Apache Airflow │ Azure Monitor │ Custom Dashboards          │
└─────────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
azure-data-engineering/
├── src/
│   ├── ingestion/           # Data ingestion modules
│   ├── processing/          # Spark & stream processing
│   ├── storage/             # ADLS Gen2, Delta Lake ops
│   ├── orchestration/       # Airflow DAGs
│   ├── monitoring/          # Logging, metrics, alerts
│   └── utils/               # Shared utilities
├── config/                  # Environment configurations
├── infrastructure/          # Terraform / Bicep IaC
├── tests/                   # Unit & integration tests
├── scripts/                 # Deployment & setup scripts
└── docs/                    # Architecture & API docs
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Event Hubs, ADF, REST APIs |
| Processing | Azure Databricks, PySpark, Delta Lake |
| Storage | ADLS Gen2, Azure Synapse Analytics |
| Orchestration | Apache Airflow (on AKS) |
| Secrets | Azure Key Vault |
| Monitoring | Azure Monitor, Log Analytics |
| IaC | Terraform |
| CI/CD | GitHub Actions |

## Quick Start

```bash
# 1. Clone & setup
git clone https://github.com/your-org/azure-data-engineering
cd azure-data-engineering
pip install -r requirements.txt

# 2. Configure environment
cp config/config.example.yaml config/config.yaml
# Edit config.yaml with your Azure credentials

# 3. Deploy infrastructure
cd infrastructure && terraform init && terraform apply

# 4. Run ingestion pipeline
python -m src.ingestion.batch_ingestor --source api --target bronze

# 5. Run Spark processing
python -m src.processing.bronze_to_silver --date 2026-05-04
```

## License
MIT
