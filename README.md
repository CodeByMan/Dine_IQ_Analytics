<div align="center">

# 🍽️ DineIQ Analytics

### Restaurant Data Science and Operations Intelligence Platform

An end-to-end restaurant intelligence platform combining PySpark data engineering, Spark MLlib, an independent Python ML pipeline, persisted analytics, Streamlit dashboards, SQLite operations, model serving, and downloadable reports.

![Python](https://img.shields.io/badge/Python-3.13-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-4.1.3-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.64-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.9-F7931E?style=for-the-badge&logo=scikitlearn&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-003B57?style=for-the-badge&logo=sqlite&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)

</div>

---

## 📌 Project Overview

**DineIQ Analytics** transforms related restaurant datasets into operational intelligence for restaurant managers, analysts, regional managers, and administrators.

The platform provides:

- Spark-based ingestion, validation, cleaning, joins, feature engineering, and Parquet processing.
- Independent Spark MLlib and Python demand-model pipelines.
- Menu, customer, demand, wastage, pricing, promotion, rating, location, channel, and anomaly intelligence.
- A supervised wastage-risk model with persisted model loading and inference.
- Interactive Streamlit dashboards with filters, KPIs, charts, recommendations, scenarios, and CSV report downloads.
- SQLite-backed authentication, role permissions, management workflows, audit records, processing jobs, model metadata, and prediction results.
- An optional authenticated local HTTP integration API over the same domain services.

---

## ✨ Core Capabilities

### Data Engineering

- Twelve related restaurant data tables with primary and foreign-key relationships.
- CSV and Parquet ingestion with schema and value validation.
- Data-quality, duplicate, missing-value, invalid-value, and relationship checks.
- Documented cleaning decisions and reproducible processing outputs.
- Spark SQL aggregations, partition-aware Parquet storage, and read-back checks.
- Chronological train, validation, and test splits for demand modelling.

### Restaurant Intelligence

- Revenue, cost, contribution margin, profitability, and menu performance.
- Four menu classes: Profit Driver, Volume Driver, Hidden Opportunity, and Low Performer.
- Tricky menu cases with supporting analytical evidence.
- Customer segmentation and RFM analysis.
- Market-basket association rules with support, confidence, lift, and bundle recommendations.
- Demand forecasting for item, category, and location levels.
- Wastage trends, risk scoring, and supervised wastage prediction.
- Price sensitivity and promotion effectiveness analysis.
- Promotion trap detection.
- Rating and sales anomaly detection.
- Location comparison and location-specific menu intelligence.
- Ordering-channel analysis and customer churn-risk analysis.
- Evidence-linked menu, inventory, and customer recommendations.
- What-if scenario analysis for pricing, promotion, demand, and inventory assumptions.

### Machine Learning

- Spark MLlib demand models with validation-based model selection.
- Independent Python demand model using scikit-learn and the canonical chronological splits.
- Spark-versus-Python prediction comparison with agreement and difference metrics.
- Persisted model artifacts, model versions, evaluation metadata, and inference paths.
- Random Forest wastage-risk classifier using historical and contextual features.

### Dashboards and Reports

- Executive dashboard.
- Menu intelligence dashboard.
- Customer intelligence dashboard.
- Wastage dashboard.
- Forecast dashboard.
- Dual-pipeline comparison dashboard.
- Shared date, location, category, channel, customer, menu, promotion, rating, inventory, and scenario filters.
- Twelve downloadable CSV report categories with safe spreadsheet serialization.
  
<img width="1009" height="391" alt="image" src="https://github.com/user-attachments/assets/aa42d65c-8c9a-421e-bb81-3e2d90db13d5" />

---

## 🧱 System Architecture

<img width="1090" height="594" alt="image" src="https://github.com/user-attachments/assets/c89fd30e-36d4-4e05-8dd0-e52e44531dfb" />


Streamlit is the primary user interface. The optional local API and Streamlit use the same authentication, permission, persistence, analytics, model, and error-handling services.

---

## 🧰 Technology Stack

- Python 3.13
- Apache Spark 4.1.3, PySpark, and Spark SQL
- pandas, NumPy, PyArrow, and scikit-learn
- Streamlit
- SQLite
- pytest and Python unittest
- Java 17+ for Spark execution

---

## 📁 Repository Structure

```text
DineIQ_Analytics/
├── data/source_dataset/       # Canonical CSV, Parquet, splits, fixtures, docs
├── src/dineiq/
│   ├── analytics/             # Analytics, models, inference, recommendations
│   ├── api/                   # Optional authenticated local HTTP API
│   ├── dataset/               # Ingestion and Spark pipeline orchestration
│   ├── db/                    # SQLite schema, auth, RBAC, audit, persistence
│   ├── features/              # Reusable feature engineering
│   └── ui/                    # Streamlit app, dashboards, reports, components
├── artifacts/                 # Generated Parquet, model, and feature artifacts
├── reports/                   # Generated reports and execution evidence
├── docs/                      # Architecture, dataset, model, and SRS documents
├── scripts/                   # Dataset, parity, reproducibility, and release tools
├── tests/                     # Unit, integration, data, ML, API, and UI tests
├── requirements.txt
├── requirements-dev.txt
├── AI_USAGE.md
├── LICENSE
└── README.md
```

---

## ✅ Prerequisites

- Ubuntu/WSL or another Linux-compatible environment.
- Python 3.13.
- Java 17 or newer.
- Git.
- The packaged dataset under `data/source_dataset/`.

---

## 🚀 Installation and Setup

Run these commands from the project root:

```bash
source dineiq_analytics_env/bin/activate
python -m pip install -r requirements-dev.txt

export DINEIQ_DATA_DIR="$PWD/data/source_dataset"
export DINEIQ_ARTIFACTS_DIR="$PWD/artifacts"
export DINEIQ_REPORTS_DIR="$PWD/reports"
export PYTHONPATH="$PWD/src"
```

The environment variables keep the source dataset immutable and place generated artifacts, reports, model files, and operational output in their configured project directories.

---

## 🗃️ Dataset and Pipeline Commands

```bash
python -m dineiq.cli doctor
python -m dineiq.cli dataset-check
python -m dineiq.cli validate-data
python -m dineiq.cli build-data-foundation
python -m dineiq.cli build-descriptive-analytics
python -m dineiq.cli build-demand-planning --horizon-days 30
python -m dineiq.cli build-operational-intelligence
python -m dineiq.cli build-demand-models
```

The demand horizon accepts values from 1 to 366 days.

---

## 🖥️ Streamlit Application

Initialize the local database and create the first administrator:

```bash
python -m dineiq.cli init
python -m dineiq.cli create-admin
```

Launch the application:

```bash
streamlit run src/dineiq/ui/app.py
```

The application opens at the local Streamlit URL displayed by the command. Use the sidebar navigation to access dashboards, reports, management, predictions, scenarios, and operational views.

---

## 🔌 Optional Integration API

```bash
python -m dineiq.cli serve-api --host 127.0.0.1 --port 8502
```

Available workflows include:

- `GET /health` and `GET /api/v1/health`
- Authenticated report catalog access
- Authenticated processing-job status
- Permission-scoped entity CRUD
- Persisted demand prediction
- Persisted wastage-risk prediction

Health is public; operational routes require authentication and role permissions.

---

## 📊 Report Catalog

The application provides page-level CSV downloads for:

1. Menu performance
2. Profitability
3. Customer segmentation
4. Market-basket analysis
5. Demand forecast
6. Wastage
7. Promotions
8. Pricing
9. Location performance
10. Anomalies
11. Recommendations
12. Spark-versus-Python comparison

Reports are generated from the selected filters and use stable, meaningful filenames.

---

## 🔐 Data and Security

- Customer data is handled through anonymized identifiers and controlled access.
- Authentication uses salted password hashes and role-based permissions.
- SQLite foreign-key enforcement and scoped CRUD protect operational records.
- Audit records capture operational actions without storing secrets.
- Local environment files, credentials, databases, caches, and generated build files are excluded from version control.
- AI-assisted development disclosures are maintained in `AI_USAGE.md`.

<img width="1090" height="551" alt="image" src="https://github.com/user-attachments/assets/8790f9f4-3e5e-4775-a884-b558289ab40c" />

---

## 📚 Documentation

Project documentation is available under `docs/`, including:

- `PROJECT_ARCHITECTURE.md`
- `DATASET_REPORT.md`
- `SPARK_EVIDENCE.md`
- `PHASE_7E_MODEL_AUDIT.md`
- `PHASE_7F_WASTAGE_VERIFICATION.md`
- `PHASE_7G_DASHBOARD_TRACEABILITY.md`
- `PHASE_7H_REPORT_TRACEABILITY.md`
- `PHASE_7I_API_TRACEABILITY.md`
- `PHASE_7J_INTEGRATION_TRACEABILITY.md`
- `final_project_report.md`
- `final_submission_checklist.md`

## 📚 Blog URL
https://medium.com/@muhammadalinawaz.dev/engineering-dineiq-analytics-building-a-dual-pipeline-big-data-machine-learning-platform-for-a516a0fa70b7

---

## 📄 License

This project is released under the MIT License. See [LICENSE](LICENSE).

<div align="center">

### DineIQ Analytics — Restaurant Intelligence Through Data


</div>
