# DineIQ Execution Guide

Activate the environment and export the variables from `INSTALLATION.md` before
running these commands from the project root.

## Build and validate the data foundation

```bash
python -m dineiq.cli dataset-check
python -m dineiq.cli validate-data
python -m dineiq.cli build-data-foundation
```

This validates raw inputs, applies documented cleaning rules, creates processed
Parquet tables, and writes data-foundation reports.

## Build required analytics

```bash
python -m dineiq.cli build-descriptive-analytics
python -m dineiq.cli build-demand-planning --horizon-days 30
python -m dineiq.cli build-operational-intelligence
python -m dineiq.cli build-demand-models
```

`--horizon-days` accepts 1 through 366. The commands produce the SRS-required
menu classification, customer segmentation, association rules, forecasts,
wastage and pricing analysis, promotion analysis, anomaly detection,
recommendations, and the independent Spark/Python demand-model comparison.

## Start the dashboard

```bash
streamlit run src/dineiq/ui/app.py --server.address 127.0.0.1 --server.port 8501
```

Open `http://127.0.0.1:8501`, sign in, and use an account whose role and location
scope match the requested workflow. Administrators have system-wide management;
regional managers and managers are scope-limited; analysts are read-only.

## Start the optional API

```bash
python -m dineiq.cli serve-api --host 127.0.0.1 --port 8502
```

Bind to a non-loopback interface only in a secured deployment with authentication,
TLS termination, firewall rules, and protected secrets.

## Validate outputs

```bash
python -m unittest discover -s tests -p 'test_*.py' -q
```

Review `reports/validation_report.json` first, followed by module build summaries
under `reports/data_foundation/`, `reports/descriptive_analytics/`,
`reports/demand_planning/`, `reports/operational_intelligence/`, and
`reports/demand_models/`. A pipeline is successful only when its command exits
successfully and its newly generated report identifies the run as complete.

## Recommended execution order for a new dataset

1. Replace only the source files in `DINEIQ_DATA_DIR`.
2. Run dataset check and validation.
3. Build the data foundation.
4. Build descriptive analytics and demand planning.
5. Build operational intelligence and demand models.
6. Run the test suite and inspect the generated reports.
7. Start the dashboard or API for authorized users.
