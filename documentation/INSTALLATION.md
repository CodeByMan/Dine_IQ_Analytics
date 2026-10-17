# DineIQ Installation Guide

## Prerequisites

- Ubuntu Linux or Windows Subsystem for Linux (WSL)
- Python 3.13
- Java 17 or later for Apache Spark
- Git
- At least 8 GB RAM and sufficient disk space for the CSV and Parquet datasets

## Environment setup

From the project root:

```bash
python3 -m venv dineiq_analytics_env
source dineiq_analytics_env/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

Configure the data, output, source, and Java locations:

```bash
export DINEIQ_DATA_DIR="$PWD/data/source_dataset"
export DINEIQ_ARTIFACTS_DIR="$PWD/artifacts"
export DINEIQ_REPORTS_DIR="$PWD/reports"
export PYTHONPATH="$PWD/src"
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"
```

If Java is installed elsewhere, set `JAVA_HOME` to that Java 17+ installation.
Keep raw CSV files under `DINEIQ_DATA_DIR`; generated Parquet, model, and report
files belong in their configured output directories.

## Verify the installation

```bash
python -m dineiq.cli doctor
python -m dineiq.cli dataset-check
python -m dineiq.cli validate-data
```

Successful verification confirms that Python dependencies, Java/Spark, required
dataset files, and the configured output directories are accessible. Resolve any
reported missing file, schema, permission, or Java error before running pipelines.

## Initialize application storage

```bash
python -m dineiq.cli init
python -m dineiq.cli create-admin
```

Use a strong administrator password. Do not commit credentials, database files,
raw personal fields, or generated secrets to version control.

## Run tests

```bash
python -m unittest discover -s tests -p 'test_*.py' -q
```

The supplied tests cover the SRS data, analytics, model, dashboard, API,
operations, reporting, and security contracts.
