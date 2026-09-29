# Pakistani Restaurant Operations Dataset

This package contains a reproducible synthetic dataset for the restaurant analytics project. All monetary values are synthetic PKR. Customer names and example contact addresses are fictional; contact addresses use the reserved `.invalid` domain.

## Dataset contents

`data/` contains the 11 required related tables—Customers, Orders, Order_Items, Menu_Items, Menu_Categories, Restaurants, Pricing_History, Promotions, Ratings, Inventory, and Wastage—plus the referenced `ordering_channels` lookup. The canonical source tables retain flagged anomaly and missing-value cases; duplicate-key test cases are isolated under `fixtures/` so canonical primary keys remain unique.

The package includes raw and cleaned sample files, documented cleaning decisions, chronological demand train/validation/test splits, isolated data-quality and hidden-evaluation cases, complete table schemas and key relationships, raw-table Parquet outputs, and a processed transaction-terms dataset. `create_parquet.py` writes Parquet for all raw tables and every processed CSV, including the large transaction-terms dataset. `documentation/REALISM_COVERAGE.csv` records evidence for each of the 20 required realism elements. In this remediation archive the Parquet files are carried forward from the prior package and have not yet been synchronized with the changed CSVs; follow `documentation/BUILD_STATUS.txt` before using the ZIP as a final submission.

## Rebuild the complete dataset

Python 3.10 or later is recommended. Install the declared packages; `pyarrow` is required for Parquet output and read-back. Java and `pyspark` are needed only for the Spark join pipeline.

```bash
python -m pip install -r requirements.txt
python scripts/build_submission.py
```

The build uses seed 42 by default. `generate_dataset.py` creates the synthetic source tables; `complete_srs_requirements.py` applies effective historical pricing and eligible campaign terms, adds the price-sensitivity cohort and wastage denominator, and creates isolated quality fixtures; the remaining scripts build samples, documentation, processed terms, chronological splits, and Parquet artifacts. Build output is reproducible for the same seed and dependency versions.

To run the concise dataset verification separately:

```bash
python scripts/validate_dataset_gate.py
```

## Scale and time coverage

The required minimums are at least 1,000,000 order lines, 100,000 unique orders, 50,000 customers, 150 menu items, 10 categories, 20 locations, 12 months of transactions, 100,000 ratings, and 50,000 wastage records. See `documentation/VALIDATION_REPORT.txt` for the actual counts and date coverage produced by the current build.

## Data-quality and analysis notes

Raw source anomalies are retained and flagged for quality assessment. Cleaning rules and counts are in `documentation/CLEANING_RULES.md` and `documentation/CLEANING_DECISIONS.csv`; the cleaned files are explicitly samples, not replacements for raw tables. All 15 problem types listed in the dataset requirements have isolated test fixtures, preserving the source-table key integrity. The source requirement labels that list “14 types” although it enumerates 15; the discrepancy is recorded in `documentation/DATA_QUALITY.md`.

Historical prices are effective-dated by item and location. Campaign discounts are applied only when dates, basket minimum, location, category, and delivery conditions qualify. The documented synthetic price-sensitive cohort creates a controlled historical demand response for later analysis; no elasticity model or final sensitivity analysis is performed in this dataset package. Wastage includes preparation quantity and wastage percentage. See the specific rules under `documentation/`.

Forecast splits use chronological date windows: earliest dates for training, then validation, then testing. No future date is included in an earlier split. Split boundaries and row counts are recorded in `splits/demand_forecast/split_metadata.json`.

## Spark and Python inputs

Both pipelines use the same generated table records. The Spark pipeline reads the corresponding Parquet tables when available and can check each required cross-table join. Run it with:

```bash
python scripts/spark_pipeline.py
```

The Python validator and Spark pipeline use the table/key contract in `documentation/RELATIONSHIPS.md`. The Spark command requires Java and PySpark.
