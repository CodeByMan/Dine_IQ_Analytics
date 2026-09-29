# Included dataset package

The complete restaurant dataset is included in `source_dataset/`. This folder contains source CSVs, generated Parquet tables, samples, demand train/validation/test splits, dataset-generation and validation scripts, schemas and relationship definitions, statistics, the data dictionary, and hidden-data-readiness documentation. The application reads this data as immutable input; generated application outputs remain under `artifacts/` and `reports/`.

The default configuration discovers this packaged dataset. To use a separate copy, set `DINEIQ_DATA_DIR` to the dataset root (the directory containing its `data/` folder).
