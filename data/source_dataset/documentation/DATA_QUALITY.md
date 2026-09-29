# Data-quality coverage

The SRS text labels the list as 14 types but enumerates 15. This package retains all 15 listed checks and records the discrepancy in `fixtures/data_quality/manifest.json`. Canonical source data contains natural missingness, cancellations, and flagged anomalies. Duplicate keys and malformed/invalid reference cases are kept in isolated fixtures so the analysis tables retain valid primary keys. Run `python scripts/validate_srs_compliance.py` for the quality-fixture detection result.
