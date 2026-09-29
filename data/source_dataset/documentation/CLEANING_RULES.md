# Cleaning rules and outputs

`data/*.csv` are immutable source tables for this remediation. `samples/cleaned/*_clean_sample.csv` are small clean views. Exact rule, source, detection, action, counts, correction/removal, anomaly retention, and reason are in `CLEANING_DECISIONS.csv`. No raw source rows were corrected or removed. Clean views exclude only flagged anomaly/duplicate rows, invalid quantities/ratings, noncompleted order lines, and repeated primary keys as applicable.
