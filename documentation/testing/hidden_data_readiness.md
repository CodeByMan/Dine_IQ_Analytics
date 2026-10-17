# Hidden-Data Readiness

The system is designed to accept an unseen dataset that follows the SRS contract.
The evaluator's actual hidden dataset is not available in this package, so this
document records implemented safeguards and the acceptance checks to run when it
is supplied; it does not claim that unseen evaluator data has already passed.

## Implemented safeguards

- Explicit schemas and required-header checks for all source tables.
- Type, null, range, primary-key, foreign-key, and temporal validation.
- Clear failure on missing required fields instead of guessed column mappings.
- Effective-dated price validation and promotion-window checks.
- Duplicate and orphan detection with documented cleaning decisions.
- Chronological train/test splits and past-only demand features to prevent leakage.
- Rejection of invalid or non-finite model features.
- Unknown-category handling for Spark ML pipelines.
- Safe handling of empty inputs and insufficient history.
- Independent Spark MLlib and Python model outputs with disagreement reporting.
- Fresh-output guards so a failed run cannot be mistaken for a successful one.

## Required hidden-data acceptance cases

| Case | Expected behavior |
|---|---|
| Missing values | Validate, report, and apply only documented cleaning rules. |
| Duplicate records | Detect by declared keys; prevent duplicate analytical facts. |
| Unknown menu items or customers | Report referential violations without silent remapping. |
| New restaurant locations | Preserve valid identifiers and include them in location analytics. |
| Price changes | Select the price effective on the transaction date. |
| Unusual promotions | Validate scope and active dates before attribution. |
| Extreme wastage | Flag the outlier and keep model inputs finite. |
| Seasonal demand change | Preserve chronological order and recompute time features. |
| Unexpected categories | Use supported unknown-category behavior or fail clearly. |
| Empty or short history | Return a clear insufficient-data result, not a fabricated forecast. |
| Model disagreement | Retain both predictions and report the comparison transparently. |

Run the SRS test suite and regenerate `reports/validation_report.json` after
placing hidden inputs in the configured data directory. Acceptance requires no
unhandled exception, no schema guessing, no data leakage, and complete validation
evidence for the supplied files.
