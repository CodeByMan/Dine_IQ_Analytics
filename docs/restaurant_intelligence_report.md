# DineIQ Restaurant Intelligence Report

**Evidence source:** generated build summaries and analytical artifacts included with this release.  
**Dataset:** synthetic restaurant operations data; see `data/source_dataset/documentation/DATA_GENERATION.md`.  
**Reporting window:** the generated dataset spans 24 months ending 2026-09-24.  
**Interpretation:** forecasts and recommendations are decision-support estimates, not causal guarantees or live POS data.

## Executive summary

The integrated analytics pipeline processed 12 related tables, including 1,136,583 cleaned order lines, 124,945 orders, 62,000 customers, 210 menu items, and 27 restaurant locations. It produced menu/customer classifications, daily demand forecasts, wastage and promotion analyses, location/channel comparisons, anomaly flags, evidence-linked recommendations, and two independently trained demand models.

The results support operational review of low-performing dishes, price-sensitive items, promotion effects, inventory planning, and customers at risk. The available build summaries do not contain a complete financial P&L; use the generated Parquet report outputs for exact item/location figures and apply the dashboard filters for follow-up.

## Data and preparation

The data foundation report records 12 cleaned tables and successful Parquet read-back. It also records three feature outputs: 48,974 customer feature rows, 210 menu feature rows, and 5,670 item/location feature rows. The original input dataset is included in `data/source_dataset/` and remains unchanged by the analytics pipeline.

The underlying synthetic data includes canceled transactions, historical prices, promotions, channel and location variation, customer lifecycle patterns, ratings, inventory, and wastage. Data-quality checks and isolated fixtures are included in the dataset package.

## Menu performance

All 210 menu items received one of the four required labels:

| Classification | Items | Share |
| --- | ---: | ---: |
| Profit Driver | 57 | 27.1% |
| Volume Driver | 39 | 18.6% |
| Hidden Opportunity | 12 | 5.7% |
| Low Performer | 102 | 48.6% |

The large Low Performer group is a screening signal for item-level review, not an automatic instruction to remove items. Check margin, demand, ratings, wastage, location variation, and tricky-case flags together. Detail is in `artifacts/descriptive_analytics/menu_profitability_classification/` and `artifacts/descriptive_analytics/tricky_menu_cases/`.

## Customer segments

All 62,000 customer rows were segmented:

| Segment | Customers | Share |
| --- | ---: | ---: |
| Occasional | 33,981 | 54.8% |
| High-Value Loyal | 18,426 | 29.7% |
| At-Risk | 4,475 | 7.2% |
| Frequent | 3,297 | 5.3% |
| New | 1,477 | 2.4% |
| Promotion-Driven | 344 | 0.6% |

The At-Risk segment is a retention-review population; the output does not prove customers will churn. Segment and RFM details are in `artifacts/descriptive_analytics/customer_segments/` and `artifacts/descriptive_analytics/customer_rfm/`.

## Demand and model evaluation

The demand-planning build created a configurable 30-day forecast with chronological train, validation, and test partitions. It generated 170,100 item/location/day forecast rows and evaluation families for MAE, RMSE, nonzero-actual MAPE, and R².

Three Spark MLlib candidates were trained and selected using validation data. The selected Spark model was Linear Regression; the independent Python model was SGDRegressor. On the same 629,370 held-out records:

| Model | MAE | RMSE | R² | RMSE improvement over seasonal naive |
| --- | ---: | ---: | ---: | ---: |
| Spark Linear Regression | 0.7830 | 1.0881 | 0.1000 | 28.05% |
| Python SGDRegressor | 0.6340 | 1.1260 | 0.0361 | 25.54% |
| Seasonal-naive baseline | 0.8339 | 1.5123 | -0.7388 | — |

The two model predictions agreed within the documented tolerance on 4.31% of held-out rows. This low agreement is disclosed for evaluator review; both models improved RMSE over the seasonal-naive baseline, but their predictions should not be treated as interchangeable. Full comparison records, actual demand, differences, match flags, and explanations are stored in `artifacts/demand_models/dual_pipeline_comparison/`.

## Operations and recommendations

The operational-intelligence build compared 27 locations, produced 8 channel summaries, scored all 62,000 customers for churn risk, and generated evidence-linked recommendations from all nine required families. All five rating-anomaly families were observed. Four of the six supported sales-anomaly families were observed in this generated dataset; the remaining two are covered by detector tests and fixtures rather than naturally occurring in the full data.

Promotion trap analysis evaluates all five named risk patterns. Wastage outputs include item, location, reason/shift, monthly trends, and risk estimates. Review the detailed output tables before changing menu prices, promotions, staffing, or stock levels.

## Limitations and responsible use

- The dataset is synthetic and is not evidence of actual restaurant performance.
- Forecasts depend on the generated historical patterns and should be recalibrated against real operational data before business use.
- Promotion comparisons are observational; they do not establish causal lift.
- Model disagreement is substantial under the documented agreement tolerance and should be investigated before operational automation.
- Customer IDs are pseudonymous; restrict database access and avoid exporting direct identifiers.

## Reproducible evidence

Build summaries are retained under `reports/` by data-product name. The complete row-level Parquet outputs and saved models are under `artifacts/`. Rebuild instructions and test commands are in the project `README.md`; requirement-level mapping is in `docs/*_traceability.csv`.
