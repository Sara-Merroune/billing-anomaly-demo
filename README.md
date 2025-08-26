# Billing Anomaly Demo

## Overview
This repo contains an end-to-end demo pipeline that:
1. Generates synthetic billing data (`data/billing.csv`).
2. Loads and cleans data into MySQL (`billing_clean`), with incremental logic and audit (`etl_control`).
3. Scores invoices for anomalies using a 3-month rolling mean + 2×stddev rule, writes anomalies to `data/anomalies.csv` and `anomalies` table.
4. Produces a Power BI report `reports/BillingAnomalies.pbix`.

## Files of interest
- `jobs/load_incremental.sql` — SQL that loads CSV → staging, computes row_hash, upserts into `billing_clean`, writes `etl_control`.
- `scripts/export_anomalies.py` — scores and writes `data/anomalies.csv`.
- `scripts/generate_billing.py` — generates synthetic `data/billing.csv`.
- `reports/BillingAnomalies.pbix` — Power BI report to present anomalies.

## Demo checklist
- Run billing_job.kjb
- Show `SELECT * FROM etl_control ORDER BY id DESC LIMIT 1;`.
- Open `reports/BillingAnomalies.pbix` → Refresh → walkthrough visuals.