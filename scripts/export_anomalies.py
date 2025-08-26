#!/usr/bin/env python3
"""
export_anomalies.py
Arguments : --host, --user, --password, --database, --out
"""

import argparse
import csv
import mysql.connector
from mysql.connector import errorcode

parser = argparse.ArgumentParser()
parser.add_argument("--host", default="localhost")
parser.add_argument("--user", required=True)
parser.add_argument("--password", required=True)
parser.add_argument("--database", required=True)
parser.add_argument("--out", default="anomalies.csv")
args = parser.parse_args()

SCORING_SQL = """
/* Scoring query: per-invoice compute 3-month rolling mean & stddev for that customer using invoices prior to this invoice.
   Then flag anomaly when invoice.amount > mean + 2*stddev.
   We require at least 2 prior invoices to compute stddev; otherwise not flagged.
*/
SELECT
  b.invoice_id,
  b.customer_id,
  b.billing_date,
  b.amount,
  b.plan_type,
  rs.rolling_mean,
  rs.rolling_stddev,
  CASE WHEN rs.rolling_mean IS NULL THEN 0
       WHEN rs.rolling_stddev IS NULL THEN 0
       WHEN b.amount > rs.rolling_mean + 2 * rs.rolling_stddev THEN 1
       ELSE 0 END AS is_anomaly
FROM billing_clean b
LEFT JOIN (
  SELECT
    cur.invoice_id,
    cur.customer_id,
    -- compute averages over 3 months prior (excluding current invoice)
    (SELECT AVG(x.amount) FROM billing_clean x
       WHERE x.customer_id = cur.customer_id
         AND x.billing_date >= DATE_SUB(cur.billing_date, INTERVAL 3 MONTH)
         AND x.billing_date < cur.billing_date
    ) AS rolling_mean,
    (SELECT STDDEV_SAMP(x.amount) FROM billing_clean x
       WHERE x.customer_id = cur.customer_id
         AND x.billing_date >= DATE_SUB(cur.billing_date, INTERVAL 3 MONTH)
         AND x.billing_date < cur.billing_date
    ) AS rolling_stddev
  FROM billing_clean cur
) rs ON rs.invoice_id = b.invoice_id
WHERE
  -- filter only the anomalous ones (outer WHERE); change to remove WHERE to export all scored rows
  (rs.rolling_mean IS NOT NULL AND rs.rolling_stddev IS NOT NULL
   AND b.amount > rs.rolling_mean + 2 * rs.rolling_stddev)
ORDER BY b.billing_date DESC
LIMIT 10000;
"""

def main():
    try:
        conn = mysql.connector.connect(
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database,
            autocommit=True,
            use_pure=True
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("ERROR: Access denied - check username/password")
        else:
            print("ERROR: Could not connect:", err)
        return 2

    cur = conn.cursor(dictionary=True)
    cur.execute(SCORING_SQL)
    rows = cur.fetchall()
    if not rows:
        print("No anomalies found (query returned 0 rows).")
    else:
        with open(args.out, "w", newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} anomalies to {args.out}")

    # summary counts
    cur.execute("SELECT COUNT(*) FROM billing_clean WHERE amount >= 500;")
    outlier_count = cur.fetchone()[0]
    print("Injected outliers in data (amount>=500):", outlier_count)
    cur.close()
    conn.close()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
