-- scoring_select.sql
USE billing_db;

SELECT
  b.invoice_id,
  b.customer_id,
  b.billing_date,
  b.amount,
  b.plan_type,
  -- rolling stats over prior 3 months 
  (SELECT AVG(x.amount)
     FROM billing_clean x
     WHERE x.customer_id = b.customer_id
       AND x.billing_date >= DATE_SUB(b.billing_date, INTERVAL 3 MONTH)
       AND x.billing_date < b.billing_date
       AND x.amount IS NOT NULL
  ) AS rolling_mean,
  (SELECT STDDEV_SAMP(x.amount)
     FROM billing_clean x
     WHERE x.customer_id = b.customer_id
       AND x.billing_date >= DATE_SUB(b.billing_date, INTERVAL 3 MONTH)
       AND x.billing_date < b.billing_date
       AND x.amount IS NOT NULL
  ) AS rolling_stddev,
  -- determine anomaly (1) only if rolling_mean and rolling_stddev are not null and stddev>0
  CASE
    WHEN
      (SELECT COUNT(1) FROM billing_clean x
        WHERE x.customer_id = b.customer_id
          AND x.billing_date >= DATE_SUB(b.billing_date, INTERVAL 3 MONTH)
          AND x.billing_date < b.billing_date
          AND x.amount IS NOT NULL) < 2
    THEN 0
    WHEN b.amount > ( (SELECT AVG(x.amount) FROM billing_clean x
                        WHERE x.customer_id = b.customer_id
                          AND x.billing_date >= DATE_SUB(b.billing_date, INTERVAL 3 MONTH)
                          AND x.billing_date < b.billing_date
                          AND x.amount IS NOT NULL)
                      + 2 * (SELECT STDDEV_SAMP(x.amount) FROM billing_clean x
                        WHERE x.customer_id = b.customer_id
                          AND x.billing_date >= DATE_SUB(b.billing_date, INTERVAL 3 MONTH)
                          AND x.billing_date < b.billing_date
                          AND x.amount IS NOT NULL)
                    )
    THEN 1
    ELSE 0
  END AS is_anomaly
FROM billing_clean b
WHERE b.is_processed = 0
ORDER BY b.billing_date DESC;
