
USE billing_db;
DROP TABLE IF EXISTS billing_stage;
CREATE TABLE billing_stage LIKE billing_raw;
TRUNCATE TABLE billing_stage;

LOAD DATA LOCAL INFILE 'C:/full/path/to/your/file/data/billing.csv'
INTO TABLE billing_stage
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(invoice_id,customer_id,@billing_date,@amount,plan_type,is_processed,@last_updated_date,row_checksum)
SET
  billing_date = CASE WHEN @billing_date = 'MALFORMED_DATE' THEN NULL ELSE STR_TO_DATE(@billing_date, '%Y-%m-%d %H:%i:%s') END,
  amount = NULLIF(@amount, ''),
  last_updated_date = NULLIF(@last_updated_date, '');
-- rest of staging compute, hash update, upsert into billing_clean, etl_control insert as posted earlier
