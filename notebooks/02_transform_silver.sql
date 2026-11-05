-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 - Bronze to Silver
-- MAGIC Filters invalid rows, casts types, removes duplicates.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS main.banking.silver_transactions (
  transaction_id  STRING        NOT NULL,
  customer_id     BIGINT        NOT NULL,
  amount          DECIMAL(18,2) NOT NULL,
  currency        STRING,
  txn_type        STRING,
  txn_timestamp   TIMESTAMP     NOT NULL,
  txn_date        DATE,
  _ingest_ts      TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- Idempotent load: MERGE on transaction_id, keep latest ingested record
MERGE INTO main.banking.silver_transactions AS t
USING (
  SELECT *
  FROM (
    SELECT
      CAST(transaction_id AS STRING)         AS transaction_id,
      CAST(customer_id    AS BIGINT)         AS customer_id,
      CAST(amount         AS DECIMAL(18,2))  AS amount,
      UPPER(TRIM(currency))                  AS currency,
      UPPER(TRIM(txn_type))                  AS txn_type,
      CAST(txn_timestamp  AS TIMESTAMP)      AS txn_timestamp,
      CAST(txn_timestamp  AS DATE)           AS txn_date,
      _ingest_ts,
      ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY _ingest_ts DESC
      ) AS rn
    FROM main.banking.bronze_transactions
    WHERE transaction_id IS NOT NULL
      AND customer_id    IS NOT NULL
      AND amount         IS NOT NULL
      AND txn_timestamp  IS NOT NULL
      AND _rescued_data  IS NULL
  )
  WHERE rn = 1
) AS s
ON t.transaction_id = s.transaction_id
WHEN MATCHED THEN UPDATE SET
  t.customer_id   = s.customer_id,
  t.amount        = s.amount,
  t.currency      = s.currency,
  t.txn_type      = s.txn_type,
  t.txn_timestamp = s.txn_timestamp,
  t.txn_date      = s.txn_date,
  t._ingest_ts    = s._ingest_ts
WHEN NOT MATCHED THEN INSERT (
  transaction_id, customer_id, amount, currency, txn_type, txn_timestamp, txn_date, _ingest_ts
) VALUES (
  s.transaction_id, s.customer_id, s.amount, s.currency, s.txn_type, s.txn_timestamp, s.txn_date, s._ingest_ts
);

-- COMMAND ----------

-- Sanity check
SELECT COUNT(*) AS silver_rows FROM main.banking.silver_transactions;

