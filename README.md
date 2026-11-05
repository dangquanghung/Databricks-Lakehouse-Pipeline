# Databricks-Lakehouse-Pipeline

Incremental ingestion and transformation pipeline on Databricks using the Medallion architecture. Source data is mock banking transaction events (JSON).

## Architecture

```
Landing (JSON files in cloud storage)
        │  Auto Loader (cloudFiles)
        ▼
Bronze  – raw data as-is, plus ingestion metadata, _rescued_data kept
        │  Spark SQL: filter, cast, deduplicate
        ▼
Silver  – cleaned, typed, deduplicated Delta tables
        │  Aggregations / business logic
        ▼
Gold    – aggregated tables for reporting (daily volume per customer)
```

| Layer | Purpose | Notes |
|---|---|---|
| Bronze | Raw history of what arrived | Append-only, schema evolution via `addNewColumns`, bad fields go to `_rescued_data` |
| Silver | Clean, typed data | Null filtering, type casting, dedup on `transaction_id` |
| Gold | Consumption layer | Aggregates for BI |

## Key design points

- **Auto Loader (`cloudFiles`)**: discovers new files incrementally, tracks state in the checkpoint, no manual file listing.
- **Schema location**: inferred schema is persisted, so schema evolution is controlled.
- **Rescued data column**: records that do not match the schema are kept, not dropped.
- **Delta Lake**: ACID writes, time travel, `MERGE` support for idempotent reruns.
- **`availableNow` trigger**: runs as an incremental batch job, cheap to schedule with Databricks Workflows.

## Folder structure

```
Databricks-Lakehouse-Pipeline/
├── README.md
├── data/
│   └── sample/
│       └── transactions_sample.jsonl
├── notebooks/
│   ├── 01_ingestion_autoloader.py
│   ├── 02_transform_silver.sql
│   └── 03_aggregate_gold.sql
├── config/
│   └── pipeline_config.yaml
└── docs/
    └── architecture.md
```

## How to run

1. Import the `notebooks/` folder into a Databricks workspace (Repos).
2. Edit paths in `01_ingestion_autoloader.py` to your storage location / Unity Catalog volume.
3. Upload files from `data/sample/` into the landing path.
4. Run notebooks in order: 01 → 02 → 03 (or chain them in a Workflows job).

## Data

Mock data only. Fields: `transaction_id`, `customer_id`, `amount`, `currency`, `txn_type`, `txn_timestamp`.

