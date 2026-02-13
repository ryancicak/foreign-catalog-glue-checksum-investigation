# Reproduce: DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED

When you query a foreign Iceberg table from Databricks (via Glue Catalog), you might see this error:

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Clone validation failed - Size and number
of data files in target table should match with source table.
```

This repo reproduces the error at 100 million rows, explains why it happens, and shows how to fix it in place.

## Why this happens

Every Iceberg snapshot has a summary with running totals like total file size and file count. Databricks is the only engine that actually checks whether these totals are correct. If they're wrong, you get this error. Spark, Trino, Presto, and Athena all ignore the summary and read the table fine.

Databricks engineering confirmed their validation is correct. The problem is on the Iceberg side.

Three things can make the summary wrong:

**1. `add_files` (Hive-to-Iceberg migration)**

When you migrate a Hive table to Iceberg using `add_files`, it records `total-files-size: 0` in the summary even though the files have real sizes. Bug in the procedure.

> Tested at 100M rows (1.5 GB, 50 files). Fixed in 73 seconds.

**2. Streaming metadata drift**

Multiple applications writing to the same table through an HMS-based catalog that can't keep up. The summary falls behind because the metastore handles updates asynchronously. This only happens in production under specific conditions. You can't reproduce it on demand. The native Glue Iceberg catalog handles commits atomically, so it doesn't have this problem.

> Tested at 100M rows (101 rapid commits). Summary stayed accurate. Could not reproduce.

**3. Files in multiple locations + `add_files`**

Data files spread across different subdirectories get adopted via `add_files`. The summary only counts the original native writes. Adopted file sizes never get added to `total-files-size`. The gap grows with every adoption.

> Tested at 100M rows (883 MB actual, summary said 136 MB). Fixed in 100 seconds.

Full details and test results in `INVESTIGATION_WRITEUP.md`.

## How to fix it

Run Iceberg maintenance from Spark. Two options:

**Option A (lightweight, try first):**

```sql
CALL catalog.system.rewrite_manifests('db.table');
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

**Option B (guaranteed fix, if A doesn't work):**

```sql
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

`expire_snapshots` is required. Without it, the old broken snapshot sticks around and Databricks will still fail.

## Quick start

```bash
pip install -r requirements.txt
cp .env.example .env        # fill in your EMR and Databricks details
bash scripts/reproduce.sh    # create the broken table on EMR
python scripts/query_from_databricks.py   # trigger the error
python scripts/cleanup.py    # fix it and verify
```

You need an EMR cluster with Spark/Iceberg and a Databricks workspace with a foreign catalog pointing at Glue. See `.env.example` for all the config fields.

## Files

| File | What it does |
|------|-------------|
| `scripts/reproduce.sh` | Creates the broken table on EMR |
| `scripts/setup_foreign_catalog.py` | Sets up the Databricks foreign catalog and Lake Formation permissions |
| `scripts/query_from_databricks.py` | Queries from Databricks to trigger the error |
| `scripts/cleanup.py` | Runs the full fix and verifies from Databricks |
