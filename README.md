# Reproduce: DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED

When you query a foreign Iceberg table from Databricks (via Glue Catalog), you might see this error:

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Clone validation failed - Size and number
of data files in target table should match with source table.
```

This repo reproduces the error at 100 million rows, explains why it happens, and shows how to fix it in place.

## Why this happens

Every Iceberg snapshot has a summary with running totals like total file size and file count. These fields are optional in the Iceberg spec, and nothing enforces that writers keep them accurate. Databricks is the only engine that checks whether these totals are correct. If they're wrong, you get this error. Spark, Trino, Presto, and Athena all skip the summary and go straight to the manifests, which is why the table reads fine everywhere else.

Databricks engineering confirmed their validation is correct. The problem is on the Iceberg side.

Three things can make the summary wrong:

**1. `add_files` (Hive-to-Iceberg migration)**

When you migrate a Hive table to Iceberg using `add_files`, it records `total-files-size: 0` in the summary even though the files have real sizes. Bug in the procedure.

> Tested at 100M rows (1.5 GB, 50 files). Fixed in 73 seconds.

**2. Files in multiple locations + `add_files`**

Data files spread across different subdirectories get adopted via `add_files`. The summary only counts the original native writes. Adopted file sizes never get added to `total-files-size`. The gap grows with every adoption.

> Tested at 100M rows (883 MB actual, summary said 136 MB). Fixed in 100 seconds.

**3. Non-atomic writes to Iceberg**

When metadata is written separately from the data (common with streaming tables from engines like Snowflake), the summary fields can end up inaccurate. If the optional summary fields are populated but wrong, Databricks catches the mismatch. The native Glue Iceberg catalog handles commits atomically, so it doesn't have this problem on its own.

> Could not reproduce organically (Glue Iceberg catalog commits atomically). Simulated at 100M rows by corrupting the metadata JSON to mimic a non-atomic writer. Error triggered. Fixed by correcting the summary directly from manifest data. Note: `rewrite_data_files` does NOT fix this case because Iceberg computes summaries incrementally, so wrong values carry forward.

Full details and test results in `INVESTIGATION_WRITEUP.md`.

## How to fix it

Run Iceberg maintenance from Spark. The right fix depends on the cause.

**For Causes 1 and 2 (`add_files` related). Try Option A first:**

```sql
-- Option A: lightweight, rewrites manifests only
CALL catalog.system.rewrite_manifests('db.table');
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

```sql
-- Option B: if A doesn't work, rewrite the actual data files
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

`expire_snapshots` is required. Without it, the old broken snapshot sticks around and Databricks will still fail.

**For Cause 3 (non-atomic writers with fundamentally wrong summary):**

`rewrite_data_files` does not fix this. Iceberg computes summaries incrementally, so wrong values carry forward into the new snapshot. The fix is to correct the summary directly:

1. Read actual file count and total size from the manifests (via Spark)
2. Update the snapshot summary in the metadata JSON with correct values
3. Write the corrected metadata file to S3
4. Update the catalog pointer to the new metadata file

See `INVESTIGATION_WRITEUP.md` for a full walkthrough with before/after results.

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
