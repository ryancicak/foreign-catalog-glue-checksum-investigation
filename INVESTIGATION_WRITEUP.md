# DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED

## What to tell the customer

The Iceberg table's snapshot summary has wrong numbers in it. Databricks checks these numbers, sees they don't match the actual data, and throws this error. No other engine checks, which is why the table reads fine everywhere else.

Not data corruption. Not data loss. The data is fine. Just the summary metadata is wrong.

Tell them to run this from Spark:

```sql
-- Try this first (lightweight, doesn't rewrite data)
CALL catalog.system.rewrite_manifests('db.table');
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

If the error persists:

```sql
-- Guaranteed fix (rewrites all data files)
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

Don't skip `expire_snapshots`. It removes the old snapshot with the bad numbers. Without it, Databricks still sees the broken one.

If the error comes back after writes resume, their write pipeline or catalog setup is the root cause and needs to be addressed.

---

## The error

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Failed to convert the table version 0 
to the universal format iceberg. Clone validation failed - Size and number of data 
files in target table should match with source table. 
srcTableSize: 0, targetTableSize: 2767 srcTableNumFiles: 2, targetTableNumFiles: 2
```

Reading this:

- `srcTableSize` = what the Iceberg snapshot summary says the total file size is
- `targetTableSize` = what Databricks actually computed by reading the metadata

Databricks' numbers are right. The summary is wrong.

---

## How Databricks reads foreign Iceberg tables

When you run `SELECT * FROM glue_catalog.schema.table`, Databricks:

1. Clones the Iceberg table into an internal Delta table
2. Before returning results, checks that the snapshot summary matches what it computed
3. If they don't match, throws `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED`

Every Iceberg snapshot has a summary. It's a small set of key-value pairs with running totals like `total-files-size` (sum of all data file sizes) and `total-data-files` (number of data files). These totals are supposed to be updated every time a commit happens. Databricks reads them and compares against what it finds by actually walking the manifests.

Databricks engineering confirmed this logic is correct.

---

## Three ways the summary goes wrong

### 1. `add_files` (Hive-to-Iceberg migration)

Customer migrates from Hive to Iceberg using `add_files`. The procedure adopts the existing parquet files but sets `total-files-size: 0` in the summary. Bug in the procedure.

### 2. Streaming metadata drift

Multiple applications writing to the same table at the same time through an HMS-based catalog that handles summary updates asynchronously. The catalog can't keep up. The summary falls behind. This only happens in production under specific conditions with the right combination of catalog, streaming framework, and concurrency. You can't reproduce it on demand. The native Glue Iceberg catalog handles commits atomically, so it doesn't have this problem.

### 3. Files in multiple locations + `add_files`

Data files in different subdirectories get adopted via `add_files`. The summary only counts native writes. Adopted file sizes never get added to `total-files-size`. The gap grows with every adoption.

### In all three cases

The table works from every engine except Databricks Foreign Catalog. The manifests are correct. The data is correct. It's just the summary totals that are wrong.

---

## 100M row test results

All tests ran on 2026-02-13 using EMR cluster `j-LSKO44EQSF9H` (4x m5.2xlarge, YARN) and Databricks workspace `dbc-0b9b3487-05d4`.

### Cause 1: `add_files`

Wrote 100M rows as Hive/Parquet (50 files, 1.5 GB). Created an Iceberg table. Ran `add_files`.

| | Before fix | After fix |
|---|---|---|
| Summary `total-files-size` | `0` | Correct |
| Actual size | 1,591,756,391 (1.5 GB) | Matches summary |
| Databricks | Error: `srcTableSize: 0` | 100,000,000 rows |
| Fix time | | 73.6 seconds |

### Cause 2: Streaming simulation

Wrote 90M rows, then 100 micro-batches of 100k rows each. 101 total snapshots.

| | Result |
|---|---|
| Summary `total-files-size` | `289,344,340` |
| Actual size | `289,344,340` |
| Match? | Yes, perfect |
| Databricks | Read fine, no error |

Could not reproduce the drift. The native Glue Iceberg catalog maintains accurate summaries through rapid sequential appends. The drift requires an HMS-based catalog with async metadata updates, concurrent writers from separate applications, and specific timing. It's a production issue, not a controlled-test issue.

### Cause 3: Multi-location `add_files`

Wrote 50M rows natively (136 MB). Then wrote 50M more as Hive/Parquet across 5 batch directories and adopted each via `add_files`.

| | Before fix | After fix |
|---|---|---|
| Summary `total-files-size` | `142,632,339` (136 MB, only native writes) | Correct |
| Actual size | `926,360,095` (883 MB) | Matches summary |
| Gap | 747 MB missing from summary | None |
| Databricks | Error: `srcTableSize: 142,632,339` | 100,000,000 rows |
| Fix time | | 100.5 seconds |

---

## Things that do NOT trigger this error

Tried all of these before finding the real cause:

- Stale Hive files sitting next to a clean Iceberg table: Databricks ignored them
- Compaction orphans alongside old Hive files: also fine
- Manually changing a parquet file's size on S3: parquet readers ignore trailing garbage
- Clean Iceberg table at the same path as old Hive data: fine

Extra files on disk don't matter. Databricks reads the metadata, not directory listings. The only thing that triggers this error is the snapshot summary being wrong.

---

## Why not CTAS?

CTAS to a new table works, but it's more work than necessary:

- Need a new table name and new S3 path
- Have to drop the old table and rename
- Temporarily doubles storage
- Downstream references need updating

`rewrite_data_files` + `expire_snapshots` fixes the table in place. Same name, same location, nothing else changes.

---

## Environment

- Databricks: `dbc-0b9b3487-05d4.cloud.databricks.com`
- Foreign Catalog: `glue_catalog` (AWS Glue, `us-west-2`)
- Schema: `federation_demo_db_ryan`
- S3: `s3://glue-federation-demo-332745928618/`
- EMR (initial): `j-1HYXM0VTY8K39` (Spark 3.5.6, Iceberg 1.10.0)
- EMR (100M tests): `j-LSKO44EQSF9H` (4x m5.2xlarge, YARN)

## Files

| File | What it does |
|------|-------------|
| `scripts/reproduce.sh` | Creates the broken table on EMR |
| `scripts/setup_foreign_catalog.py` | Sets up the Databricks foreign catalog and Lake Formation permissions |
| `scripts/query_from_databricks.py` | Queries from Databricks to trigger the error |
| `scripts/cleanup.py` | Runs the full fix and verifies from Databricks |
