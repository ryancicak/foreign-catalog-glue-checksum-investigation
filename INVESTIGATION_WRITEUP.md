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
-- Rewrites all data files, creates a fresh snapshot
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

Don't skip `expire_snapshots`. It removes the old snapshot with the bad numbers. Without it, Databricks still sees the broken one.

**Important caveat for Cause 3:** The above fixes work for Causes 1 and 2 (`add_files` related). For Cause 3 (non-atomic writers), `rewrite_data_files` does not fix it because Iceberg computes summaries incrementally and the wrong values carry forward. The customer needs to correct the snapshot summary directly. See the "100M row test results" section for how.

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

Every Iceberg snapshot has a summary. It's a small set of key-value pairs with running totals like `total-files-size` (sum of all data file sizes) and `total-data-files` (number of data files). These fields are optional in the Iceberg spec. Not all writers populate them, and not all writers that do populate them keep them accurate. These totals are supposed to be updated every time a commit happens, but nothing enforces that.

Databricks reads them and compares against what it finds by actually walking the manifests. If a writer populated these fields but got the numbers wrong, Databricks catches the mismatch. Other engines like Spark, Trino, and Presto never look at the summary. They go straight to the manifests and read the files directly, which is why the table works fine everywhere else.

Databricks engineering confirmed this validation logic is correct.

---

## Three ways the summary goes wrong

### 1. `add_files` (Hive-to-Iceberg migration)

Customer migrates from Hive to Iceberg using `add_files`. The procedure adopts the existing parquet files but sets `total-files-size: 0` in the summary. Bug in the procedure.

### 2. Files in multiple locations + `add_files`

Data files in different subdirectories get adopted via `add_files`. The summary only counts native writes. Adopted file sizes never get added to `total-files-size`. The gap grows with every adoption.

### 3. Non-atomic writes to Iceberg

When metadata is written separately from the data (common with streaming tables from engines like Snowflake), the summary fields can end up inaccurate. If the optional summary fields are populated but wrong, Databricks catches the mismatch. The native Glue Iceberg catalog handles commits atomically, so it doesn't have this problem on its own.

Important distinction: when the summary is fundamentally wrong (not off by a predictable amount like the `add_files` bug, but entirely fabricated by a non-atomic writer), `rewrite_data_files` does NOT fix it. Iceberg computes summaries incrementally, so the bad numbers carry forward into every new snapshot. The fix is to correct the summary metadata directly. See the Cause 3 test results below for a step-by-step walkthrough.

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

### Cause 2: Multi-location `add_files`

Wrote 50M rows natively (136 MB). Then wrote 50M more as Hive/Parquet across 5 batch directories and adopted each via `add_files`.

| | Before fix | After fix |
|---|---|---|
| Summary `total-files-size` | `142,632,339` (136 MB, only native writes) | Correct |
| Actual size | `926,360,095` (883 MB) | Matches summary |
| Gap | 747 MB missing from summary | None |
| Databricks | Error: `srcTableSize: 142,632,339` | 100,000,000 rows |
| Fix time | | 100.5 seconds |

### Cause 3: Non-atomic writes

**Organic reproduction attempt (streaming simulation):**

Wrote 90M rows, then 100 micro-batches of 100k rows each. 101 total snapshots through the Glue Iceberg catalog.

| | Result |
|---|---|
| Summary `total-files-size` | `289,344,340` |
| Actual size | `289,344,340` |
| Match? | Yes, perfect |
| Databricks | Read fine, no error |

Could not reproduce organically. The Glue Iceberg catalog commits atomically, so the summary stays accurate even through rapid appends. This kind of drift requires a writer that updates metadata separately from data (like Snowflake streaming tables), which is not how Spark + Glue works.

**Simulated reproduction (manual metadata corruption):**

Since we can't organically produce a wrong summary with Spark + Glue, we simulated it. Created a clean 100M row table (50 files, 275 MB), then edited the metadata JSON on S3 to set `total-files-size: 0` and `total-data-files: 5`. Updated the Glue catalog pointer to the corrupted file. Manifests still correct, summary now wrong. Exactly what a non-atomic writer would produce.

| | Before fix | After fix |
|---|---|---|
| Summary `total-files-size` | `0` (actual: 288,516,122) | `308,073,018` (correct) |
| Summary `total-data-files` | `5` (actual: 50) | `1` (correct, compacted) |
| Databricks | Error: `srcTableSize: 0, targetTableSize: 288516122 srcTableNumFiles: 5, targetTableNumFiles: 50` | 100,000,000 rows |

**Key finding: `rewrite_data_files` did NOT fix this.**

After running `rewrite_data_files` + `expire_snapshots`, the error changed but persisted:

```
srcTableSize: 19556896, targetTableSize: 308073018
srcTableNumFiles: 1, targetTableNumFiles: 1
```

File count matched (1 = 1), but size was still wrong (19.5 MB vs 293.8 MB). The math:

```
new_total = old_total (0, corrupted) - deleted (288 MB) + added (308 MB) = 19.5 MB
```

The wrong starting value carries forward through every rewrite.

**The actual fix:** Use Spark to read the real file count and total size from the manifests. Write a corrected metadata JSON with accurate summary values. Update the Glue catalog pointer to the new file. After this, Databricks queried all 100M rows successfully.

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

CTAS to a new table works, but it's more disruptive than necessary:

- Need a new table name and new S3 path
- Have to drop the old table and rename
- Temporarily doubles storage
- Downstream references need updating

For Causes 1 and 2, `rewrite_data_files` + `expire_snapshots` fixes the table in place. Same name, same location, nothing else changes. For Cause 3, correcting the metadata JSON is even lighter since you don't rewrite any data files at all.

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
