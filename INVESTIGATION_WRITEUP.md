# DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED - Foreign Iceberg Table (Glue Catalog)

## The Error

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Failed to convert the table version 0 
to the universal format iceberg. Clone validation failed - Size and number of data 
files in target table should match with source table. 
srcTableSize: 0, targetTableSize: 2767 srcTableNumFiles: 2, targetTableNumFiles: 2
```

## Environment (reproduction)

- Databricks Workspace: `dbc-0b9b3487-05d4.cloud.databricks.com`
- Foreign Catalog: `glue_catalog` (AWS Glue, `us-west-2`)
- Schema: `federation_demo_db_ryan`
- S3 Bucket: `s3://glue-federation-demo-332745928618/`
- EMR Cluster: `j-1HYXM0VTY8K39` (Spark 3.5.6, Iceberg 1.10.0)

---

## Quick Summary

The error fires when Databricks' internal clone validation finds that the Iceberg snapshot summary statistics (file count, total data size) don't match what it computes by actually following the metadata's file references.

The `src` side of the error comes from the Iceberg snapshot summary (`total-files-size`, `total-data-files`). The `target` side is what Databricks computes by reading the manifests and summing up the actual file sizes. Databricks' numbers are correct. The Iceberg snapshot summary is what's wrong.

This has been confirmed by Databricks engineering: the validation logic accurately follows file references in the metadata. The mismatch is caused by inaccurate snapshot summary statistics on the Iceberg side.

The fix: Iceberg table maintenance (`rewrite_manifests` and/or `rewrite_data_files`, followed by `expire_snapshots`) to create a new snapshot with correct summary stats and remove the old broken ones.

---

## How Foreign Catalog reads work in Databricks

When you query `SELECT * FROM glue_catalog.schema.table`, Databricks doesn't just read the Iceberg table directly. What actually happens:

1. It clones the foreign Iceberg table into an internal Delta table (stored under `__unitystorage/catalogs/.../uniform`)
2. That Delta table gets UniForm properties so it maintains both Delta and Iceberg metadata
3. Before serving results, it validates the clone: the snapshot summary statistics need to match what it computes by following the metadata file references

If they don't match, you get `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED`.

After a successful clone, the internal Delta table has properties like:
- `lastConvertedMetadataLocation` pointing back to the Glue Iceberg metadata JSON
- `lastConvertedSnapshotId` for the Iceberg snapshot it cloned from
- `icebergTableId` for the Iceberg table UUID

---

## What actually causes the error

The Iceberg snapshot summary includes aggregate statistics like `total-files-size` and `total-data-files`. These are supposed to be running totals maintained as commits happen. Databricks reads these summaries and cross-checks them against what it computes by walking the actual manifest entries.

When the summary stats are wrong, the validation fails. There are multiple ways the summary can become inaccurate:

### Cause 1: `add_files` procedure (what we reproduced)

When a customer migrates from Hive to Iceberg using `add_files` (or `migrate`), the procedure creates a snapshot that adopts the existing Hive parquet files. But it records `total-files-size: 0` in the snapshot summary, even though the adopted files have real sizes. This is a bug in the `add_files` procedure.

In our reproduction:

```
srcTableSize: 0       <-- from snapshot summary (total-files-size: 0, the bug)
targetTableSize: 2767 <-- actual sum of file sizes (Databricks computed correctly)
srcTableNumFiles: 2
targetTableNumFiles: 2
```

File count matches (2 = 2), but size doesn't (0 != 2767).

### Cause 2: Streaming metadata drift

High-frequency streaming writes can cause the snapshot summary aggregates to drift from reality. The catalog may not keep up with the rate of changes, or the thread responsible for updating summary stats may hang. This is more common with HMS-based catalogs like Glue. The Iceberg community moved away from HMS-based catalogs early on partly for this reason.

### Cause 3: File location configuration changes

If the customer changes where new data files land (e.g., from writing at the table root to a `/data/` subdirectory, or changing to date-stamped directories), the summary aggregation can lose track of files from the old location. The manifests correctly reference all the files regardless of where they live, but the summary totals only account for a subset.

In production, this can result in massive discrepancies where the snapshot summary reports a fraction of the actual file count and total size. The manifests correctly reference files across all subdirectories, but the summary totals lag behind or only account for files written under a specific path prefix.

### In all cases

The actual Iceberg table works fine from Spark, Trino, Presto, etc. The manifests and file entries are correct. Queries return the right data. It's only the snapshot-level summary stats that are wrong. Databricks is the only engine that validates these summary stats because of its internal UniForm clone process.

---

## Reproduction details

Reproduced on 2026-02-12 using EMR cluster `j-1HYXM0VTY8K39` and Databricks workspace `dbc-0b9b3487-05d4`.

### Steps

1. Wrote 10 rows as Hive/Parquet to `s3://glue-federation-demo-332745928618/federation_demo_db_ryan/hive_migrated_to_ice/` using Spark on EMR (2 parquet files, standard Hive naming convention)

2. Created an Iceberg table in Glue at the same S3 location using the Glue Iceberg catalog on EMR

3. Ran `CALL glue_iceberg.system.add_files(...)` to adopt the existing Hive parquet files into the Iceberg table metadata. Returned `added_files_count: 2`

4. Queried `SELECT * FROM glue_catalog.federation_demo_db_ryan.hive_migrated_to_ice` from Databricks SQL warehouse

5. Got the error:
```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Failed to convert the table version 0 
to the universal format iceberg. Clone validation failed - Size and number of data 
files in target table should match with source table. 
srcTableSize: 0, targetTableSize: 2767 srcTableNumFiles: 2, targetTableNumFiles: 2
```

### Things I tried that did NOT trigger the error

Took me a few attempts to actually reproduce this. These approaches did not work:

- `iceberg_with_stale_hive_data`: Planted Hive files alongside a clean Iceberg table. Databricks read it fine. Stale untracked files on their own don't trip the validation.
- `iceberg_stale_files_test`: Compaction orphans + planted Hive files. Also read fine.
- `iceberg_size_mismatch_test`: Modified the actual parquet data file on S3 to be a different size than what Iceberg metadata records. Also read fine because parquet readers just ignore trailing garbage.
- `migrated_hive_to_iceberg`: Clean Iceberg table at same path as old Hive files. Read fine.

The thing that actually matters is the snapshot summary stats being inaccurate. Just having extra junk files sitting in the directory isn't enough. Databricks follows the metadata file references, not directory listings.

The EMR catalog configuration was also annoying to deal with. The cluster was set up with the UC REST catalog as the default Spark catalog (`cicaktest_catalog`), and the SparkSessionCatalog + Glue metastore factory had classloader issues with the Iceberg Hive client. Ended up using the Glue Iceberg catalog directly with `add_files` to get the migration path working.

---

## How to fix it

The goal is to create a new snapshot with correct summary statistics, then remove the old snapshots that had incorrect stats.

### Tier 1: `rewrite_manifests` + `expire_snapshots` (try this first)

This is the cheapest option. It rewrites the manifest files and creates a new snapshot without touching the actual data files. If the summary stats get recomputed correctly from the manifest rewrite, this is all you need.

```sql
CALL catalog.system.rewrite_manifests('db.table');

CALL catalog.system.expire_snapshots(
    table => 'db.table',
    retain_last => 1
);
```

After running both, query the table from Databricks. If the error is gone, you're done.

### Tier 2: `rewrite_data_files` + `expire_snapshots` (guaranteed fix)

If `rewrite_manifests` didn't fix it, rewrite the actual data files. This is heavier (reads and rewrites every data file) but guaranteed to produce a new snapshot with correct statistics, because the stats are computed from the freshly written files.

```sql
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);

CALL catalog.system.expire_snapshots(
    table => 'db.table',
    retain_last => 1
);
```

The `min-input-files: 1` forces Iceberg to rewrite even if it thinks the files are already well-organized. The sort strategy ensures the rewrite actually happens (bin-packing may skip small files that already meet the target size).

For large tables, run this on a properly sized cluster using `--master yarn` to distribute the work across the cluster.

### Optional: `remove_orphan_files`

After either tier, optionally clean up orphaned files (files on disk that aren't referenced by any snapshot):

```sql
CALL catalog.system.remove_orphan_files(table => 'db.table');
```

This won't fix the error on its own (the error is about summary stats, not orphan files), but it's good hygiene.

### Important: `expire_snapshots` is required

Running `rewrite_manifests` or `rewrite_data_files` alone isn't enough. These commands create new snapshots, but the old snapshots with bad summary stats still exist. `expire_snapshots` with `retain_last => 1` removes them so Databricks won't try to validate against the broken ones.

### Why not CTAS?

CTAS (Create Table As Select) into a new table at a fresh S3 path also works, but it's heavier than it needs to be:

- Requires a new table name and a new S3 location
- Requires dropping the old table and renaming the new one (or updating all downstream references)
- Doubles your storage temporarily while both tables exist
- For large tables, that's a lot of unnecessary data copying

Iceberg maintenance procedures fix the table in-place. Same name, same location, no downstream changes.

### Important: if the root cause is ongoing, it will happen again

If the inaccurate summary stats are caused by something ongoing (e.g., a streaming write pipeline, a misconfigured catalog, or a write pattern that doesn't update summaries correctly), the error will come back after the next batch of writes. The customer needs to identify and fix whatever is causing the summary drift, not just run maintenance as a one-time fix.

---

## What to tell the customer

This error means the Iceberg table's snapshot summary statistics are inaccurate. The summary reports different file counts or total sizes than what the metadata actually references. Databricks validates these stats during its internal Foreign Catalog clone process and catches the mismatch.

This is not data corruption. No data loss. The Iceberg table itself works fine from Spark, Trino, or any other engine that doesn't validate snapshot summaries. This is specific to how Databricks clones foreign Iceberg tables into its internal Delta + UniForm format.

Common causes:
1. Hive-to-Iceberg migration via `add_files` or `migrate` (records `total-files-size: 0`)
2. High-frequency streaming writes causing summary metadata drift
3. File location configuration changes causing the summary to undercount

Fix: run Iceberg table maintenance from Spark (or wherever they manage their Iceberg tables):

1. Try `rewrite_manifests` + `expire_snapshots` first (lightweight, no data rewrite)
2. If that doesn't resolve it, run `rewrite_data_files` + `expire_snapshots` (heavier, but guaranteed)
3. Optionally `remove_orphan_files` for cleanup

If the problem recurs after writes resume, the underlying write pipeline or catalog configuration needs attention.

---

## Files

| File | What it does |
|------|-------------|
| `scripts/reproduce.sh` | Sets up the broken table on EMR (Hive write + Iceberg create + add_files). Optionally sets up the foreign catalog first. |
| `scripts/setup_foreign_catalog.py` | Creates Databricks storage/service credentials, external location, Glue connection, federated catalog, and Lake Formation permissions |
| `scripts/query_from_databricks.py` | Queries the foreign catalog table from Databricks, shows the error |
| `scripts/cleanup.py` | Fixes the table via `rewrite_manifests` + `rewrite_data_files` + `expire_snapshots` on EMR, re-queries from Databricks to confirm |
