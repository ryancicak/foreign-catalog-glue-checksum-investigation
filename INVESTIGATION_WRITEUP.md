# DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED - Foreign Iceberg Table (Glue Catalog)

## The Error

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Failed to convert the table version 0 
to the universal format iceberg. Clone validation failed - Size and number of data 
files in target table should match with source table. 
srcTableSize: 0, targetTableSize: 2767 srcTableNumFiles: 2, targetTableNumFiles: 2
```

## Environment

- Databricks Workspace: `dbc-0b9b3487-05d4.cloud.databricks.com`
- Foreign Catalog: `glue_catalog` (AWS Glue, `us-west-2`)
- Schema: `federation_demo_db_ryan`
- S3 Bucket: `s3://glue-federation-demo-332745928618/`
- EMR Cluster: `j-1HYXM0VTY8K39` (Spark 3.5.6, Iceberg 1.10.0)

---

## Quick Summary

Reproduced and confirmed. The customer had a Hive/Parquet table, migrated it to Iceberg, and the old data files at the root of the S3 path cause a size mismatch during Databricks' internal clone validation. It's not corrupted data, it's just leftover junk from the old table format. Clean it up and the error goes away.

---

## How Foreign Catalog reads work in Databricks

When you query `SELECT * FROM glue_catalog.schema.table`, Databricks doesn't just read the Iceberg table directly. What actually happens:

1. It clones the foreign Iceberg table into an internal Delta table (stored under `__unitystorage/catalogs/.../uniform`)
2. That Delta table gets UniForm properties so it maintains both Delta and Iceberg metadata
3. Before serving results, it validates the clone. The Delta side and the Iceberg side need to agree on file counts and total data size

If they don't agree, you get `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED`.

After a successful clone, the internal Delta table has properties like:
- `lastConvertedMetadataLocation` pointing back to the Glue Iceberg metadata JSON
- `lastConvertedSnapshotId` for the Iceberg snapshot it cloned from
- `icebergTableId` for the Iceberg table UUID

---

## What actually causes the error

Reproduced this exactly. Here's the sequence:

### 1. Customer writes data with Hive/Parquet

Standard Hive writes to S3. Files land at the root of the table path:

```
s3://bucket/db/table/
  _SUCCESS
  part-00000-436014f8-*.snappy.parquet   (1,377 bytes, 5 rows)
  part-00001-436014f8-*.snappy.parquet   (1,390 bytes, 5 rows)
```

### 2. Customer migrates to Iceberg

They create an Iceberg table at the same S3 location and use `add_files` (or `migrate`) to adopt the existing Hive parquet files into Iceberg metadata. Now Iceberg knows about those two `part-*` files.

S3 location now looks like:

```
s3://bucket/db/table/
  _SUCCESS                                       <-- leftover Hive artifact
  part-00000-436014f8-*.snappy.parquet           <-- adopted by Iceberg
  part-00001-436014f8-*.snappy.parquet           <-- adopted by Iceberg
  metadata/00000-*.metadata.json                 <-- Iceberg: empty table creation
  metadata/00001-*.metadata.json                 <-- Iceberg: schema setup
  metadata/00002-*.metadata.json                 <-- Iceberg: after add_files
  metadata/snap-*.avro                           <-- Iceberg snapshot
  metadata/stage-*-manifest-*.avro               <-- Iceberg manifest
```

### 3. Databricks tries to read it via foreign catalog

Databricks clones the table. Here's the problem: the Delta clone starts at version 0 (empty table creation), which has `srcTableSize: 0`. But the Iceberg UniForm side already has a snapshot with 2 data files totaling 2,767 bytes (`targetTableSize: 2767`).

The validation catches this:

```
srcTableSize: 0       <-- Delta version 0 (empty)
targetTableSize: 2767 <-- Iceberg snapshot (has the adopted Hive files)
srcTableNumFiles: 2
targetTableNumFiles: 2
```

File count matches (2 = 2), but size doesn't (0 != 2767). Clone validation fails.

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

The thing that actually matters is the Delta-to-Iceberg version mismatch during the internal clone. Specifically when the Iceberg snapshot has files that weren't part of the Delta table version being converted. Just having extra junk files sitting in the directory isn't enough.

The EMR catalog configuration was also annoying to deal with. The cluster was set up with the UC REST catalog as the default Spark catalog (`cicaktest_catalog`), and the SparkSessionCatalog + Glue metastore factory had classloader issues with the Iceberg Hive client. Ended up using the Glue Iceberg catalog directly with `add_files` to get the migration path working.

---

## How to fix it

### For tables migrated from Hive/Parquet

The data files adopted via `add_files` or `migrate` create this version history inconsistency. Cleanest fix is to just re-create the table:

1. CTAS into a new Iceberg table at a fresh S3 location:

```sql
CREATE TABLE glue_iceberg.db.table_clean
USING iceberg
LOCATION 's3://bucket/db/table_clean/'
AS SELECT * FROM glue_iceberg.db.table_broken;
```

2. Drop the old table and rename the new one.

### Important: deleting orphan files alone does NOT fix this

During the investigation I initially assumed that removing stale Hive files from S3 would fix the error. It doesn't. The adopted `part-*` files are tracked by Iceberg (they were imported via `add_files`), so they're not orphans. The `_SUCCESS` marker is the only actual orphan, and removing it changes nothing.

The root cause is in the metadata version history, not in what's on disk. The Iceberg table's version 0 is an empty table creation, but the snapshot already references files. That inconsistency is what Databricks catches. The only fix is CTAS.

### For tables with orphaned files from compaction (different problem)

If you're seeing this error for a different reason (compaction orphans rather than a Hive migration), Iceberg maintenance might help:

```sql
CALL glue_iceberg.system.expire_snapshots(
    table => 'db.table',
    older_than => TIMESTAMP '2026-02-01 00:00:00',
    retain_last => 1
);

CALL glue_iceberg.system.remove_orphan_files(
    table => 'db.table'
);
```

But if the table was migrated from Hive, CTAS is the answer.

---

## What to tell the customer

Don't worry about this. The error means:

1. The table was originally Hive/Parquet and got migrated to Iceberg
2. The migration left inconsistencies in how the data files relate to the Iceberg version history
3. Databricks Foreign Catalog's internal clone validation catches that mismatch

No data corruption. No data loss. The Iceberg table itself works fine from Spark, Trino, whatever. This is specific to how Databricks clones foreign Iceberg tables into its internal Delta + UniForm format.

Fix: re-create the table cleanly (CTAS to a new S3 location). The new table has a consistent version history from the start. Foreign Catalog will clone it without complaints after that.

---

## Files

| File | What it does |
|------|-------------|
| `scripts/reproduce.sh` | Sets up the broken table on EMR (Hive write + Iceberg create + add_files). Optionally sets up the foreign catalog first. |
| `scripts/setup_foreign_catalog.py` | Creates Databricks storage/service credentials, external location, Glue connection, federated catalog, and Lake Formation permissions |
| `scripts/query_from_databricks.py` | Queries the foreign catalog table from Databricks, shows the error |
| `scripts/cleanup.py` | Fixes the table via CTAS on EMR, re-queries both broken and fixed tables from Databricks |
