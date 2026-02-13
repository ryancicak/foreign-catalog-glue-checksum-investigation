# Reproduce: DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED

Reproduces the `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED` error that occurs when querying a foreign Iceberg table (Glue Catalog) from Databricks. The root cause is inaccurate Iceberg snapshot summary statistics -- the `total-files-size` and/or `total-data-files` values in the snapshot summary don't match what the metadata actually references. Databricks is the only engine that validates these summary stats (during its internal UniForm clone), so the table reads fine from Spark, Trino, etc., but fails from Databricks Foreign Catalog.

This has been confirmed by Databricks engineering: the validation logic is correct. The problem is on the Iceberg metadata side.

See `INVESTIGATION_WRITEUP.md` for the full root cause analysis.

## Prerequisites

You need these before you start:

- **AWS EMR cluster** (EMR 7.x) with Spark and Iceberg installed, SSH access via a PEM key. The EMR instance profile needs S3 and Glue permissions.
- **Databricks workspace** with Unity Catalog enabled and a service principal with admin or metastore-level privileges.
- **Python 3.9+** and **pip** on your local machine.
- **SSH access** from your machine to the EMR master node (security group needs to allow your IP on port 22).
- **AWS CLI configured** on your local machine (needed only if setting up the foreign catalog from scratch, for Lake Formation permissions).

## Setup

```bash
# Clone or navigate to this folder
cd foreign-catalog-glue-checksum-investigation

# Install Python dependencies
pip install -r requirements.txt

# Copy the example env and fill in your values
cp .env.example .env
```

Edit `.env` with your details:

```
EMR_HOST=ec2-xx-xx-xx-xx.us-west-2.compute.amazonaws.com
EMR_PEM_PATH=~/Downloads/your-key.pem
DATABRICKS_HOST=https://dbc-xxxxx.cloud.databricks.com
DATABRICKS_CLIENT_ID=your-service-principal-client-id
DATABRICKS_CLIENT_SECRET=your-service-principal-secret
S3_BUCKET=your-glue-federation-bucket
GLUE_DATABASE=your_glue_database
TABLE_NAME=hive_migrated_to_ice
FOREIGN_CATALOG=glue_catalog
AWS_REGION=us-west-2
```

If you don't already have a foreign catalog set up, also fill in:

```
IAM_ROLE_ARN=arn:aws:iam::123456789012:role/your-cross-account-role
```

This IAM role needs S3 read access on your bucket, Glue read access, and a trust policy allowing the Databricks UC AWS account (`414351767826`) to assume it. The reproduce script will ask if you need the foreign catalog created and handle it for you (storage credential, service credential, external location, Glue connection, federated catalog, and Lake Formation permissions).

## Step 1: Reproduce the bad table state

The script first asks if you have a foreign catalog already. If not, it sets up the Databricks foreign catalog, Glue connection, and Lake Formation permissions automatically.

Then it SSHes into your EMR cluster, writes Hive/Parquet data to S3, creates an Iceberg table at the same location in Glue, and uses `add_files` to adopt the Hive data into the Iceberg metadata. This is exactly how customers end up in this situation.

```bash
bash scripts/reproduce.sh
```

## Step 2: Trigger the error from Databricks

Queries the table via the Databricks foreign catalog. You should see:

```
[DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED] Failed to convert the table version 0
to the universal format iceberg. Clone validation failed - Size and number of data
files in target table should match with source table.
srcTableSize: 0, targetTableSize: 2767 srcTableNumFiles: 2, targetTableNumFiles: 2
```

```bash
python scripts/query_from_databricks.py
```

## Step 3: Fix it

The error comes from inaccurate Iceberg snapshot summary statistics. The `total-files-size` in the snapshot summary doesn't match the actual sum of file sizes referenced by the metadata. Databricks reads the summary stats during its internal clone validation and fails when they don't line up.

The fix is Iceberg table maintenance run from Spark (on EMR, Glue jobs, or wherever the customer manages their Iceberg tables):

### Tier 1: Try `rewrite_manifests` first (cheapest)

This rewrites the manifest files and creates a new snapshot. If the problem is that the snapshot summary aggregates drifted from what the manifests describe, this may recompute the stats correctly without touching any data files.

```sql
CALL catalog.system.rewrite_manifests('db.table');
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

### Tier 2: `rewrite_data_files` (guaranteed fix)

If `rewrite_manifests` alone doesn't resolve it, rewrite the actual data files. This reads every file, writes new ones, and creates a new snapshot with correct statistics computed from scratch.

```sql
CALL catalog.system.rewrite_data_files(
    table => 'db.table',
    strategy => 'sort',
    sort_order => 'id ASC NULLS LAST',
    options => map('min-input-files', '1')
);
CALL catalog.system.expire_snapshots(table => 'db.table', retain_last => 1);
```

### Then clean up orphans

After either tier, optionally remove any leftover files that are no longer referenced:

```sql
CALL catalog.system.remove_orphan_files(table => 'db.table');
```

The cleanup script runs the full sequence (rewrite manifests, rewrite data files, expire snapshots), then queries from Databricks to confirm:

```bash
python scripts/cleanup.py
```

For large tables (hundreds of GBs or more), run on a properly sized cluster using `--master yarn` instead of `local[*]`.

## What's going on

When Databricks reads a foreign Iceberg table via the Glue catalog, it clones the table internally into a Delta table with UniForm (Iceberg compatibility). During the clone, it validates that the snapshot summary statistics (file count, total data size) match what it computes by following the metadata's file references. If they don't match, you get `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED`.

The snapshot summary can become inaccurate through several paths:

- **Hive-to-Iceberg migration via `add_files`**: The `add_files` procedure records `total-files-size: 0` in the snapshot summary, even though the adopted files have real sizes. This is a bug in the procedure.
- **Streaming writes with metadata drift**: High-frequency streaming writes can cause the catalog's summary aggregation to fall behind or lose track, especially with HMS-based catalogs like Glue.
- **File location configuration changes**: Changing where new data files land (e.g., switching from root-level writes to a `/data/` subdirectory) can cause the summary to undercount files and sizes from the old location.

In all cases, the actual Iceberg table works fine. The manifests and file entries are correct. It's only the summary-level aggregate stats that are wrong. Databricks is the only engine strict enough to validate them.

This is not a Databricks bug. Databricks engineering has confirmed the validation logic is correct -- it follows the Iceberg metadata's file references and correctly computes totals. The problem is that the Iceberg snapshot summary statistics don't accurately reflect what the metadata references.

## Files

| File | What it does |
|------|-------------|
| `scripts/reproduce.sh` | Sets up the broken table on EMR (Hive write + Iceberg create + add_files). Optionally sets up the foreign catalog first. |
| `scripts/setup_foreign_catalog.py` | Creates Databricks storage/service credentials, external location, Glue connection, federated catalog, and Lake Formation permissions |
| `scripts/query_from_databricks.py` | Queries the foreign catalog table from Databricks, shows the error |
| `scripts/cleanup.py` | Fixes the table via `rewrite_manifests` + `rewrite_data_files` + `expire_snapshots` on EMR, re-queries from Databricks to confirm |
