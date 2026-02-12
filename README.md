# Reproduce: DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED

Reproduces the `DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED` error that occurs when querying a foreign Iceberg table (Glue Catalog) from Databricks, where the table was migrated from Hive/Parquet and has leftover data file inconsistencies.

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

The error isn't caused by stale files on disk. It's caused by the Iceberg table's version history: version 0 is an empty table, but the current snapshot has data adopted via `add_files`. Databricks' clone process sees that mismatch and fails. Deleting orphan files won't help.

The fix is to CTAS the data into a new Iceberg table at a fresh S3 path. The new table has a clean version history where data exists from the first snapshot, so Databricks' clone validation passes.

This script SSHes to EMR, runs the CTAS, then queries both the broken and fixed tables from Databricks to prove it:

```bash
python scripts/cleanup.py
```

You should see the broken table still failing and the fixed table returning data.

## What's going on

When Databricks reads a foreign Iceberg table via the Glue catalog, it clones the table internally into a Delta table with UniForm (Iceberg compatibility). During the clone, it validates that the Delta side and the Iceberg side agree on file count and total data size.

If the table was migrated from Hive to Iceberg (via `add_files`, `migrate`, etc.), the Iceberg table's version history is inconsistent. Version 0 is an empty table creation (size 0), but the Iceberg snapshot references the adopted Hive files (size > 0). The Delta clone starts at version 0, generates UniForm Iceberg metadata, and the validation catches the size mismatch.

The fix: CTAS into a new Iceberg table at a clean S3 location. The new table's version history is consistent from the start. No data corruption, no data loss. Just a metadata inconsistency from how the migration was done.
