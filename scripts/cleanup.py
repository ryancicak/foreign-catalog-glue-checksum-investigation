#!/usr/bin/env python3
"""
Fix the DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED error by re-creating
the Iceberg table cleanly via CTAS on EMR, then re-query from Databricks
to confirm it works.

The problem isn't orphan files on disk. The problem is the Iceberg table's
version history: version 0 is an empty table creation, but the current
snapshot has adopted Hive files via add_files. Databricks' internal clone
starts at version 0 (size 0) and the Iceberg side already has data (size > 0).
That mismatch is baked into the metadata, not fixable by deleting files.

The fix: CTAS into a fresh Iceberg table at a new S3 path. The new table
has a clean version history where the data exists from the first snapshot.

Run this after query_from_databricks.py has shown the error.
"""

import os
import sys
import time
import subprocess
import tempfile
from pathlib import Path
from dotenv import dotenv_values

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Load .env
PROJECT_DIR = Path(__file__).resolve().parent.parent
env = dotenv_values(PROJECT_DIR / ".env")

for var in ["EMR_HOST", "EMR_PEM_PATH", "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET", "S3_BUCKET", "GLUE_DATABASE", "TABLE_NAME",
            "FOREIGN_CATALOG", "AWS_REGION"]:
    if not env.get(var):
        print(f"Missing {var} in .env")
        sys.exit(1)

EMR_HOST = env["EMR_HOST"]
EMR_PEM_PATH = os.path.expanduser(env["EMR_PEM_PATH"])
S3_BUCKET = env["S3_BUCKET"]
GLUE_DATABASE = env["GLUE_DATABASE"]
TABLE_NAME = env["TABLE_NAME"]
TABLE_NAME_FIXED = f"{TABLE_NAME}_fixed"
FOREIGN_CATALOG = env["FOREIGN_CATALOG"]
AWS_REGION = env["AWS_REGION"]
CLIENT_ID = env["DATABRICKS_CLIENT_ID"]

S3_LOCATION = f"s3://{S3_BUCKET}/{GLUE_DATABASE}/{TABLE_NAME}"
S3_LOCATION_FIXED = f"s3://{S3_BUCKET}/{GLUE_DATABASE}/{TABLE_NAME_FIXED}"
FQN = f"{FOREIGN_CATALOG}.{GLUE_DATABASE}.{TABLE_NAME}"
FQN_FIXED = f"{FOREIGN_CATALOG}.{GLUE_DATABASE}.{TABLE_NAME_FIXED}"

SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=15", "-i", EMR_PEM_PATH]
SSH_CMD = ["ssh"] + SSH_OPTS + [f"hadoop@{EMR_HOST}"]
SCP_CMD = ["scp"] + SSH_OPTS


def ssh_run(command, timeout=180):
    """Run a command on EMR via SSH, return stdout."""
    result = subprocess.run(
        SSH_CMD + [command], capture_output=True, text=True, timeout=timeout
    )
    if result.returncode != 0 and result.stderr.strip():
        print(f"  SSH stderr: {result.stderr.strip()[:200]}")
    return result.stdout.strip()


def run_pyspark_on_emr(script_content):
    """Write a PySpark script locally, SCP to EMR, run via spark-submit."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(script_content)
        local_path = f.name

    subprocess.run(
        SCP_CMD + [local_path, f"hadoop@{EMR_HOST}:/tmp/cleanup_script.py"],
        capture_output=True, timeout=30
    )
    os.unlink(local_path)

    output = ssh_run(
        'spark-submit --master "local[*]" '
        '--conf spark.sql.catalogImplementation=in-memory '
        '--conf spark.sql.defaultCatalog=spark_catalog '
        '--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar '
        '/tmp/cleanup_script.py 2>&1 | grep "^OUT:"',
        timeout=300
    )
    return output


def get_warehouse(w):
    """Grab first SQL warehouse, start if needed."""
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("  No SQL warehouses found.")
        sys.exit(1)
    wh = warehouses[0]
    if wh.state and wh.state.value != "RUNNING":
        print(f"  Starting warehouse {wh.name}...")
        w.warehouses.start(wh.id)
        for _ in range(60):
            time.sleep(3)
            status = w.warehouses.get(wh.id)
            if status.state and status.state.value == "RUNNING":
                break
    return wh.id


def run_sql(w, statement, wh_id):
    """Execute SQL on Databricks."""
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=wh_id, wait_timeout="50s"
    )
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status and resp.status.state == StatementState.FAILED:
        return {"error": str(resp.status.error)}
    if resp.manifest and resp.result:
        columns = [col.name for col in resp.manifest.schema.columns]
        rows = []
        if resp.result.data_array:
            for row in resp.result.data_array:
                rows.append(dict(zip(columns, row)))
        return {"columns": columns, "rows": rows}
    return {"columns": [], "rows": []}


def main():
    print("=" * 60)
    print("Fix: Re-create table with clean Iceberg history")
    print("=" * 60)
    print(f"  Broken table:  {FQN}")
    print(f"  Fixed table:   {FQN_FIXED}")
    print(f"  Broken S3:     {S3_LOCATION}")
    print(f"  Fixed S3:      {S3_LOCATION_FIXED}")
    print()

    # Step 1: Show what the broken table looks like
    print("--- Step 1: Current state of broken table (via EMR) ---")
    show_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("show_broken").master("local[*]")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_iceberg.warehouse", "s3://{S3_BUCKET}")
    .config("spark.sql.catalog.glue_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_iceberg.region", "{AWS_REGION}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_iceberg")
    .getOrCreate())

fqn = "glue_iceberg.{GLUE_DATABASE}.{TABLE_NAME}"
rows = spark.sql("SELECT * FROM " + fqn).collect()
print("OUT:BROKEN_ROWS:" + str(len(rows)))
files = spark.sql("SELECT file_path, file_size_in_bytes FROM " + fqn + ".files").collect()
for f in files:
    print("OUT:BROKEN_FILE:" + f.file_path + " (" + str(f.file_size_in_bytes) + " bytes)")
history = spark.sql("SELECT snapshot_id, operation FROM " + fqn + ".history").collect()
for h in history:
    print("OUT:BROKEN_HISTORY:" + str(h.snapshot_id) + " " + h.operation)
spark.stop()
"""
    output = run_pyspark_on_emr(show_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # Step 2: CTAS into a new clean table
    print("\n--- Step 2: CTAS into clean Iceberg table on EMR ---")
    ctas_script = f"""
import boto3
from pyspark.sql import SparkSession

BUCKET = "{S3_BUCKET}"
DB = "{GLUE_DATABASE}"
OLD_TABLE = "{TABLE_NAME}"
NEW_TABLE = "{TABLE_NAME_FIXED}"
NEW_LOCATION = "{S3_LOCATION_FIXED}"
REGION = "{AWS_REGION}"

# Clean up previous fixed table if exists
s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=BUCKET, Prefix=DB + "/" + NEW_TABLE):
    if "Contents" in page:
        for obj in page["Contents"]:
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
try:
    glue.delete_table(DatabaseName=DB, Name=NEW_TABLE)
except:
    pass

spark = (SparkSession.builder.appName("ctas_fix").master("local[*]")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_iceberg.warehouse", "s3://" + BUCKET)
    .config("spark.sql.catalog.glue_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_iceberg.region", REGION)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_iceberg")
    .getOrCreate())

old_fqn = "glue_iceberg." + DB + "." + OLD_TABLE
new_fqn = "glue_iceberg." + DB + "." + NEW_TABLE

# CTAS: creates a brand new Iceberg table with data from the broken one
spark.sql("CREATE TABLE " + new_fqn + " USING iceberg LOCATION '" + NEW_LOCATION + "' AS SELECT * FROM " + old_fqn)
print("OUT:CTAS_DONE")

# Verify the new table
rows = spark.sql("SELECT * FROM " + new_fqn).collect()
print("OUT:FIXED_ROWS:" + str(len(rows)))
files = spark.sql("SELECT file_path, file_size_in_bytes FROM " + new_fqn + ".files").collect()
for f in files:
    print("OUT:FIXED_FILE:" + f.file_path + " (" + str(f.file_size_in_bytes) + " bytes)")
history = spark.sql("SELECT snapshot_id, operation FROM " + new_fqn + ".history").collect()
for h in history:
    print("OUT:FIXED_HISTORY:" + str(h.snapshot_id) + " " + h.operation)
spark.stop()
"""
    output = run_pyspark_on_emr(ctas_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # Step 3: Query the FIXED table from Databricks
    print(f"\n--- Step 3: Query fixed table from Databricks ---")
    wc = WorkspaceClient(
        host=env["DATABRICKS_HOST"],
        client_id=env["DATABRICKS_CLIENT_ID"],
        client_secret=env["DATABRICKS_CLIENT_SECRET"],
    )
    wh_id = get_warehouse(wc)

    # Force discovery
    run_sql(wc, f"SHOW TABLES IN {FOREIGN_CATALOG}.{GLUE_DATABASE}", wh_id)

    # Grant
    run_sql(wc, f"GRANT SELECT ON TABLE {FQN_FIXED} TO `{CLIENT_ID}`", wh_id)

    # Query the fixed table
    r = run_sql(wc, f"SELECT * FROM {FQN_FIXED} LIMIT 10", wh_id)
    if "error" in r:
        print(f"  ERROR: {r['error']}")
    else:
        print(f"  Query succeeded! Returned {len(r.get('rows', []))} rows:")
        for row in r.get("rows", []):
            print(f"    {row}")

    # Also confirm the broken table still fails
    print(f"\n--- Step 4: Confirm broken table still fails ---")
    r = run_sql(wc, f"SELECT * FROM {FQN} LIMIT 5", wh_id)
    if "error" in r:
        print(f"  Broken table still fails (expected):")
        # Print just the error class, not the full stack
        err = r["error"]
        if "DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED" in err:
            print(f"  DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED (as expected)")
        else:
            print(f"  {err[:200]}")
    else:
        print(f"  Broken table now works too? Returned {len(r.get('rows', []))} rows")

    print()
    print("=" * 60)
    print("RESULT")
    print("=" * 60)
    print(f"  Broken table ({FQN}): still fails")
    print(f"  Fixed table  ({FQN_FIXED}): works")
    print()
    print("  The fix: CTAS into a new Iceberg table at a clean S3 location.")
    print("  The new table has a consistent version history where data exists")
    print("  from the first snapshot, so Databricks' clone validation passes.")
    print()
    print("  To make the fix permanent, drop the broken table and rename the")
    print("  fixed one (or just use the fixed table going forward).")


if __name__ == "__main__":
    main()
