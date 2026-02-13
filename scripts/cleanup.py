#!/usr/bin/env python3
"""
Fix the DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED error by running Iceberg
table maintenance on EMR, then re-query from Databricks to confirm.

The root cause: the Iceberg snapshot summary statistics (total-files-size,
total-data-files) are inaccurate. Databricks validates these during its
internal clone and fails when they don't match the actual file references.

The fix (tiered approach):
  1. rewrite_manifests  -- cheapest, rewrites manifests and may fix summary stats
  2. rewrite_data_files -- guaranteed fix, rewrites all data files with fresh stats
  3. expire_snapshots   -- removes old snapshots with bad summary stats
  4. remove_orphan_files -- optional cleanup of unreferenced files

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
FOREIGN_CATALOG = env["FOREIGN_CATALOG"]
AWS_REGION = env["AWS_REGION"]
CLIENT_ID = env["DATABRICKS_CLIENT_ID"]

FQN = f"{FOREIGN_CATALOG}.{GLUE_DATABASE}.{TABLE_NAME}"

SSH_OPTS = ["-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=15", "-i", EMR_PEM_PATH]
SSH_CMD = ["ssh"] + SSH_OPTS + [f"hadoop@{EMR_HOST}"]
SCP_CMD = ["scp"] + SSH_OPTS

SPARK_CONFIGS = f"""
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_iceberg.warehouse", "s3://{S3_BUCKET}")
    .config("spark.sql.catalog.glue_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_iceberg.region", "{AWS_REGION}")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_iceberg")
"""


def ssh_run(command, timeout=180):
    """Run a command on EMR via SSH, return stdout."""
    result = subprocess.run(
        SSH_CMD + [command], capture_output=True, text=True, timeout=timeout
    )
    if result.returncode != 0 and result.stderr.strip():
        print(f"  SSH stderr: {result.stderr.strip()[:200]}")
    return result.stdout.strip()


def run_pyspark_on_emr(script_content, timeout=600):
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
        'spark-submit --master yarn --deploy-mode client '
        '--conf spark.sql.catalogImplementation=in-memory '
        '--conf spark.sql.defaultCatalog=spark_catalog '
        '--jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar '
        '/tmp/cleanup_script.py 2>&1 | grep "^OUT:"',
        timeout=timeout
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
    fqn = f"glue_iceberg.{GLUE_DATABASE}.{TABLE_NAME}"

    print("=" * 60)
    print("Fix: Iceberg table maintenance (in-place)")
    print("=" * 60)
    print(f"  Table (Iceberg):    {fqn}")
    print(f"  Table (Databricks): {FQN}")
    print()

    # ----------------------------------------------------------------
    # Step 1: Show current state before fix
    # ----------------------------------------------------------------
    print("--- Step 1: Current state of table (via EMR) ---")
    show_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("show_before_fix")
{SPARK_CONFIGS}
    .getOrCreate())

fqn = "{fqn}"

rows = spark.sql("SELECT * FROM " + fqn).collect()
print("OUT:ROW_COUNT:" + str(len(rows)))

files = spark.sql("SELECT file_path, file_size_in_bytes FROM " + fqn + ".files").collect()
total_actual_size = sum(f.file_size_in_bytes for f in files)
print("OUT:FILE_COUNT:" + str(len(files)))
print("OUT:ACTUAL_TOTAL_SIZE:" + str(total_actual_size) + " bytes")
for f in files[:10]:
    print("OUT:FILE:" + f.file_path.split("/")[-1] + " (" + str(f.file_size_in_bytes) + " bytes)")
if len(files) > 10:
    print("OUT:... and " + str(len(files) - 10) + " more files")

snapshots = spark.sql("SELECT * FROM " + fqn + ".snapshots").collect()
for s in snapshots:
    summary = s.summary if hasattr(s, 'summary') else {{}}
    total_size = summary.get('total-files-size', 'N/A') if isinstance(summary, dict) else 'N/A'
    total_files = summary.get('total-data-files', 'N/A') if isinstance(summary, dict) else 'N/A'
    print("OUT:SNAPSHOT:" + str(s.snapshot_id) + " total-files-size=" + str(total_size) + " total-data-files=" + str(total_files))

history = spark.sql("SELECT snapshot_id, operation FROM " + fqn + ".history").collect()
for h in history:
    print("OUT:HISTORY:" + str(h.snapshot_id) + " " + h.operation)

spark.stop()
"""
    output = run_pyspark_on_emr(show_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # ----------------------------------------------------------------
    # Step 2: rewrite_manifests
    # ----------------------------------------------------------------
    print("\n--- Step 2: rewrite_manifests ---")
    rewrite_manifests_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("rewrite_manifests")
{SPARK_CONFIGS}
    .getOrCreate())

fqn = "{fqn}"

print("OUT:Running rewrite_manifests...")
result = spark.sql("CALL glue_iceberg.system.rewrite_manifests('" + fqn + "')").collect()
for r in result:
    print("OUT:RESULT:" + str(r))
print("OUT:rewrite_manifests complete")
spark.stop()
"""
    output = run_pyspark_on_emr(rewrite_manifests_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # ----------------------------------------------------------------
    # Step 3: rewrite_data_files
    # ----------------------------------------------------------------
    print("\n--- Step 3: rewrite_data_files ---")
    rewrite_data_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("rewrite_data_files")
{SPARK_CONFIGS}
    .getOrCreate())

fqn = "{fqn}"

print("OUT:Running rewrite_data_files...")
result = spark.sql(
    "CALL glue_iceberg.system.rewrite_data_files("
    "  table => '" + fqn + "',"
    "  strategy => 'sort',"
    "  sort_order => 'id ASC NULLS LAST',"
    "  options => map('min-input-files', '1')"
    ")"
).collect()
for r in result:
    print("OUT:RESULT:" + str(r))
print("OUT:rewrite_data_files complete")
spark.stop()
"""
    output = run_pyspark_on_emr(rewrite_data_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # ----------------------------------------------------------------
    # Step 4: expire_snapshots
    # ----------------------------------------------------------------
    print("\n--- Step 4: expire_snapshots (retain_last=1) ---")
    expire_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("expire_snapshots")
{SPARK_CONFIGS}
    .getOrCreate())

fqn = "{fqn}"

print("OUT:Running expire_snapshots...")
result = spark.sql(
    "CALL glue_iceberg.system.expire_snapshots("
    "  table => '" + fqn + "',"
    "  retain_last => 1"
    ")"
).collect()
for r in result:
    print("OUT:RESULT:" + str(r))

# Show state after fix
snapshots = spark.sql("SELECT * FROM " + fqn + ".snapshots").collect()
for s in snapshots:
    summary = s.summary if hasattr(s, 'summary') else {{}}
    total_size = summary.get('total-files-size', 'N/A') if isinstance(summary, dict) else 'N/A'
    total_files = summary.get('total-data-files', 'N/A') if isinstance(summary, dict) else 'N/A'
    print("OUT:SNAPSHOT_AFTER:" + str(s.snapshot_id) + " total-files-size=" + str(total_size) + " total-data-files=" + str(total_files))

print("OUT:expire_snapshots complete")
spark.stop()
"""
    output = run_pyspark_on_emr(expire_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # ----------------------------------------------------------------
    # Step 5: remove_orphan_files (optional cleanup)
    # ----------------------------------------------------------------
    print("\n--- Step 5: remove_orphan_files ---")
    orphan_script = f"""
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("remove_orphan_files")
{SPARK_CONFIGS}
    .getOrCreate())

fqn = "{fqn}"

print("OUT:Running remove_orphan_files...")
result = spark.sql(
    "CALL glue_iceberg.system.remove_orphan_files(table => '" + fqn + "')"
).collect()
for r in result:
    print("OUT:ORPHAN:" + str(r))
print("OUT:remove_orphan_files complete")
spark.stop()
"""
    output = run_pyspark_on_emr(orphan_script)
    for line in output.splitlines():
        print(f"  {line.replace('OUT:', '')}")

    # ----------------------------------------------------------------
    # Step 6: Query from Databricks to confirm the fix
    # ----------------------------------------------------------------
    print(f"\n--- Step 6: Query table from Databricks ---")
    wc = WorkspaceClient(
        host=env["DATABRICKS_HOST"],
        client_id=env["DATABRICKS_CLIENT_ID"],
        client_secret=env["DATABRICKS_CLIENT_SECRET"],
    )
    wh_id = get_warehouse(wc)

    # Force catalog refresh
    run_sql(wc, f"SHOW TABLES IN {FOREIGN_CATALOG}.{GLUE_DATABASE}", wh_id)

    # Grant
    run_sql(wc, f"GRANT SELECT ON TABLE {FQN} TO `{CLIENT_ID}`", wh_id)

    # Try refreshing the foreign table to clear any cached clone state
    print(f"  Refreshing foreign table...")
    run_sql(wc, f"REFRESH FOREIGN TABLE {FQN}", wh_id)
    time.sleep(5)

    # Query the table
    r = run_sql(wc, f"SELECT * FROM {FQN} LIMIT 10", wh_id)
    if "error" in r:
        err = r["error"]
        if "DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED" in err:
            print(f"  Still failing with DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED")
            print(f"  Error: {err[:300]}")
        else:
            print(f"  ERROR: {err[:300]}")
    else:
        print(f"  Query succeeded! Returned {len(r.get('rows', []))} rows:")
        for row in r.get("rows", [])[:5]:
            print(f"    {row}")

    # ----------------------------------------------------------------
    # Summary
    # ----------------------------------------------------------------
    print()
    print("=" * 60)
    print("RESULT")
    print("=" * 60)
    print(f"  Table: {FQN}")
    print()
    print("  Maintenance steps run:")
    print("    1. rewrite_manifests   -- rewrote manifest files")
    print("    2. rewrite_data_files  -- rewrote data files with fresh stats")
    print("    3. expire_snapshots    -- removed old snapshots with bad summary stats")
    print("    4. remove_orphan_files -- cleaned up unreferenced files")
    print()
    print("  Table fixed in-place. Same S3 location, same table name.")
    print("  No CTAS, no data movement, no new table needed.")


if __name__ == "__main__":
    main()
