#!/usr/bin/env python3
"""
Query the foreign catalog table from Databricks to trigger
DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED.

Run this after reproduce.sh has set up the table on EMR.
"""

import os
import sys
import time
from pathlib import Path
from dotenv import dotenv_values

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Load .env
PROJECT_DIR = Path(__file__).resolve().parent.parent
env = dotenv_values(PROJECT_DIR / ".env")

for var in ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
            "GLUE_DATABASE", "TABLE_NAME", "FOREIGN_CATALOG"]:
    if not env.get(var):
        print(f"Missing {var} in .env")
        sys.exit(1)

FOREIGN_CATALOG = env["FOREIGN_CATALOG"]
GLUE_DATABASE = env["GLUE_DATABASE"]
TABLE_NAME = env["TABLE_NAME"]
CLIENT_ID = env["DATABRICKS_CLIENT_ID"]
FQN = f"{FOREIGN_CATALOG}.{GLUE_DATABASE}.{TABLE_NAME}"

# Connect
w = WorkspaceClient(
    host=env["DATABRICKS_HOST"],
    client_id=env["DATABRICKS_CLIENT_ID"],
    client_secret=env["DATABRICKS_CLIENT_SECRET"],
)


def get_warehouse():
    """Grab the first SQL warehouse. Start it if stopped."""
    warehouses = list(w.warehouses.list())
    if not warehouses:
        print("No SQL warehouses found in workspace.")
        sys.exit(1)

    wh = warehouses[0]
    print(f"  Using warehouse: {wh.name} ({wh.id}), state: {wh.state}")

    if wh.state and wh.state.value != "RUNNING":
        print("  Starting warehouse...")
        w.warehouses.start(wh.id)
        for _ in range(60):
            time.sleep(3)
            status = w.warehouses.get(wh.id)
            if status.state and status.state.value == "RUNNING":
                break
        print("  Running.")

    return wh.id


def run_sql(statement, warehouse_id):
    """Execute SQL and return result dict."""
    resp = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=warehouse_id, wait_timeout="50s"
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
    print("Query foreign catalog table from Databricks")
    print("=" * 60)
    print(f"  Table: {FQN}")
    print()

    wh_id = get_warehouse()

    # Force table discovery
    print("\n--- SHOW TABLES ---")
    r = run_sql(f"SHOW TABLES IN {FOREIGN_CATALOG}.{GLUE_DATABASE}", wh_id)
    if "error" in r:
        print(f"  Error: {r['error']}")
    else:
        for row in r.get("rows", []):
            print(f"  {row.get('tableName', row)}")

    # Grant SELECT to the service principal
    print(f"\n--- GRANT SELECT ---")
    r = run_sql(f"GRANT SELECT ON TABLE {FQN} TO `{CLIENT_ID}`", wh_id)
    if "error" in r:
        print(f"  Error: {r['error']}")
    else:
        print("  Granted.")

    # DESCRIBE EXTENDED
    print(f"\n--- DESCRIBE EXTENDED {FQN} ---")
    r = run_sql(f"DESCRIBE EXTENDED {FQN}", wh_id)
    if "error" in r:
        print(f"  {r['error']}")
    else:
        for row in r.get("rows", []):
            cn = row.get("col_name", "")
            dt = row.get("data_type", "")
            if cn in ("Provider", "Location", "Type", "Table Properties", "Table"):
                print(f"  {cn}: {dt}")

    # SELECT *
    print(f"\n--- SELECT * FROM {FQN} ---")
    r = run_sql(f"SELECT * FROM {FQN} LIMIT 10", wh_id)
    if "error" in r:
        print(f"  {r['error']}")
    else:
        print(f"  Returned {len(r.get('rows', []))} rows:")
        for row in r.get("rows", []):
            print(f"    {row}")


if __name__ == "__main__":
    main()
