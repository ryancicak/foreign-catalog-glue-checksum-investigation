#!/usr/bin/env python3
"""
Optional: Set up a Databricks foreign catalog for Glue federation.

Creates:
  1. Storage credential (IAM role for S3 access)
  2. Service credential (IAM role for Glue connection)
  3. External location (S3 bucket mapping)
  4. Glue connection
  5. Federated catalog
  6. Lake Formation permissions (so tables actually show up)

Only run this if you don't already have a foreign catalog set up.
Reads everything from .env.
"""

import os
import sys
import json
from pathlib import Path
from dotenv import dotenv_values

import boto3
from databricks.sdk import WorkspaceClient

PROJECT_DIR = Path(__file__).resolve().parent.parent
env = dotenv_values(PROJECT_DIR / ".env")

# Validate required vars
REQUIRED = [
    "DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_SECRET",
    "S3_BUCKET", "GLUE_DATABASE", "FOREIGN_CATALOG", "AWS_REGION", "IAM_ROLE_ARN",
]
missing = [v for v in REQUIRED if not env.get(v)]
if missing:
    print("Missing from .env: " + ", ".join(missing))
    if "IAM_ROLE_ARN" in missing:
        print("\nIAM_ROLE_ARN is required for foreign catalog setup.")
        print("This is a cross-account IAM role that Databricks UC can assume.")
        print("It needs: S3 read on your bucket, Glue read access, and a trust")
        print("policy allowing the Databricks UC AWS account to assume it.")
    sys.exit(1)

HOST = env["DATABRICKS_HOST"].rstrip("/")
CLIENT_ID = env["DATABRICKS_CLIENT_ID"]
CLIENT_SECRET = env["DATABRICKS_CLIENT_SECRET"]
S3_BUCKET = env["S3_BUCKET"]
GLUE_DATABASE = env["GLUE_DATABASE"]
FOREIGN_CATALOG = env["FOREIGN_CATALOG"]
AWS_REGION = env["AWS_REGION"]
IAM_ROLE_ARN = env["IAM_ROLE_ARN"]
PREFIX = FOREIGN_CATALOG  # use the catalog name as prefix for resource naming


# ---------------------------------------------------------------------------
# Databricks UC setup
# ---------------------------------------------------------------------------

def setup_databricks():
    """Create all Databricks UC resources for Glue federation."""
    print("=" * 60)
    print("DATABRICKS UNITY CATALOG FEDERATION SETUP")
    print("=" * 60)
    print(f"  Host:     {HOST}")
    print(f"  Bucket:   {S3_BUCKET}")
    print(f"  IAM Role: {IAM_ROLE_ARN}")
    print(f"  Catalog:  {FOREIGN_CATALOG}")
    print()

    w = WorkspaceClient(
        host=HOST,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    # Get AWS account ID from the IAM role ARN
    aws_account_id = IAM_ROLE_ARN.split(":")[4]

    storage_cred_name = f"{PREFIX}_storage_cred"
    service_cred_name = f"{PREFIX}_service_cred"
    ext_loc_name = f"{PREFIX}_ext_loc"
    conn_name = f"{PREFIX}_glue_conn"
    s3_url = f"s3://{S3_BUCKET}/"

    # 1. Storage credential
    print(f"--- Storage credential: {storage_cred_name} ---")
    try:
        w.storage_credentials.delete(storage_cred_name, force=True)
    except:
        pass
    try:
        from databricks.sdk.service.catalog import AwsIamRoleRequest
        w.storage_credentials.create(
            name=storage_cred_name,
            aws_iam_role=AwsIamRoleRequest(role_arn=IAM_ROLE_ARN),
            comment="Storage credential for foreign catalog investigation",
        )
        print("  Created.")
    except Exception as e:
        print(f"  Failed: {e}")
        print("  Continuing anyway (might already exist)...")

    # 2. Service credential (must be PURPOSE=SERVICE for Glue connections)
    print(f"\n--- Service credential: {service_cred_name} ---")
    try:
        # SDK doesn't have a clean delete for service credentials, use API
        import requests
        headers = {}
        # Get a token via the workspace client's config
        token = w.config.authenticate()
        # Use requests directly for service credential (SDK support varies)
        api_base = f"{HOST}/api/2.1/unity-catalog"
        auth_headers = {"Authorization": f"Bearer {w.config.authenticate()}"} if hasattr(w.config, 'authenticate') else {}
    except:
        pass

    # Use requests for service credential since SDK support varies by version
    import requests
    def get_auth_headers():
        """Get auth headers from the workspace client."""
        # Use OAuth2 client credentials directly
        token_resp = requests.post(
            f"{HOST}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope": "all-apis",
            },
        )
        token_resp.raise_for_status()
        token = token_resp.json()["access_token"]
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    headers = get_auth_headers()
    api_base = f"{HOST}/api/2.1/unity-catalog"

    # Delete if exists
    requests.delete(f"{api_base}/credentials/{service_cred_name}?force=true", headers=headers)

    resp = requests.post(f"{api_base}/credentials", headers=headers, json={
        "name": service_cred_name,
        "purpose": "SERVICE",
        "aws_iam_role": {"role_arn": IAM_ROLE_ARN},
        "comment": "Service credential for Glue connection",
    })
    if resp.status_code in (200, 201):
        print("  Created.")
    else:
        print(f"  Response ({resp.status_code}): {resp.text[:200]}")
        print("  Continuing anyway...")

    # 3. External location
    print(f"\n--- External location: {ext_loc_name} ---")
    try:
        w.external_locations.delete(ext_loc_name, force=True)
    except:
        pass
    try:
        w.external_locations.create(
            name=ext_loc_name,
            url=s3_url,
            credential_name=storage_cred_name,
            skip_validation=True,
            comment="External location for foreign catalog investigation",
        )
        print("  Created.")
    except Exception as e:
        print(f"  Failed: {e}")
        print("  Continuing anyway...")

    # 4. Glue connection
    print(f"\n--- Glue connection: {conn_name} ---")
    requests.delete(f"{api_base}/connections/{conn_name}", headers=headers)
    resp = requests.post(f"{api_base}/connections", headers=headers, json={
        "name": conn_name,
        "connection_type": "GLUE",
        "options": {
            "aws_region": AWS_REGION,
            "aws_account_id": aws_account_id,
            "credential": service_cred_name,
        },
        "comment": "Glue connection for foreign catalog investigation",
    })
    if resp.status_code in (200, 201):
        print("  Created.")
    else:
        print(f"  Response ({resp.status_code}): {resp.text[:200]}")
        print("  Continuing anyway...")

    # 5. Federated catalog
    print(f"\n--- Federated catalog: {FOREIGN_CATALOG} ---")
    requests.delete(f"{api_base}/catalogs/{FOREIGN_CATALOG}?force=true", headers=headers)
    resp = requests.post(f"{api_base}/catalogs", headers=headers, json={
        "name": FOREIGN_CATALOG,
        "connection_name": conn_name,
        "options": {"authorized_paths": s3_url},
        "comment": "Foreign catalog for Glue federation investigation",
    })
    if resp.status_code in (200, 201):
        print("  Created.")
    else:
        print(f"  Response ({resp.status_code}): {resp.text[:200]}")
        print("  Continuing anyway...")

    # 6. Grant permissions
    print(f"\n--- Granting permissions ---")
    for securable, name, principal, perms in [
        ("storage_credential", storage_cred_name, "account users", ["CREATE_EXTERNAL_LOCATION", "READ_FILES"]),
        ("external_location", ext_loc_name, "account users", ["READ_FILES"]),
    ]:
        resp = requests.patch(
            f"{api_base}/permissions/{securable}/{name}",
            headers=headers,
            json={"changes": [{"principal": principal, "add": perms}]},
        )
        if resp.status_code == 200:
            print(f"  Granted {perms} on {securable}/{name}")
        else:
            print(f"  Grant on {securable}/{name}: {resp.status_code} {resp.text[:100]}")

    print()
    return True


# ---------------------------------------------------------------------------
# Lake Formation setup
# ---------------------------------------------------------------------------

def setup_lake_formation():
    """Grant Lake Formation permissions so tables show up in Databricks."""
    print("=" * 60)
    print("LAKE FORMATION PERMISSIONS")
    print("=" * 60)

    lf = boto3.client("lakeformation", region_name=AWS_REGION)

    # Get the role name from the ARN for IAM policy updates
    role_name = IAM_ROLE_ARN.split("/")[-1]

    # 1. Grant catalog DESCRIBE
    print(f"\n--- Catalog DESCRIBE for {IAM_ROLE_ARN} ---")
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": IAM_ROLE_ARN},
            Resource={"Catalog": {}},
            Permissions=["DESCRIBE"],
        )
        print("  Granted.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Already exists.")
        else:
            print(f"  {e}")

    # 2. Grant database ALL
    print(f"\n--- Database ALL on {GLUE_DATABASE} ---")
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": IAM_ROLE_ARN},
            Resource={"Database": {"Name": GLUE_DATABASE}},
            Permissions=["ALL"],
        )
        print("  Granted.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Already exists.")
        else:
            print(f"  {e}")

    # 3. Grant on 'default' database (Databricks checks this)
    print(f"\n--- Database DESCRIBE on 'default' ---")
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": IAM_ROLE_ARN},
            Resource={"Database": {"Name": "default"}},
            Permissions=["DESCRIBE"],
        )
        print("  Granted.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Already exists.")
        else:
            print(f"  {e}")

    # 4. Opt database out of Lake Formation (IAM_ALLOWED_PRINCIPALS)
    #    This is the big one. Without this, tables exist in Glue but
    #    Databricks can't see them through the foreign catalog.
    print(f"\n--- Opt-out: IAM_ALLOWED_PRINCIPALS on {GLUE_DATABASE} ---")
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Database": {"Name": GLUE_DATABASE}},
            Permissions=["ALL"],
        )
        print("  Granted.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Already exists.")
        else:
            print(f"  {e}")

    # 5. Opt tables out too (wildcard)
    print(f"\n--- Opt-out: IAM_ALLOWED_PRINCIPALS on all tables in {GLUE_DATABASE} ---")
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Table": {"DatabaseName": GLUE_DATABASE, "TableWildcard": {}}},
            Permissions=["ALL"],
        )
        print("  Granted.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  Already exists.")
        else:
            print(f"  {e}")

    print()
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print()
    success = True
    success &= setup_databricks()
    success &= setup_lake_formation()

    print("=" * 60)
    if success:
        print("FOREIGN CATALOG SETUP COMPLETE")
    else:
        print("SETUP COMPLETED WITH WARNINGS (check output above)")
    print("=" * 60)
    print(f"  Catalog name: {FOREIGN_CATALOG}")
    print(f"  Test query:   SHOW TABLES IN {FOREIGN_CATALOG}.{GLUE_DATABASE}")
    print()


if __name__ == "__main__":
    main()
