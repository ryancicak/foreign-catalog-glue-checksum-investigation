#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$PROJECT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Missing .env file. Copy .env.example to .env and fill it in."
    exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

# Validate required vars
for var in EMR_HOST EMR_PEM_PATH S3_BUCKET GLUE_DATABASE TABLE_NAME AWS_REGION; do
    if [[ -z "${!var:-}" ]]; then
        echo "Missing $var in .env"
        exit 1
    fi
done

EMR_PEM_PATH="${EMR_PEM_PATH/#\~/$HOME}"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=15 -i $EMR_PEM_PATH"
EMR_USER="hadoop"
S3_LOCATION="s3://$S3_BUCKET/$GLUE_DATABASE/$TABLE_NAME"

# Check if the user needs a foreign catalog set up
echo ""
read -rp "Do you already have a foreign catalog set up in Databricks? (y/n) " HAS_CATALOG
if [[ "$HAS_CATALOG" != "y" && "$HAS_CATALOG" != "Y" ]]; then
    echo ""
    echo "Setting up foreign catalog in Databricks..."
    echo "(This creates storage/service credentials, external location,"
    echo " Glue connection, federated catalog, and Lake Formation permissions.)"
    echo ""
    python3 "$SCRIPT_DIR/setup_foreign_catalog.py"
    echo ""
fi

echo "============================================================"
echo " Reproduce: DELTA_UNIVERSAL_FORMAT_CONVERSION_FAILED"
echo "============================================================"
echo "  EMR host:     $EMR_HOST"
echo "  S3 location:  $S3_LOCATION"
echo "  Glue table:   $GLUE_DATABASE.$TABLE_NAME"
echo ""

# Test SSH
echo "Testing SSH connection..."
ssh $SSH_OPTS "$EMR_USER@$EMR_HOST" "echo 'SSH OK'" 2>/dev/null
echo ""

# Generate the PySpark script with templated values
PYSPARK_SCRIPT=$(mktemp /tmp/reproduce_XXXXXX.py)
cat > "$PYSPARK_SCRIPT" << 'PYSPARK_EOF'
import boto3
import sys

BUCKET = "@@BUCKET@@"
DB = "@@DATABASE@@"
TABLE = "@@TABLE@@"
S3_LOCATION = "s3://" + BUCKET + "/" + DB + "/" + TABLE
REGION = "@@REGION@@"

s3 = boto3.client("s3", region_name=REGION)
glue = boto3.client("glue", region_name=REGION)

# --- Create Glue database if missing ---
print("=" * 60)
print("STEP 0: Ensure Glue database exists")
print("=" * 60)
try:
    glue.get_database(Name=DB)
    print("  Database " + DB + " exists.")
except glue.exceptions.EntityNotFoundException:
    print("  Creating database " + DB + "...")
    glue.create_database(DatabaseInput={"Name": DB})
    print("  Created.")

# --- Cleanup previous run ---
print("")
print("=" * 60)
print("STEP 1: Cleanup previous test data")
print("=" * 60)
paginator = s3.get_paginator("list_objects_v2")
deleted = 0
for page in paginator.paginate(Bucket=BUCKET, Prefix=DB + "/" + TABLE):
    if "Contents" in page:
        for obj in page["Contents"]:
            s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
            deleted += 1
print("  Deleted " + str(deleted) + " objects from S3.")
try:
    glue.delete_table(DatabaseName=DB, Name=TABLE)
    print("  Dropped Glue table " + DB + "." + TABLE)
except:
    pass
print("  Clean.")

# --- Write Hive parquet data ---
print("")
print("=" * 60)
print("STEP 2: Write Hive/Parquet data to S3")
print("=" * 60)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

spark = (SparkSession.builder
    .appName("reproduce_step2_hive_write")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.hadoop.mapreduce.output.fs.optimized.committer.enabled", "false")
    .getOrCreate())

schema = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("created_at", StringType()),
])

data = [(i, "Customer_" + str(i), "c" + str(i) + "@hive.com", "2025-" + str((i % 12) + 1).zfill(2) + "-01")
        for i in range(1, 11)]
df = spark.createDataFrame(data, schema)
df.repartition(2).write.mode("overwrite").parquet(S3_LOCATION)
spark.stop()
print("  Wrote 10 rows as Hive/Parquet (2 files).")

print("  S3 files:")
for page in paginator.paginate(Bucket=BUCKET, Prefix=DB + "/" + TABLE):
    if "Contents" in page:
        for obj in page["Contents"]:
            print("    " + obj["Key"] + " (" + str(obj["Size"]) + " bytes)")

# --- Create Iceberg table and adopt files ---
print("")
print("=" * 60)
print("STEP 3: Create Iceberg table in Glue + add_files")
print("=" * 60)

spark2 = (SparkSession.builder
    .appName("reproduce_step3_iceberg")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_iceberg.warehouse", "s3://" + BUCKET)
    .config("spark.sql.catalog.glue_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_iceberg.region", REGION)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog", "glue_iceberg")
    .getOrCreate())

fqn = "glue_iceberg." + DB + "." + TABLE

spark2.sql("CREATE TABLE " + fqn + " (id BIGINT, name STRING, email STRING, created_at STRING) USING iceberg LOCATION '" + S3_LOCATION + "'")
print("  Created Iceberg table.")

print("  Running add_files to adopt Hive data into Iceberg metadata...")
result = spark2.sql("CALL glue_iceberg.system.add_files(table => '" + DB + "." + TABLE + "', source_table => '`parquet`.`" + S3_LOCATION + "`')")
result.show(truncate=False)

print("  Iceberg table data:")
spark2.sql("SELECT * FROM " + fqn).show()

print("  Iceberg tracked files:")
spark2.sql("SELECT file_path, file_size_in_bytes, record_count FROM " + fqn + ".files").show(truncate=False)

spark2.stop()

# --- Summary ---
print("")
print("=" * 60)
print("DONE")
print("=" * 60)
print("  S3 location: " + S3_LOCATION)
total = 0
for page in paginator.paginate(Bucket=BUCKET, Prefix=DB + "/" + TABLE):
    if "Contents" in page:
        for obj in page["Contents"]:
            total += obj["Size"]
            print("    " + obj["Key"] + " (" + str(obj["Size"]) + " bytes)")
print("  Total S3 size: " + str(total) + " bytes")

info = glue.get_table(DatabaseName=DB, Name=TABLE)
params = info["Table"].get("Parameters", {})
print("  Glue table_type: " + params.get("table_type", "N/A"))
print("  metadata_location: " + params.get("metadata_location", "N/A"))
print("")
print("  Now run: python scripts/query_from_databricks.py")
PYSPARK_EOF

# Template in the .env values
sed -i.bak "s|@@BUCKET@@|$S3_BUCKET|g" "$PYSPARK_SCRIPT"
sed -i.bak "s|@@DATABASE@@|$GLUE_DATABASE|g" "$PYSPARK_SCRIPT"
sed -i.bak "s|@@TABLE@@|$TABLE_NAME|g" "$PYSPARK_SCRIPT"
sed -i.bak "s|@@REGION@@|$AWS_REGION|g" "$PYSPARK_SCRIPT"
rm -f "${PYSPARK_SCRIPT}.bak"

# Upload and run on EMR
echo "Uploading PySpark script to EMR..."
scp $SSH_OPTS "$PYSPARK_SCRIPT" "$EMR_USER@$EMR_HOST:/tmp/reproduce.py" 2>/dev/null

echo "Running on EMR via spark-submit..."
echo ""
ssh $SSH_OPTS "$EMR_USER@$EMR_HOST" \
    'spark-submit --master yarn --deploy-mode client --conf spark.sql.catalogImplementation=in-memory --conf spark.sql.defaultCatalog=spark_catalog --jars /usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar /tmp/reproduce.py 2>&1 | grep -v "^26/" | grep -v "^SLF4J" | grep -v "^Output:"'

rm -f "$PYSPARK_SCRIPT"
echo ""
echo "Done. Table is ready to query from Databricks."
