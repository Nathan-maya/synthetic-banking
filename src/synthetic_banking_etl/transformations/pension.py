from pyspark import pipelines as dp
from pyspark.sql import functions as F

from synthetic_banking_etl.quality_rules import (
    add_quality_flags,
    expectations_for,
    normalize_bronze_dates,
    normalize_silver,
    quarantine_rows,
)

DATASET = "pension"
BRONZE = f"bronze_{DATASET}"
QUALITY = f"quality_{DATASET}"
SILVER = f"silver_{DATASET}"
QUARANTINE = f"quarantine_{DATASET}"

@dp.table(name=BRONZE, comment="Raw Parquet input with source lineage")
def bronze_source():
    landing = spark.conf.get("landing_path").rstrip("/")
    frame = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(f"{landing}/{DATASET}/")
    )
    frame = normalize_bronze_dates(frame, DATASET)
    return (
        frame
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

@dp.materialized_view(name=QUALITY, private=True)
@dp.expect_all(expectations_for(DATASET))
def quality_source():
    source = spark.read.table(BRONZE)
    if DATASET == "customers":
        return add_quality_flags(source, DATASET)
    customers = spark.read.table("silver_customers").select("client_id")
    return add_quality_flags(source, DATASET, customers)

@dp.materialized_view(name=SILVER)
def silver_source():
    source = spark.read.table(QUALITY).where("_dq_valid")
    return normalize_silver(source, DATASET)

@dp.materialized_view(name=QUARANTINE)
def quarantine_source():
    return quarantine_rows(spark.read.table(QUALITY))

