from datetime import date, datetime

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

from synthetic_banking_etl.quality_rules import add_quality_flags, quarantine_rows


def test_customer_quality_flags_split_valid_rows_and_quarantine(spark):
    schema = StructType(
        [
            StructField("client_id", StringType()),
            StructField("segment", StringType()),
            StructField("gender_cd", StringType()),
            StructField("region_cd", StringType()),
            StructField("birth_or_reg_dt", DateType()),
            StructField("bank_reg_dt", DateType()),
            StructField("_rescued_data", StringType()),
            StructField("_source_file", StringType()),
            StructField("_ingested_at", TimestampType()),
        ]
    )
    rows = [
        ("CL-1", "Private", "F", "TLN", date(1980, 1, 1), date(2020, 1, 1), None, "customers/a.parquet", datetime(2026, 1, 1)),
        ("CL-2", "Private", "X", "TLN", date(1980, 1, 1), date(2020, 1, 1), None, "customers/a.parquet", datetime(2026, 1, 1)),
        ("CL-3", "Private", "M", "TLN", date(2022, 1, 1), date(2020, 1, 1), None, "customers/b.parquet", datetime(2026, 1, 1)),
    ]
    source = spark.createDataFrame(rows, schema)

    checked = add_quality_flags(source, "customers")
    valid = checked.where("_dq_valid")
    quarantined = quarantine_rows(checked)

    assert [row.client_id for row in valid.collect()] == ["CL-1"]
    failures = {row.client_id: row._failed_rules for row in quarantined.collect()}
    assert "gender_domain" in failures["CL-2"]
    assert "registration_dates" in failures["CL-3"]
    assert all(row._source_file for row in quarantined.collect())
    assert quarantined.where(F.col("_quarantined_at").isNull()).count() == 0


def test_transaction_quality_flags_orphan_customer(spark):
    schema = StructType(
        [
            StructField("txn_id", StringType()),
            StructField("client_id_fk", StringType()),
            StructField("amount_orig", DoubleType()),
            StructField("amount_eur", DoubleType()),
            StructField("trx_ccy", StringType()),
            StructField("dc_indicator", StringType()),
            StructField("channel", StringType()),
            StructField("txn_type", StringType()),
            StructField("book_date", DateType()),
            StructField("period_yy_mm", StringType()),
            StructField("day_in_month", IntegerType()),
            StructField("_rescued_data", StringType()),
        ]
    )
    transactions = spark.createDataFrame(
        [
            ("TX-1", "CL-1", 10.0, 10.0, "EUR", "D", "card", "purchase", date(2025, 1, 1), "2501", 1, None),
            ("TX-2", "CL-MISSING", 5.0, 5.0, "EUR", "C", "online", "salary", date(2025, 1, 2), "2501", 2, None),
        ],
        schema,
    )
    customers = spark.createDataFrame([("CL-1",)], ["client_id"])

    checked = add_quality_flags(transactions, "transactions", customers)

    assert checked.where("_dq_valid").select("txn_id").collect()[0].txn_id == "TX-1"
    rejected = checked.where("NOT _dq_valid").first()
    assert "customer_fk_exists" in rejected._failed_rules
