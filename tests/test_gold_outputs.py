import importlib.util
import sys
from decimal import Decimal
from pathlib import Path
from types import ModuleType

import pyspark
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
GOLD_PATH = ROOT / "src" / "synthetic_banking_etl" / "transformations" / "gold"


def _load_gold(monkeypatch, filename: str, module_name: str):
    pipelines = ModuleType("pyspark.pipelines")
    pipelines.materialized_view = lambda **kwargs: lambda function: function
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", pipelines)
    monkeypatch.setattr(pyspark, "pipelines", pipelines, raising=False)
    spec = importlib.util.spec_from_file_location(module_name, GOLD_PATH / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_monthly_kpis_reconcile_credit_and_debit(spark, monkeypatch):
    transactions = spark.createDataFrame(
        [
            ("CL-1", "2025-01-05", "card", "card_purchase", "D", "10.10"),
            ("CL-1", "2025-01-20", "online", "salary", "C", "6.25"),
        ],
        ["client_id_fk", "book_date", "channel", "txn_type", "dc_indicator", "amount_eur"],
    ).withColumn("book_date", F.to_date("book_date")).withColumn("amount_eur", F.col("amount_eur").cast("decimal(18,2)"))
    customers = spark.createDataFrame([("CL-1", "Private")], ["client_id", "segment"])
    transactions.createOrReplaceTempView("silver_transactions")
    customers.createOrReplaceTempView("silver_customers")
    module = _load_gold(monkeypatch, "monthly_transaction_kpis.py", "test_monthly_kpis")
    module.spark = spark

    rows = module.gold_monthly_transaction_kpis().collect()
    values = {row.dc_indicator: row for row in rows}

    assert len(rows) == 2
    assert values["D"].transaction_count == 1
    assert values["D"].debit_amount_eur == Decimal("10.10")
    assert values["C"].credit_amount_eur == Decimal("6.25")
    assert values["D"].net_amount_eur == Decimal("-10.10")


def test_customer_360_keeps_customer_grain_and_latest_snapshot(spark, monkeypatch):
    spark.createDataFrame([("CL-1", "Private"), ("CL-2", "SME")], ["client_id", "segment"]).createOrReplaceTempView(
        "silver_customers"
    )
    spark.createDataFrame(
        [("CL-1", "C", 5.0, "2025-01-01"), ("CL-1", "D", 8.0, "2025-01-02")],
        ["client_id_fk", "dc_indicator", "amount_eur", "book_date"],
    ).withColumn("book_date", F.to_date("book_date")).createOrReplaceTempView("silver_transactions")
    product_specs = [
        ("silver_leasing_agreements", "snapshot_date", ["outstanding_principal"]),
        ("silver_life_insurance", "snapshot_date", ["sum_insured"]),
        ("silver_property_insurance", "snapshot_date", ["premium_eur", "surrender_value_eur"]),
        ("silver_pension", "contrib_date", ["unit_qty"]),
        ("silver_margin_assets", "month_end", ["end_balance_eur"]),
        ("silver_margin_liabilities", "month_end", ["end_balance_eur"]),
    ]
    for table, date_column, amount_columns in product_specs:
        spark.createDataFrame(
            [("CL-1", "2025-01-31", *([1.0] * len(amount_columns))), ("CL-1", "2025-02-28", *([2.0] * len(amount_columns)))],
            ["client_id_fk", date_column, *amount_columns],
        ).withColumn(date_column, F.to_date(date_column)).createOrReplaceTempView(table)
    module = _load_gold(monkeypatch, "customer_product_360.py", "test_customer_360")
    module.spark = spark

    result = module.gold_customer_product_360()
    rows = result.collect()

    assert len(rows) == 2
    assert result.groupBy("client_id").count().where("count > 1").count() == 0
    customer = next(row for row in rows if row.client_id == "CL-1")
    assert customer.transaction_count == 2
    assert customer.silver_leasing_agreements_outstanding_principal == 2.0
