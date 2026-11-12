from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _latest_by_customer(table: str, date_column: str, sums: list[str]):
    source = spark.read.table(table).alias("source")
    latest = source.groupBy("client_id_fk").agg(F.max(date_column).alias("_latest_date")).alias("latest")
    selected = source.join(
        latest,
        (F.col("source.client_id_fk") == F.col("latest.client_id_fk"))
        & (F.col(f"source.{date_column}") == F.col("latest._latest_date")),
        "inner",
    ).select("source.*")
    aggregations = [F.count("*").alias(f"{table}_row_count")]
    aggregations.extend(F.sum(column).alias(f"{table}_{column}") for column in sums)
    return selected.groupBy("client_id_fk").agg(*aggregations)


@dp.materialized_view(name="gold_customer_product_360")
def gold_customer_product_360():
    customers = spark.read.table("silver_customers")
    transactions = spark.read.table("silver_transactions").groupBy("client_id_fk").agg(
        F.count("*").alias("transaction_count"),
        F.sum(F.when(F.col("dc_indicator") == "C", F.col("amount_eur"))).alias("credit_amount_eur"),
        F.sum(F.when(F.col("dc_indicator") == "D", F.col("amount_eur"))).alias("debit_amount_eur"),
        F.max("book_date").alias("last_transaction_date"),
    )
    products = (
        ("silver_leasing_agreements", "snapshot_date", ["outstanding_principal"]),
        ("silver_life_insurance", "snapshot_date", ["sum_insured"]),
        ("silver_property_insurance", "snapshot_date", ["premium_eur", "surrender_value_eur"]),
        ("silver_pension", "contrib_date", ["unit_qty"]),
        ("silver_margin_assets", "month_end", ["end_balance_eur"]),
        ("silver_margin_liabilities", "month_end", ["end_balance_eur"]),
    )
    result = customers.join(transactions, customers.client_id == transactions.client_id_fk, "left").drop(
        transactions.client_id_fk
    )
    for table, date_column, sums in products:
        product = _latest_by_customer(table, date_column, sums)
        result = result.join(product, result.client_id == product.client_id_fk, "left").drop(product.client_id_fk)
    return result
