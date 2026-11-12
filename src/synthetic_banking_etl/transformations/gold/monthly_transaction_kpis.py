from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="gold_monthly_transaction_kpis")
def gold_monthly_transaction_kpis():
    transactions = spark.read.table("silver_transactions").alias("t")
    customers = spark.read.table("silver_customers").select("client_id", "segment").alias("c")
    joined = transactions.join(customers, F.col("t.client_id_fk") == F.col("c.client_id"), "inner")
    amount = F.col("t.amount_eur").cast("decimal(18,2)")
    return (
        joined.withColumn("month", F.trunc(F.col("t.book_date"), "month"))
        .groupBy("month", F.col("t.channel"), F.col("t.txn_type"), F.col("c.segment"), F.col("t.dc_indicator"))
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum(F.when(F.col("t.dc_indicator") == "D", amount).otherwise(F.lit(0))).alias("debit_amount_eur"),
            F.sum(F.when(F.col("t.dc_indicator") == "C", amount).otherwise(F.lit(0))).alias("credit_amount_eur"),
        )
        .withColumn("net_amount_eur", F.col("credit_amount_eur") - F.col("debit_amount_eur"))
    )
