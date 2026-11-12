from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

RULES: dict[str, dict[str, str]] = {
    "customers": {
        "client_id_required": "client_id IS NOT NULL",
        "client_id_unique": "NOT _dq_duplicate_key",
        "customer_segment": "segment IN ('Private', 'SME', 'Corporate')",
        "gender_domain": "gender_cd IN ('M', 'F', 'U')",
        "region_domain": "region_cd IN ('TLN', 'TRT', 'NRV', 'PRN', 'RUR', 'OTH')",
        "registration_dates": "birth_or_reg_dt IS NOT NULL AND bank_reg_dt IS NOT NULL AND birth_or_reg_dt <= bank_reg_dt",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "transactions": {
        "txn_id_required": "txn_id IS NOT NULL",
        "txn_id_unique": "NOT _dq_duplicate_key",
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "amounts_required": "amount_orig IS NOT NULL AND amount_eur IS NOT NULL",
        "currency_domain": "trx_ccy IN ('EUR', 'USD', 'SEK', 'NOK', 'GBP')",
        "direction_domain": "dc_indicator IN ('C', 'D')",
        "channel_domain": "channel IN ('card', 'online', 'mobile', 'branch', 'atm')",
        "txn_type_required": "txn_type IS NOT NULL",
        "date_consistency": "book_date IS NOT NULL AND period_yy_mm = date_format(book_date, 'yyMM') AND day_in_month = dayofmonth(book_date)",
        "eur_amount_matches_original": "trx_ccy <> 'EUR' OR ABS(amount_eur - amount_orig) < 0.01",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "leasing_agreements": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "snapshot_required": "snapshot_date IS NOT NULL",
        "status_domain": "lease_status IN ('offer', 'enactment', 'valid', 'temporary terminated', 'terminated', 'completely terminated', 'archived')",
        "principal_nonnegative": "orig_principal >= 0 AND outstanding_principal >= 0 AND carrying_amount >= 0",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "life_insurance": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "snapshot_required": "snapshot_date IS NOT NULL",
        "status_domain": "policy_status IN ('ALG', 'REGTUD', 'MAKSEOOT', 'JOUS', 'KATK', 'LOPET', 'LOPPKAES', 'LOPPSURM', 'ANULL')",
        "amounts_nonnegative": "sum_insured >= 0 AND next_premium >= 0",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "property_insurance": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "snapshot_required": "snapshot_date IS NOT NULL",
        "agreement_status_domain": "CAST(agreement_status AS INT) IN (-1, 0, 1, 2)",
        "cover_type_domain": "cover_type_cd IN ('E', 'P')",
        "amounts_nonnegative": "premium_eur >= 0 AND surrender_value_eur >= 0",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "pension": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "contribution_date_required": "contrib_date IS NOT NULL",
        "fund_domain": "fund_code IN ('PEN-A', 'PEN-B', 'PEN-C', 'PEN-D', 'PEN-E')",
        "units_nonnegative": "unit_qty >= 0",
        "status_domain": "contrib_status IN ('active', 'stopped', '')",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "margin_assets": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "month_end_required": "month_end IS NOT NULL",
        "positive_balance": "end_balance_eur > 0",
        "interest_base_valid": "avg_balance_eur >= 0 AND avg_interest_base_eur >= 0 AND avg_interest_base_eur <= avg_balance_eur",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
    "margin_liabilities": {
        "client_id_required": "client_id_fk IS NOT NULL",
        "customer_fk_exists": "_ref_client_id IS NOT NULL",
        "month_end_required": "month_end IS NOT NULL",
        "positive_balance": "end_balance_eur > 0",
        "interest_base_valid": "avg_balance_eur >= 0 AND avg_interest_base_eur >= 0 AND avg_interest_base_eur <= avg_balance_eur",
        "schema_not_rescued": "_rescued_data IS NULL",
    },
}

PRIMARY_KEYS = {"customers": ["client_id"], "transactions": ["txn_id"]}
DATE_COLUMNS = {
    "customers": ["birth_or_reg_dt", "bank_reg_dt"],
    "transactions": ["book_date"],
    "leasing_agreements": ["snapshot_date"],
    "life_insurance": ["snapshot_date"],
    "property_insurance": ["snapshot_date"],
    "pension": ["contrib_date"],
    "margin_assets": ["month_end"],
    "margin_liabilities": ["month_end"],
}
MONEY_COLUMNS = {
    "transactions": ["amount_orig", "amount_eur"],
    "leasing_agreements": ["orig_principal", "outstanding_principal", "carrying_amount"],
    "life_insurance": ["sum_insured", "next_premium"],
    "property_insurance": ["premium_eur", "surrender_value_eur"],
    "margin_assets": ["end_balance_eur", "avg_balance_eur", "avg_interest_base_eur"],
    "margin_liabilities": ["end_balance_eur", "avg_balance_eur", "avg_interest_base_eur"],
}


def expectations_for(dataset: str) -> dict[str, str]:
    return RULES[dataset]


def normalize_bronze_dates(df: DataFrame, dataset: str) -> DataFrame:
    normalized = df
    for column in DATE_COLUMNS[dataset]:
        normalized = normalized.withColumn(column, F.col(column).cast("string"))
    return normalized


def add_quality_flags(df: DataFrame, dataset: str, reference_ids: DataFrame | None = None) -> DataFrame:
    checked = df
    primary_key = PRIMARY_KEYS.get(dataset)
    if primary_key:
        duplicates = (
            checked.groupBy(*primary_key)
            .count()
            .where(F.col("count") > 1)
            .select(*primary_key)
            .withColumn("_dq_duplicate_key", F.lit(True))
        )
        checked = checked.join(duplicates, primary_key, "left").withColumn(
            "_dq_duplicate_key", F.coalesce(F.col("_dq_duplicate_key"), F.lit(False))
        )
    else:
        checked = checked.withColumn("_dq_duplicate_key", F.lit(False))
    if reference_ids is not None:
        keys = reference_ids.select(F.col("client_id").alias("_ref_client_id")).dropDuplicates()
        checked = checked.join(F.broadcast(keys), F.col("client_id_fk") == F.col("_ref_client_id"), "left")
    predicates = {
        name: F.coalesce(F.expr(expression), F.lit(False))
        for name, expression in RULES[dataset].items()
    }
    failures = [F.when(~condition, F.lit(name)) for name, condition in predicates.items()]
    return checked.withColumn("_failed_rules", F.array_compact(F.array(*failures))).withColumn(
        "_dq_valid", F.size("_failed_rules") == 0
    )


def quarantine_rows(df: DataFrame) -> DataFrame:
    return df.where("NOT _dq_valid").withColumn("_quarantined_at", F.current_timestamp())


def normalize_silver(df: DataFrame, dataset: str) -> DataFrame:
    normalized = df
    for column in DATE_COLUMNS[dataset]:
        normalized = normalized.withColumn(column, F.col(column).cast("date"))
    for column in MONEY_COLUMNS.get(dataset, []):
        normalized = normalized.withColumn(column, F.round(F.col(column), 2).cast("decimal(18,2)"))
    if dataset == "transactions":
        normalized = normalized.withColumn("amount_eur", F.round("amount_eur", 2).cast("decimal(18,2)"))
        normalized = normalized.withColumn("amount_orig", F.round("amount_orig", 2).cast("decimal(18,2)"))
    return normalized.drop("_failed_rules", "_dq_valid", "_dq_duplicate_key", "_ref_client_id", "_rescued_data")
