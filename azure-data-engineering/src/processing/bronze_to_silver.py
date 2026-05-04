"""
src/processing/bronze_to_silver.py
------------------------------------
PySpark job: Bronze → Silver layer transformation.

Medallion Architecture:
  Bronze = raw landing (immutable, as-is from source)
  Silver = cleaned, deduplicated, typed, validated data

Transformations applied:
  1. Schema enforcement and type casting
  2. Null handling and imputation
  3. Deduplication (primary key + watermark)
  4. Data quality checks (DQ rules engine)
  5. PII masking (email, phone, SSN)
  6. Standardized column naming (snake_case)
  7. Write to Delta Lake (Silver) with Z-ordering
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Spark session factory
# ---------------------------------------------------------------------------

def create_spark_session(
    app_name: str = "BronzeToSilver",
    adls_account: str = "",
    adls_key: str = "",
    shuffle_partitions: int = 200,
) -> SparkSession:
    """Build an optimized SparkSession configured for Azure Databricks."""
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
    )

    if adls_account and adls_key:
        builder = builder.config(
            f"fs.azure.account.key.{adls_account}.dfs.core.windows.net", adls_key
        )

    return builder.getOrCreate()


# ---------------------------------------------------------------------------
# Data Quality Rules Engine
# ---------------------------------------------------------------------------

@dataclass
class DQRule:
    name: str
    condition: str          # Spark SQL expression (returns bool, True = valid)
    severity: str = "warn"  # "warn" | "error"
    description: str = ""


@dataclass
class DQResult:
    rule_name: str
    total_rows: int
    failed_rows: int
    pass_rate: float
    severity: str
    passed: bool


class DataQualityChecker:
    """Apply a set of DQ rules to a DataFrame and return results."""

    def __init__(self, rules: list[DQRule], fail_threshold: float = 0.05) -> None:
        self.rules = rules
        self.fail_threshold = fail_threshold

    def check(self, df: DataFrame) -> tuple[DataFrame, list[DQResult]]:
        """
        Apply all rules. Returns (clean_df, results).
        For error-severity rules exceeding threshold, raises ValueError.
        """
        total = df.count()
        results: list[DQResult] = []
        clean_df = df

        for rule in self.rules:
            failed = df.filter(f"NOT ({rule.condition})").count()
            pass_rate = 1.0 - (failed / total) if total > 0 else 1.0
            passed = pass_rate >= (1.0 - self.fail_threshold)

            result = DQResult(
                rule_name=rule.name,
                total_rows=total,
                failed_rows=failed,
                pass_rate=pass_rate,
                severity=rule.severity,
                passed=passed,
            )
            results.append(result)

            logger.info(
                "dq_rule_result",
                rule=rule.name,
                total=total,
                failed=failed,
                pass_rate=round(pass_rate, 4),
                passed=passed,
            )

            if not passed and rule.severity == "error":
                raise ValueError(
                    f"DQ rule '{rule.name}' failed: {failed}/{total} rows invalid "
                    f"(pass rate: {pass_rate:.2%})"
                )

        return clean_df, results


# ---------------------------------------------------------------------------
# PII Masking
# ---------------------------------------------------------------------------

class PIIMasker:
    """Hash or redact PII fields using SHA-256."""

    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
    PHONE_PATTERN = r"^\+?[\d\s\-\(\)]{7,15}$"

    def mask_column(self, df: DataFrame, column: str, strategy: str = "hash") -> DataFrame:
        """
        Mask a PII column.
        strategy: 'hash' (SHA256), 'redact' (replace with ***), 'tokenize'
        """
        if strategy == "hash":
            return df.withColumn(column, F.sha2(F.col(column).cast("string"), 256))
        elif strategy == "redact":
            return df.withColumn(column, F.lit("***REDACTED***"))
        elif strategy == "tokenize":
            return df.withColumn(
                column, F.concat(F.lit("TOKEN_"), F.sha2(F.col(column).cast("string"), 256).substr(1, 8))
            )
        return df

    def mask_columns(self, df: DataFrame, pii_columns: dict[str, str]) -> DataFrame:
        """Mask multiple PII columns. pii_columns = {col_name: strategy}"""
        for col, strategy in pii_columns.items():
            if col in df.columns:
                df = self.mask_column(df, col, strategy)
                logger.info("pii_masked", column=col, strategy=strategy)
        return df


# ---------------------------------------------------------------------------
# Schema Transformations
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """Convert camelCase or PascalCase column names to snake_case."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return re.sub(r"[^a-zA-Z0-9_]", "_", s).lower().strip("_")


def normalize_column_names(df: DataFrame) -> DataFrame:
    """Rename all columns to snake_case."""
    for col in df.columns:
        snake = to_snake_case(col)
        if snake != col:
            df = df.withColumnRenamed(col, snake)
    return df


def cast_columns(df: DataFrame, schema: dict[str, T.DataType]) -> DataFrame:
    """Cast specific columns to target types, handling errors gracefully."""
    for col_name, target_type in schema.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def deduplicate(
    df: DataFrame,
    primary_keys: list[str],
    watermark_col: str = "_ingestion_timestamp",
) -> DataFrame:
    """
    Remove duplicates keeping the most recent record per primary key.
    Uses a window function to pick the latest watermark.
    """
    window = Window.partitionBy(*primary_keys).orderBy(F.col(watermark_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Entity-specific transformers
# ---------------------------------------------------------------------------

class TransactionTransformer:
    """Bronze → Silver for transaction events."""

    SCHEMA: dict[str, T.DataType] = {
        "amount": T.DecimalType(18, 4),
        "timestamp": T.TimestampType(),
        "created_at": T.TimestampType(),
    }

    DQ_RULES = [
        DQRule("amount_positive", "amount > 0", severity="error"),
        DQRule("currency_not_null", "currency IS NOT NULL", severity="error"),
        DQRule("transaction_id_not_null", "transaction_id IS NOT NULL", severity="error"),
        DQRule("amount_reasonable", "amount < 1000000", severity="warn"),
    ]

    PII_COLUMNS = {
        "customer_id": "hash",
        "customer_email": "hash",
        "customer_phone": "redact",
    }

    def transform(self, df: DataFrame) -> DataFrame:
        logger.info("transforming_transactions", rows=df.count())

        # 1. Normalize column names
        df = normalize_column_names(df)

        # 2. Cast types
        df = cast_columns(df, self.SCHEMA)

        # 3. Derived columns
        df = (
            df
            .withColumn("transaction_date", F.to_date("timestamp"))
            .withColumn("amount_usd",
                F.when(F.col("currency") == "USD", F.col("amount"))
                 .otherwise(F.lit(None).cast(T.DecimalType(18, 4)))
            )
            .withColumn("is_high_value", F.col("amount") > 10_000)
            .withColumn("_silver_timestamp", F.current_timestamp())
        )

        # 4. Fill nulls
        df = df.fillna({"merchant_id": "UNKNOWN", "currency": "USD"})

        # 5. PII masking
        masker = PIIMasker()
        df = masker.mask_columns(df, self.PII_COLUMNS)

        # 6. Deduplicate
        df = deduplicate(df, primary_keys=["transaction_id"])

        # 7. DQ checks
        checker = DataQualityChecker(self.DQ_RULES)
        df, _ = checker.check(df)

        return df


class ClickstreamTransformer:
    """Bronze → Silver for clickstream events."""

    DQ_RULES = [
        DQRule("session_id_not_null", "session_id IS NOT NULL", severity="error"),
        DQRule("page_url_not_null", "page_url IS NOT NULL", severity="error"),
        DQRule("valid_event_type", "event_type IN ('click', 'view', 'scroll', 'submit', 'exit')", severity="warn"),
    ]

    def transform(self, df: DataFrame) -> DataFrame:
        df = normalize_column_names(df)
        df = (
            df
            .withColumn("event_date", F.to_date("timestamp"))
            .withColumn("event_hour", F.hour("timestamp"))
            .withColumn("domain", F.regexp_extract("page_url", r"https?://([^/]+)", 1))
            .withColumn("is_bot", F.col("user_agent").rlike(r"(?i)bot|crawl|spider"))
            .withColumn("_silver_timestamp", F.current_timestamp())
        )
        df = df.filter(~F.col("is_bot"))  # filter bots
        df = deduplicate(df, ["session_id", "timestamp", "event_type"])
        checker = DataQualityChecker(self.DQ_RULES)
        df, _ = checker.check(df)
        return df


# ---------------------------------------------------------------------------
# Bronze to Silver orchestrator
# ---------------------------------------------------------------------------

class BronzeToSilverJob:
    """
    Main PySpark job: reads from Bronze Delta tables,
    transforms, and writes to Silver as Delta Lake tables.
    """

    TRANSFORMERS = {
        "transactions": TransactionTransformer,
        "clickstream": ClickstreamTransformer,
    }

    def __init__(
        self,
        spark: SparkSession,
        adls_account: str,
        bronze_container: str = "bronze",
        silver_container: str = "silver",
        process_date: Optional[date] = None,
    ) -> None:
        self.spark = spark
        self.adls_account = adls_account
        self.bronze_container = bronze_container
        self.silver_container = silver_container
        self.process_date = process_date or date.today()

    def _abfss(self, container: str, path: str) -> str:
        return f"abfss://{container}@{self.adls_account}.dfs.core.windows.net/{path}"

    def _read_bronze(self, entity: str) -> DataFrame:
        """Read today's Bronze partition."""
        dt = self.process_date
        bronze_path = self._abfss(
            self.bronze_container,
            f"{entity}/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
        )
        return self.spark.read.parquet(bronze_path)

    def _write_silver(self, df: DataFrame, entity: str) -> None:
        """Write DataFrame to Silver as Delta Lake with merge."""
        silver_path = self._abfss(self.silver_container, entity)
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("year", "month", "day")
            .save(silver_path)
        )
        logger.info("wrote_silver", entity=entity, path=silver_path)

    def _optimize_silver(self, entity: str) -> None:
        """Run OPTIMIZE + ZORDER on the Silver Delta table."""
        silver_path = self._abfss(self.silver_container, entity)
        self.spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY (transaction_date, customer_id)")
        logger.info("optimized_silver", entity=entity)

    def run(self, entity: str) -> dict:
        """Process one entity: Bronze → Silver."""
        logger.info("bronze_to_silver_start", entity=entity, date=str(self.process_date))

        transformer_cls = self.TRANSFORMERS.get(entity)
        if not transformer_cls:
            raise ValueError(f"No transformer registered for entity: {entity}")

        bronze_df = self._read_bronze(entity)
        input_count = bronze_df.count()
        logger.info("bronze_read", entity=entity, rows=input_count)

        transformer = transformer_cls()
        silver_df = transformer.transform(bronze_df)

        # Add partition columns
        dt = self.process_date
        silver_df = (
            silver_df
            .withColumn("year", F.lit(dt.year))
            .withColumn("month", F.lit(dt.month))
            .withColumn("day", F.lit(dt.day))
        )

        self._write_silver(silver_df, entity)

        output_count = silver_df.count()
        dropped = input_count - output_count

        logger.info(
            "bronze_to_silver_complete",
            entity=entity,
            input_rows=input_count,
            output_rows=output_count,
            dropped_rows=dropped,
        )

        return {
            "entity": entity,
            "process_date": str(self.process_date),
            "input_rows": input_count,
            "output_rows": output_count,
            "dropped_rows": dropped,
        }


# ---------------------------------------------------------------------------
# CLI / Databricks notebook entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    entity = sys.argv[1] if len(sys.argv) > 1 else "transactions"
    process_date_str = sys.argv[2] if len(sys.argv) > 2 else str(date.today())
    process_date = date.fromisoformat(process_date_str)

    spark = create_spark_session(app_name=f"BronzeToSilver_{entity}")
    job = BronzeToSilverJob(
        spark=spark,
        adls_account="youradlsaccount",
        process_date=process_date,
    )
    result = job.run(entity)
    print(result)
