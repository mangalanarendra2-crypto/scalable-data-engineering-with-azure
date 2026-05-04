"""
src/processing/silver_to_gold.py
---------------------------------
PySpark job: Silver → Gold layer (curated, business-ready aggregates).

Gold layer = fact tables, dimension tables, and pre-aggregated data marts
ready for Power BI, Azure Synapse Analytics, and downstream ML pipelines.

Outputs:
  - gold/fact_transactions/        (transaction fact table)
  - gold/fact_sessions/            (session-level clickstream facts)
  - gold/dim_customer/             (slowly changing dimension - SCD Type 2)
  - gold/mart_daily_revenue/       (daily revenue aggregate)
  - gold/mart_customer_lifetime/   (customer lifetime value metrics)
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Optional

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from src.processing.bronze_to_silver import create_spark_session
from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Fact Tables
# ---------------------------------------------------------------------------

class FactTransactionsBuilder:
    """Build fact_transactions from Silver transactions + dimension lookups."""

    def build(self, transactions_df: DataFrame, dim_customer: DataFrame) -> DataFrame:
        return (
            transactions_df
            .join(
                dim_customer.select("customer_id_hashed", "customer_key", "segment", "country"),
                transactions_df.customer_id == dim_customer.customer_id_hashed,
                how="left",
            )
            .select(
                F.col("transaction_id").alias("transaction_key"),
                F.col("customer_key"),
                F.col("transaction_date").alias("date_key"),
                F.col("amount").alias("transaction_amount"),
                F.col("amount_usd"),
                F.col("currency"),
                F.col("merchant_id"),
                F.col("is_high_value"),
                F.col("segment").alias("customer_segment"),
                F.col("country"),
                F.col("_silver_timestamp").alias("last_updated"),
            )
        )


class FactSessionsBuilder:
    """Build session-level fact table from clickstream Silver data."""

    def build(self, clickstream_df: DataFrame) -> DataFrame:
        session_window = Window.partitionBy("session_id")
        return (
            clickstream_df
            .withColumn("session_start", F.min("timestamp").over(session_window))
            .withColumn("session_end", F.max("timestamp").over(session_window))
            .withColumn("page_views", F.count("*").over(session_window))
            .withColumn(
                "session_duration_seconds",
                (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")).cast("long")
            )
            .groupBy(
                "session_id", "user_id", "domain",
                "session_start", "session_end",
                "event_date", "page_views",
            )
            .agg(
                F.max("session_duration_seconds").alias("session_duration_seconds"),
                F.count_distinct("page_url").alias("unique_pages"),
                F.sum(F.when(F.col("event_type") == "submit", 1).otherwise(0)).alias("conversions"),
            )
            .withColumn(
                "bounce",
                (F.col("page_views") == 1).cast("boolean")
            )
        )


# ---------------------------------------------------------------------------
# Dimension Tables (SCD Type 2)
# ---------------------------------------------------------------------------

class DimCustomerSCD2:
    """
    Slowly Changing Dimension Type 2 for customers.
    Tracks historical changes by adding:
      - effective_from / effective_to
      - is_current flag
      - surrogate key
    """

    def upsert(
        self,
        spark: SparkSession,
        updates_df: DataFrame,
        target_path: str,
        natural_key: str = "customer_id_hashed",
        tracked_cols: Optional[list[str]] = None,
    ) -> None:
        """Merge updates into the dimension table with SCD2 logic."""
        tracked_cols = tracked_cols or ["segment", "country", "tier", "email_domain"]
        now = F.current_timestamp()

        # Add SCD2 columns to updates
        updates = (
            updates_df
            .withColumn("effective_from", now)
            .withColumn("effective_to", F.lit(None).cast(T.TimestampType()))
            .withColumn("is_current", F.lit(True))
        )

        try:
            target = DeltaTable.forPath(spark, target_path)

            # Expire old records that have changed
            change_condition = " OR ".join(
                [f"target.{c} != source.{c}" for c in tracked_cols]
            )

            # Step 1: Expire changed rows
            target.alias("target").merge(
                updates.alias("source"),
                f"target.{natural_key} = source.{natural_key} AND target.is_current = true",
            ).whenMatchedUpdate(
                condition=change_condition,
                set={
                    "is_current": F.lit(False),
                    "effective_to": now,
                }
            ).execute()

            # Step 2: Insert new / changed rows
            target.alias("target").merge(
                updates.alias("source"),
                f"target.{natural_key} = source.{natural_key} AND target.effective_from = source.effective_from",
            ).whenNotMatchedInsertAll().execute()

        except Exception:
            # Table doesn't exist yet - initial load
            updates.withColumn(
                "customer_key", F.monotonically_increasing_id()
            ).write.format("delta").save(target_path)

        logger.info("scd2_upsert_complete", path=target_path, rows=updates.count())


# ---------------------------------------------------------------------------
# Data Marts (Aggregates)
# ---------------------------------------------------------------------------

class DailyRevenueMart:
    """Pre-aggregated daily revenue mart for reporting."""

    def build(self, fact_transactions: DataFrame) -> DataFrame:
        return (
            fact_transactions
            .groupBy("date_key", "customer_segment", "country", "currency")
            .agg(
                F.count("transaction_key").alias("transaction_count"),
                F.sum("transaction_amount").alias("gross_revenue"),
                F.sum("amount_usd").alias("gross_revenue_usd"),
                F.avg("transaction_amount").alias("avg_transaction_value"),
                F.sum(F.when(F.col("is_high_value"), F.col("transaction_amount")).otherwise(0))
                  .alias("high_value_revenue"),
                F.count_distinct("customer_key").alias("unique_customers"),
            )
            .withColumn("revenue_per_customer", F.col("gross_revenue_usd") / F.col("unique_customers"))
            .withColumn("_mart_timestamp", F.current_timestamp())
        )


class CustomerLifetimeMart:
    """Customer Lifetime Value metrics mart."""

    def build(self, fact_transactions: DataFrame) -> DataFrame:
        return (
            fact_transactions
            .groupBy("customer_key", "customer_segment", "country")
            .agg(
                F.count("transaction_key").alias("total_transactions"),
                F.sum("amount_usd").alias("lifetime_value_usd"),
                F.avg("amount_usd").alias("avg_order_value_usd"),
                F.min("date_key").alias("first_transaction_date"),
                F.max("date_key").alias("last_transaction_date"),
                F.datediff(F.max("date_key"), F.min("date_key")).alias("tenure_days"),
            )
            .withColumn(
                "clv_tier",
                F.when(F.col("lifetime_value_usd") >= 10_000, "Platinum")
                 .when(F.col("lifetime_value_usd") >= 5_000, "Gold")
                 .when(F.col("lifetime_value_usd") >= 1_000, "Silver")
                 .otherwise("Bronze")
            )
            .withColumn("_mart_timestamp", F.current_timestamp())
        )


# ---------------------------------------------------------------------------
# Gold Layer Job Orchestrator
# ---------------------------------------------------------------------------

class SilverToGoldJob:
    """Reads Silver Delta tables and writes Gold marts."""

    def __init__(
        self,
        spark: SparkSession,
        adls_account: str,
        silver_container: str = "silver",
        gold_container: str = "gold",
        process_date: Optional[date] = None,
    ) -> None:
        self.spark = spark
        self.adls_account = adls_account
        self.silver_container = silver_container
        self.gold_container = gold_container
        self.process_date = process_date or date.today()

    def _path(self, container: str, table: str) -> str:
        return f"abfss://{container}@{self.adls_account}.dfs.core.windows.net/{table}"

    def _read_silver(self, table: str) -> DataFrame:
        return self.spark.read.format("delta").load(self._path(self.silver_container, table))

    def _write_gold(self, df: DataFrame, table: str, partition_cols: Optional[list[str]] = None) -> None:
        writer = (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(self._path(self.gold_container, table))
        logger.info("wrote_gold", table=table)

    def run_all(self) -> dict:
        results: dict = {}

        # Read silver sources
        transactions = self._read_silver("transactions")
        clickstream = self._read_silver("clickstream")

        # Build fact tables
        fact_txn = FactTransactionsBuilder().build(
            transactions, self.spark.createDataFrame([], schema=T.StructType([]))  # placeholder
        )
        self._write_gold(fact_txn, "fact_transactions", partition_cols=["date_key"])
        results["fact_transactions"] = fact_txn.count()

        fact_sessions = FactSessionsBuilder().build(clickstream)
        self._write_gold(fact_sessions, "fact_sessions", partition_cols=["event_date"])
        results["fact_sessions"] = fact_sessions.count()

        # Build aggregated marts
        daily_revenue = DailyRevenueMart().build(fact_txn)
        self._write_gold(daily_revenue, "mart_daily_revenue", partition_cols=["date_key"])
        results["mart_daily_revenue"] = daily_revenue.count()

        clv_mart = CustomerLifetimeMart().build(fact_txn)
        self._write_gold(clv_mart, "mart_customer_lifetime")
        results["mart_customer_lifetime"] = clv_mart.count()

        logger.info("silver_to_gold_complete", results=results)
        return results


if __name__ == "__main__":
    import sys
    process_date = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    spark = create_spark_session("SilverToGold")
    job = SilverToGoldJob(spark, adls_account="youradlsaccount", process_date=process_date)
    print(job.run_all())
