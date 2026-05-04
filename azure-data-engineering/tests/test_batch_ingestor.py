"""
tests/test_batch_ingestor.py
-----------------------------
Unit tests for batch ingestion pipeline.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.ingestion.batch_ingestor import APISource, BatchIngestor
from src.storage.adls_client import ADLSClient


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_adls():
    adls = MagicMock(spec=ADLSClient)
    adls.upload_dataframe_parquet.return_value = None
    adls.save_checkpoint.return_value = None
    adls.load_checkpoint.return_value = None
    return adls


@pytest.fixture
def mock_api_pages():
    """Returns 3 pages of 5 records each."""
    return [
        [
            {
                "transaction_id": f"TXN-{i:04d}",
                "customer_id": f"CUST-{i % 100:04d}",
                "amount": float(i * 10 + 0.99),
                "currency": "USD",
                "timestamp": datetime.utcnow().isoformat(),
                "merchant_id": "MERCH-001",
            }
            for i in range(page * 5, page * 5 + 5)
        ]
        for page in range(3)
    ]


# ---------------------------------------------------------------------------
# BatchIngestor tests
# ---------------------------------------------------------------------------

class TestBatchIngestor:

    def test_inject_audit_columns(self, mock_adls):
        ingestor = BatchIngestor(adls=mock_adls)
        df = pd.DataFrame([{"id": 1, "value": "test"}])
        enriched = ingestor._inject_audit_columns(df, "test_source", "abc123")

        for col in BatchIngestor.AUDIT_COLS:
            assert col in enriched.columns, f"Missing audit column: {col}"

        assert enriched["_source_system"].iloc[0] == "test_source"
        assert enriched["_source_file_hash"].iloc[0] == "abc123"
        assert enriched["_pipeline_run_id"].iloc[0] == ingestor.pipeline_run_id

    def test_build_bronze_path(self, mock_adls):
        ingestor = BatchIngestor(adls=mock_adls)
        path = ingestor._build_bronze_path("crm", "orders", date(2026, 5, 4), "batch001")
        assert "crm/orders" in path
        assert "year=2026" in path
        assert "month=05" in path
        assert "day=04" in path
        assert path.endswith(".parquet")

    def test_ingest_dataframe(self, mock_adls):
        ingestor = BatchIngestor(adls=mock_adls)
        df = pd.DataFrame([
            {"id": i, "name": f"record_{i}", "value": i * 1.5}
            for i in range(100)
        ])

        result = ingestor.ingest_dataframe(
            df=df,
            source_system="test",
            entity="records",
            dt=date(2026, 5, 4),
            batch_size=50,
        )

        assert result["total_records"] == 100
        assert result["files_written"] == 2  # 100 / 50 = 2 chunks
        assert mock_adls.upload_dataframe_parquet.call_count == 2

    def test_ingest_from_api(self, mock_adls, mock_api_pages):
        ingestor = BatchIngestor(adls=mock_adls, pipeline_run_id="test-run-001")

        mock_api = MagicMock(spec=APISource)
        mock_api.fetch_all.return_value = iter(mock_api_pages)

        result = ingestor.ingest_from_api(
            api=mock_api,
            source_system="crm",
            entity="transactions",
            dt=date(2026, 5, 4),
        )

        assert result["total_records"] == 15  # 3 pages × 5 records
        assert result["files_written"] == 3
        assert result["source_system"] == "crm"
        assert result["entity"] == "transactions"
        mock_adls.save_checkpoint.assert_called_once()

    def test_compute_hash_deterministic(self, mock_adls):
        ingestor = BatchIngestor(adls=mock_adls)
        data = [{"id": 1, "val": "test"}, {"id": 2, "val": "other"}]

        h1 = ingestor._compute_hash(data)
        h2 = ingestor._compute_hash(data)
        assert h1 == h2
        assert len(h1) == 16

    def test_different_data_different_hash(self, mock_adls):
        ingestor = BatchIngestor(adls=mock_adls)
        h1 = ingestor._compute_hash([{"id": 1}])
        h2 = ingestor._compute_hash([{"id": 2}])
        assert h1 != h2


# ---------------------------------------------------------------------------
# APISource tests
# ---------------------------------------------------------------------------

class TestAPISource:

    @patch("httpx.Client.get")
    def test_fetch_page_success(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "next_cursor": None,
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        api = APISource(
            base_url="https://api.example.com",
            endpoint="/transactions",
            page_size=100,
        )

        pages = list(api.fetch_all())
        assert len(pages) == 1
        assert len(pages[0]) == 2

    @patch("httpx.Client.get")
    def test_pagination_stops_on_empty(self, mock_get):
        responses = [
            {"data": [{"id": i} for i in range(5)], "next_cursor": "abc"},
            {"data": [{"id": i} for i in range(5, 10)], "next_cursor": "def"},
            {"data": []},  # empty page stops pagination
        ]
        mock_get.side_effect = [
            MagicMock(json=MagicMock(return_value=r), raise_for_status=MagicMock())
            for r in responses
        ]

        api = APISource(
            base_url="https://api.example.com",
            endpoint="/orders",
            page_size=5,
            pagination_type=APISource.PAGINATION_CURSOR,
        )

        pages = list(api.fetch_all())
        assert len(pages) == 2  # 3rd empty page stops


# ---------------------------------------------------------------------------
# Tests for helper utilities
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_partition_path(self):
        from src.utils.helpers import partition_path
        path = partition_path("data/events", date(2026, 5, 4))
        assert path == "data/events/year=2026/month=05/day=04"

    def test_partition_path_with_extra(self):
        from src.utils.helpers import partition_path
        path = partition_path("data/events", date(2026, 5, 4), region="us-east")
        assert "region=us-east" in path

    def test_chunked(self):
        from src.utils.helpers import chunked
        items = list(range(10))
        chunks = list(chunked(items, 3))
        assert len(chunks) == 4
        assert chunks[0] == [0, 1, 2]
        assert chunks[-1] == [9]

    def test_chunked_exact_multiple(self):
        from src.utils.helpers import chunked
        chunks = list(chunked(range(9), 3))
        assert len(chunks) == 3

    def test_to_snake_case(self):
        from src.processing.bronze_to_silver import to_snake_case
        assert to_snake_case("CustomerID") == "customer_i_d"
        assert to_snake_case("transactionDate") == "transaction_date"
        assert to_snake_case("Amount") == "amount"
        assert to_snake_case("MerchantId") == "merchant_id"

    def test_flatten_dict(self):
        from src.utils.helpers import flatten_dict
        nested = {"a": {"b": {"c": 1}, "d": 2}, "e": 3}
        flat = flatten_dict(nested)
        assert flat == {"a.b.c": 1, "a.d": 2, "e": 3}

    def test_safe_cast(self):
        from src.utils.helpers import safe_cast
        assert safe_cast("42", int) == 42
        assert safe_cast("not_a_number", int, default=-1) == -1
        assert safe_cast("3.14", float) == pytest.approx(3.14)


# ---------------------------------------------------------------------------
# Data Quality tests
# ---------------------------------------------------------------------------

class TestDataQuality:

    def test_dq_rule_passes(self):
        """Rules that pass on clean data return all rows."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master("local").appName("test").getOrCreate()
            df = spark.createDataFrame(
                [{"amount": 100.0, "currency": "USD"}, {"amount": 50.0, "currency": "EUR"}]
            )
            from src.processing.bronze_to_silver import DataQualityChecker, DQRule
            checker = DataQualityChecker([
                DQRule("amount_positive", "amount > 0", severity="error")
            ])
            clean_df, results = checker.check(df)
            assert results[0].passed
            assert results[0].failed_rows == 0
        except ImportError:
            pytest.skip("PySpark not available in test environment")

    def test_dq_rule_fails_on_bad_data(self):
        """Error-severity rule with violations should raise ValueError."""
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.master("local").appName("test").getOrCreate()
            df = spark.createDataFrame(
                [{"amount": -1.0}, {"amount": -2.0}, {"amount": -3.0},
                 {"amount": -4.0}, {"amount": -5.0}, {"amount": 100.0}]
            )
            from src.processing.bronze_to_silver import DataQualityChecker, DQRule
            checker = DataQualityChecker(
                [DQRule("amount_positive", "amount > 0", severity="error")],
                fail_threshold=0.05,  # 5% threshold — 5/6 ~83% fail should trigger
            )
            with pytest.raises(ValueError, match="DQ rule.*failed"):
                checker.check(df)
        except ImportError:
            pytest.skip("PySpark not available in test environment")


# ---------------------------------------------------------------------------
# Monitoring tests
# ---------------------------------------------------------------------------

class TestMonitoring:

    def test_pipeline_run_record(self):
        from src.monitoring.metrics import PipelineRunRecord, PipelineStatus
        run = PipelineRunRecord(
            run_id="test-001",
            dag_id="daily_pipeline",
            task_id="ingest",
            status=PipelineStatus.RUNNING,
            start_time=datetime.utcnow(),
            input_records=1000,
            output_records=980,
            error_records=20,
        )
        run.complete(PipelineStatus.SUCCESS)

        assert run.status == PipelineStatus.SUCCESS
        assert run.end_time is not None
        assert run.duration_seconds is not None
        assert run.error_rate == pytest.approx(0.02)  # 20/1000

    def test_alert_rule_gt(self):
        from src.monitoring.metrics import AlertRule
        rule = AlertRule(
            name="Test", description="", metric="ErrorRate",
            threshold=0.05, comparison="gt", severity=1
        )
        assert rule.evaluate(0.10) is True
        assert rule.evaluate(0.03) is False

    def test_pipeline_tracker_success(self):
        from src.monitoring.metrics import PipelineTracker, PipelineStatus
        tracker = PipelineTracker(monitor=None, dag_id="test_dag", task_id="test_task")

        with tracker as t:
            t.set_records(input=100, output=95, errors=5)

        assert tracker._run.status == PipelineStatus.SUCCESS
        assert tracker._run.output_records == 95

    def test_pipeline_tracker_failure(self):
        from src.monitoring.metrics import PipelineTracker, PipelineStatus
        tracker = PipelineTracker(monitor=None, dag_id="test_dag", task_id="failing_task")

        with pytest.raises(RuntimeError):
            with tracker:
                raise RuntimeError("Simulated failure")

        assert tracker._run.status == PipelineStatus.FAILED
        assert "Simulated failure" in tracker._run.error_message
