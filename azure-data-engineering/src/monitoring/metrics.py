"""
src/monitoring/metrics.py
--------------------------
Pipeline observability: custom metrics, Azure Monitor integration,
and alerting rules.

Features:
  - Structured metric emission to Azure Monitor / Log Analytics
  - Pipeline run tracking (duration, record counts, error rates)
  - Alert rule definitions
  - Health check endpoints
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Generator, Optional
from uuid import uuid4

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class PipelineStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    PARTIAL = "partial"


class MetricUnit(str, Enum):
    COUNT = "Count"
    BYTES = "Bytes"
    MILLISECONDS = "Milliseconds"
    PERCENT = "Percent"
    ROWS_PER_SECOND = "CountPerSecond"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PipelineMetric:
    name: str
    value: float
    unit: MetricUnit = MetricUnit.COUNT
    dimensions: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PipelineRunRecord:
    run_id: str
    dag_id: str
    task_id: str
    status: PipelineStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    input_records: int = 0
    output_records: int = 0
    error_records: int = 0
    error_message: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def throughput_rps(self) -> Optional[float]:
        if self.duration_seconds and self.duration_seconds > 0:
            return self.output_records / self.duration_seconds
        return None

    @property
    def error_rate(self) -> float:
        total = self.input_records
        return (self.error_records / total) if total > 0 else 0.0

    def complete(self, status: PipelineStatus, error: Optional[str] = None) -> None:
        self.end_time = datetime.utcnow()
        self.duration_seconds = (self.end_time - self.start_time).total_seconds()
        self.status = status
        self.error_message = error


# ---------------------------------------------------------------------------
# Azure Monitor client
# ---------------------------------------------------------------------------

class AzureMonitorClient:
    """
    Emits custom metrics to Azure Monitor via the Metrics Ingestion API.
    Also supports Log Analytics workspace queries.
    """

    def __init__(
        self,
        data_collection_endpoint: str,
        rule_id: str,
        stream_name: str,
        credential: Optional[Any] = None,
    ) -> None:
        self.endpoint = data_collection_endpoint.rstrip("/")
        self.rule_id = rule_id
        self.stream_name = stream_name
        self._credential = credential
        self._token: Optional[str] = None

    def _get_token(self) -> str:
        if self._credential:
            token = self._credential.get_token("https://monitor.azure.com/.default")
            return token.token
        return ""

    def emit_metric(self, metric: PipelineMetric) -> None:
        """Post a custom metric to Azure Monitor."""
        body = [
            {
                "TimeGenerated": metric.timestamp.isoformat() + "Z",
                "MetricName": metric.name,
                "MetricValue": metric.value,
                "MetricUnit": metric.unit.value,
                **{f"Dimension_{k}": v for k, v in metric.dimensions.items()},
            }
        ]
        self._post_logs(body)

    def emit_pipeline_run(self, run: PipelineRunRecord) -> None:
        """Emit a complete pipeline run record as a custom log."""
        body = [
            {
                "TimeGenerated": (run.end_time or run.start_time).isoformat() + "Z",
                "RunId": run.run_id,
                "DagId": run.dag_id,
                "TaskId": run.task_id,
                "Status": run.status.value,
                "DurationSeconds": run.duration_seconds,
                "InputRecords": run.input_records,
                "OutputRecords": run.output_records,
                "ErrorRecords": run.error_records,
                "ErrorRate": run.error_rate,
                "ThroughputRPS": run.throughput_rps,
                "ErrorMessage": run.error_message or "",
            }
        ]
        self._post_logs(body)
        logger.info(
            "pipeline_run_emitted",
            run_id=run.run_id,
            status=run.status.value,
            duration=run.duration_seconds,
        )

    def _post_logs(self, body: list[dict]) -> None:
        token = self._get_token()
        url = f"{self.endpoint}/dataCollectionRules/{self.rule_id}/streams/{self.stream_name}?api-version=2023-01-01"
        try:
            resp = requests.post(
                url,
                json=body,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.warning("azure_monitor_emit_failed", error=str(e))


# ---------------------------------------------------------------------------
# Pipeline tracker (context manager)
# ---------------------------------------------------------------------------

class PipelineTracker:
    """
    Context manager that tracks a pipeline task execution,
    emits metrics, and handles error reporting.

    Usage:
        with PipelineTracker(monitor, dag_id="my_dag", task_id="ingest") as tracker:
            df = ingest_data()
            tracker.set_records(input=1000, output=998)
    """

    def __init__(
        self,
        monitor: Optional[AzureMonitorClient],
        dag_id: str,
        task_id: str,
    ) -> None:
        self.monitor = monitor
        self._run = PipelineRunRecord(
            run_id=str(uuid4()),
            dag_id=dag_id,
            task_id=task_id,
            status=PipelineStatus.RUNNING,
            start_time=datetime.utcnow(),
        )

    def set_records(
        self,
        input: int = 0,
        output: int = 0,
        errors: int = 0,
    ) -> None:
        self._run.input_records = input
        self._run.output_records = output
        self._run.error_records = errors

    def add_metadata(self, **kwargs: Any) -> None:
        self._run.metadata.update(kwargs)

    def __enter__(self) -> "PipelineTracker":
        logger.info(
            "pipeline_task_started",
            run_id=self._run.run_id,
            dag=self._run.dag_id,
            task=self._run.task_id,
        )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type:
            self._run.complete(PipelineStatus.FAILED, error=str(exc_val))
            logger.error(
                "pipeline_task_failed",
                run_id=self._run.run_id,
                error=str(exc_val),
                duration=self._run.duration_seconds,
            )
        else:
            self._run.complete(PipelineStatus.SUCCESS)
            logger.info(
                "pipeline_task_succeeded",
                run_id=self._run.run_id,
                duration=self._run.duration_seconds,
                output_records=self._run.output_records,
                throughput=self._run.throughput_rps,
            )

        if self.monitor:
            try:
                self.monitor.emit_pipeline_run(self._run)
            except Exception as e:
                logger.warning("monitor_emit_error", error=str(e))

        return False  # re-raise exceptions


# ---------------------------------------------------------------------------
# Alert rules
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    name: str
    description: str
    metric: str
    threshold: float
    comparison: str   # "gt", "lt", "eq"
    severity: int     # 0=critical, 1=error, 2=warning, 3=info
    window_minutes: int = 60
    frequency_minutes: int = 15

    def evaluate(self, current_value: float) -> bool:
        """Returns True if alert should fire."""
        ops = {"gt": current_value > self.threshold,
               "lt": current_value < self.threshold,
               "eq": current_value == self.threshold}
        return ops.get(self.comparison, False)


STANDARD_ALERT_RULES = [
    AlertRule(
        name="HighErrorRate",
        description="Pipeline error rate exceeds 5%",
        metric="ErrorRate",
        threshold=0.05,
        comparison="gt",
        severity=1,
        window_minutes=60,
    ),
    AlertRule(
        name="LowThroughput",
        description="Processing throughput below 100 rows/sec",
        metric="ThroughputRPS",
        threshold=100,
        comparison="lt",
        severity=2,
        window_minutes=30,
    ),
    AlertRule(
        name="PipelineFailure",
        description="Any pipeline task failed",
        metric="FailedTasks",
        threshold=0,
        comparison="gt",
        severity=0,
        window_minutes=5,
    ),
    AlertRule(
        name="DataFreshness",
        description="Data not updated in 26 hours",
        metric="DataAgeHours",
        threshold=26,
        comparison="gt",
        severity=1,
        window_minutes=60,
    ),
]
