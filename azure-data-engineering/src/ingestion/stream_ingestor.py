"""
src/ingestion/stream_ingestor.py
---------------------------------
Real-time streaming ingestion from Azure Event Hubs.
Uses the azure-eventhub SDK with checkpointing to BlobStorage
for exactly-once / at-least-once delivery semantics.

Features:
  - Async consumer with checkpoint store (ADLS-backed)
  - Micro-batch accumulation before writing to Bronze
  - Dead-letter queue for poison messages
  - Schema validation via Pydantic
  - Graceful shutdown on SIGTERM
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
from pydantic import BaseModel, ValidationError

from src.storage.adls_client import ADLSClient
from src.utils.logger import get_logger, set_trace_id

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Event schemas (Pydantic models)
# ---------------------------------------------------------------------------

class TransactionEvent(BaseModel):
    transaction_id: str
    customer_id: str
    amount: float
    currency: str
    timestamp: datetime
    merchant_id: Optional[str] = None
    metadata: Optional[dict] = None


class ClickstreamEvent(BaseModel):
    session_id: str
    user_id: Optional[str] = None
    page_url: str
    event_type: str
    timestamp: datetime
    properties: Optional[dict] = None


class IoTTelemetryEvent(BaseModel):
    device_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: datetime
    location: Optional[dict] = None


# Map hub name → schema
EVENT_SCHEMAS: dict[str, type[BaseModel]] = {
    "eh-transactions": TransactionEvent,
    "eh-clickstream": ClickstreamEvent,
    "eh-iot-telemetry": IoTTelemetryEvent,
}


# ---------------------------------------------------------------------------
# Micro-batch buffer
# ---------------------------------------------------------------------------

class MicroBatchBuffer:
    """Accumulates events and flushes when full or time-based threshold hit."""

    def __init__(self, max_size: int = 1000, max_seconds: float = 30.0) -> None:
        self.max_size = max_size
        self.max_seconds = max_seconds
        self._buffer: list[dict] = []
        self._last_flush = time.monotonic()

    def add(self, event: dict) -> None:
        self._buffer.append(event)

    def should_flush(self) -> bool:
        if len(self._buffer) >= self.max_size:
            return True
        if time.monotonic() - self._last_flush >= self.max_seconds:
            return True
        return False

    def flush(self) -> list[dict]:
        events = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.monotonic()
        return events

    @property
    def size(self) -> int:
        return len(self._buffer)


# ---------------------------------------------------------------------------
# Stream Ingestor
# ---------------------------------------------------------------------------

class StreamIngestor:
    """
    Consumes events from Azure Event Hubs and writes micro-batches
    to the Bronze layer in ADLS Gen2.
    """

    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
        consumer_group: str,
        checkpoint_store_conn: str,
        checkpoint_container: str,
        adls: ADLSClient,
        bronze_container: str = "bronze",
        batch_size: int = 1000,
        batch_interval_seconds: float = 30.0,
        max_wait_time: float = 5.0,
    ) -> None:
        self.eventhub_name = eventhub_name
        self.adls = adls
        self.bronze_container = bronze_container
        self._batch_buffer = MicroBatchBuffer(batch_size, batch_interval_seconds)
        self._dlq: list[dict] = []   # dead letter queue
        self._shutdown = False
        self._total_processed = 0
        self._total_errors = 0

        self._checkpoint_store = BlobCheckpointStore.from_connection_string(
            conn_str=checkpoint_store_conn,
            container_name=checkpoint_container,
        )
        self._consumer = EventHubConsumerClient.from_connection_string(
            conn_str=connection_string,
            consumer_group=consumer_group,
            eventhub_name=eventhub_name,
            checkpoint_store=self._checkpoint_store,
        )

    # ------------------------------------------------------------------
    # Event processing
    # ------------------------------------------------------------------

    def _parse_event(self, raw: bytes, hub_name: str) -> Optional[dict]:
        """Deserialize and validate an event against its schema."""
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.warning("json_decode_error", error=str(e))
            return None

        schema = EVENT_SCHEMAS.get(hub_name)
        if schema:
            try:
                validated = schema(**payload)
                return validated.dict()
            except ValidationError as e:
                logger.warning("schema_validation_failed", errors=e.errors())
                self._dlq.append({"raw": payload, "error": str(e), "hub": hub_name})
                return None

        return payload  # no schema → pass through

    def _add_stream_metadata(self, event: dict, eh_event: EventData) -> dict:
        return {
            **event,
            "_stream_ingest_id": str(uuid4()),
            "_ingest_timestamp": datetime.utcnow().isoformat(),
            "_eventhub": self.eventhub_name,
            "_partition_id": str(eh_event.raw_amqp_message.annotations.get(b"x-opt-partition-id", "")),
            "_offset": eh_event.offset,
            "_sequence_number": eh_event.sequence_number,
        }

    async def _on_event(
        self, partition_context: Any, event: EventData
    ) -> None:
        """Callback invoked for each received event."""
        try:
            raw = event.body_as_bytes()
            parsed = self._parse_event(raw, self.eventhub_name)

            if parsed:
                enriched = self._add_stream_metadata(parsed, event)
                self._batch_buffer.add(enriched)
                self._total_processed += 1
            else:
                self._total_errors += 1

            # Flush if threshold reached
            if self._batch_buffer.should_flush():
                await self._flush_batch()

            # Checkpoint every N events
            if self._total_processed % 500 == 0:
                await partition_context.update_checkpoint(event)

        except Exception as e:
            logger.error("event_processing_error", error=str(e))
            self._total_errors += 1

    async def _flush_batch(self) -> None:
        """Write buffered events to ADLS Bronze."""
        batch = self._batch_buffer.flush()
        if not batch:
            return

        import pandas as pd
        df = pd.DataFrame(batch)
        now = datetime.utcnow()
        hub_short = self.eventhub_name.replace("eh-", "")
        path = (
            f"streaming/{hub_short}/"
            f"year={now.year:04d}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"batch_{now.strftime('%H%M%S')}_{uuid4().hex[:6]}.parquet"
        )

        self.adls.upload_dataframe_parquet(df, self.bronze_container, path)

        logger.info(
            "batch_flushed",
            hub=self.eventhub_name,
            records=len(batch),
            path=path,
            total_processed=self._total_processed,
        )

        # Flush DLQ if any
        if self._dlq:
            dlq_path = path.replace("batch_", "dlq_").replace(".parquet", ".json")
            self.adls.upload_json(self._dlq, self.bronze_container, dlq_path)
            logger.warning("dlq_flushed", count=len(self._dlq))
            self._dlq.clear()

    async def _periodic_flush(self) -> None:
        """Background task: flush buffer on time interval."""
        while not self._shutdown:
            await asyncio.sleep(10)
            if self._batch_buffer.size > 0:
                await self._flush_batch()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start consuming events. Blocks until shutdown."""
        set_trace_id()
        logger.info("stream_ingestor_starting", hub=self.eventhub_name)

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, self._handle_shutdown)
        loop.add_signal_handler(signal.SIGINT, self._handle_shutdown)

        flush_task = asyncio.create_task(self._periodic_flush())

        try:
            async with self._consumer:
                await self._consumer.receive(
                    on_event=self._on_event,
                    starting_position="-1",  # from latest
                )
        except asyncio.CancelledError:
            pass
        finally:
            self._shutdown = True
            flush_task.cancel()
            # Final flush
            await self._flush_batch()
            logger.info(
                "stream_ingestor_stopped",
                total_processed=self._total_processed,
                total_errors=self._total_errors,
            )

    def _handle_shutdown(self) -> None:
        logger.info("shutdown_signal_received")
        self._shutdown = True
        asyncio.get_event_loop().stop()

    def start(self) -> None:
        """Sync entry point."""
        asyncio.run(self.run())
