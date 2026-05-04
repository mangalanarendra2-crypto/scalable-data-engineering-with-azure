"""
src/ingestion/batch_ingestor.py
--------------------------------
Batch ingestion pipeline: fetches data from REST APIs or databases
and lands it in the Bronze layer of ADLS Gen2.

Usage:
    python -m src.ingestion.batch_ingestor --source api --target bronze
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from typing import Any, Generator, Optional
from uuid import uuid4

import click
import httpx
import pandas as pd

from src.storage.adls_client import ADLSClient
from src.utils.helpers import Timer, chunked, partition_path, with_retry
from src.utils.logger import get_logger, set_trace_id

logger = get_logger(__name__)


class APISource:
    """
    Generic REST API data source with pagination support.
    Handles: cursor-based, offset-based, and link-header pagination.
    """

    PAGINATION_CURSOR = "cursor"
    PAGINATION_OFFSET = "offset"
    PAGINATION_LINK = "link_header"

    def __init__(
        self,
        base_url: str,
        endpoint: str,
        headers: Optional[dict] = None,
        auth: Optional[tuple] = None,
        page_size: int = 500,
        pagination_type: str = PAGINATION_CURSOR,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.endpoint = endpoint
        self.page_size = page_size
        self.pagination_type = pagination_type
        self._client = httpx.Client(
            base_url=self.base_url,
            headers=headers or {},
            auth=auth,
            timeout=timeout,
        )

    @with_retry(max_attempts=4, min_wait=2.0, max_wait=60.0)
    def _fetch_page(self, params: dict) -> dict:
        response = self._client.get(self.endpoint, params=params)
        response.raise_for_status()
        return response.json()

    def fetch_all(
        self,
        extra_params: Optional[dict] = None,
        since: Optional[datetime] = None,
    ) -> Generator[list[dict], None, None]:
        """Yield pages of records from the API."""
        params: dict[str, Any] = {"page_size": self.page_size, **(extra_params or {})}
        if since:
            params["updated_since"] = since.isoformat()

        page_num = 0
        cursor: Optional[str] = None
        offset = 0

        while True:
            if self.pagination_type == self.PAGINATION_CURSOR and cursor:
                params["cursor"] = cursor
            elif self.pagination_type == self.PAGINATION_OFFSET:
                params["offset"] = offset

            page = self._fetch_page(params)
            records = page.get("data", page.get("results", page.get("items", [])))

            if not records:
                logger.info("api_pagination_exhausted", pages=page_num)
                break

            yield records
            page_num += 1
            offset += len(records)

            # Handle next cursor / link
            cursor = page.get("next_cursor") or page.get("cursor")
            if not cursor and self.pagination_type == self.PAGINATION_CURSOR:
                break
            if len(records) < self.page_size:
                break  # last page

    def close(self) -> None:
        self._client.close()


class BatchIngestor:
    """
    Orchestrates batch data ingestion from one or more sources
    into the Bronze layer.

    Features:
    - Idempotent writes (content-hash deduplication)
    - Watermark-based incremental loads
    - Schema capture (stores raw JSON + inferred schema)
    - Audit metadata injection
    """

    AUDIT_COLS = [
        "_ingestion_id",
        "_ingestion_timestamp",
        "_source_system",
        "_source_file_hash",
        "_pipeline_run_id",
    ]

    def __init__(
        self,
        adls: ADLSClient,
        bronze_container: str = "bronze",
        pipeline_run_id: Optional[str] = None,
    ) -> None:
        self.adls = adls
        self.bronze_container = bronze_container
        self.pipeline_run_id = pipeline_run_id or str(uuid4())

    def _inject_audit_columns(
        self,
        df: pd.DataFrame,
        source_system: str,
        content_hash: str,
    ) -> pd.DataFrame:
        """Add audit/lineage columns to every ingested record."""
        df = df.copy()
        now = datetime.utcnow()
        df["_ingestion_id"] = [str(uuid4()) for _ in range(len(df))]
        df["_ingestion_timestamp"] = now
        df["_source_system"] = source_system
        df["_source_file_hash"] = content_hash
        df["_pipeline_run_id"] = self.pipeline_run_id
        return df

    def _compute_hash(self, data: list[dict]) -> str:
        raw = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _build_bronze_path(
        self,
        source_system: str,
        entity: str,
        dt: Optional[date] = None,
        batch_id: str = "",
    ) -> str:
        """Construct the ADLS path for bronze landing."""
        dt = dt or date.today()
        base = f"{source_system}/{entity}/year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
        filename = f"{batch_id or uuid4().hex[:8]}.parquet"
        return f"{base}/{filename}"

    def ingest_from_api(
        self,
        api: APISource,
        source_system: str,
        entity: str,
        since: Optional[datetime] = None,
        dt: Optional[date] = None,
        extra_params: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Ingest all pages from an API source into Bronze.
        Returns a summary dict with counts and paths.
        """
        run_id = self.pipeline_run_id
        set_trace_id(run_id)
        logger.info(
            "ingest_api_start",
            source=source_system,
            entity=entity,
            since=str(since),
        )

        total_records = 0
        written_paths: list[str] = []

        try:
            for page_idx, records in enumerate(
                api.fetch_all(extra_params=extra_params, since=since)
            ):
                if not records:
                    continue

                content_hash = self._compute_hash(records)
                df = pd.json_normalize(records)
                df = self._inject_audit_columns(df, source_system, content_hash)

                path = self._build_bronze_path(
                    source_system, entity, dt, batch_id=f"p{page_idx:04d}_{content_hash}"
                )

                with Timer(f"write_bronze_page_{page_idx}"):
                    self.adls.upload_dataframe_parquet(df, self.bronze_container, path)

                written_paths.append(path)
                total_records += len(records)

                logger.info(
                    "page_ingested",
                    page=page_idx,
                    records=len(records),
                    path=path,
                )

        finally:
            api.close()

        # Save watermark checkpoint
        checkpoint = {
            "source_system": source_system,
            "entity": entity,
            "last_run": datetime.utcnow().isoformat(),
            "total_records": total_records,
            "paths": written_paths,
        }
        self.adls.save_checkpoint(
            self.bronze_container, f"{source_system}/{entity}", checkpoint
        )

        logger.info(
            "ingest_api_complete",
            source=source_system,
            entity=entity,
            total_records=total_records,
            files_written=len(written_paths),
        )

        return {
            "run_id": run_id,
            "source_system": source_system,
            "entity": entity,
            "total_records": total_records,
            "files_written": len(written_paths),
            "paths": written_paths,
        }

    def ingest_dataframe(
        self,
        df: pd.DataFrame,
        source_system: str,
        entity: str,
        dt: Optional[date] = None,
        batch_size: int = 50_000,
    ) -> dict[str, Any]:
        """
        Ingest an in-memory DataFrame into Bronze, chunked for large datasets.
        """
        total_records = 0
        written_paths: list[str] = []

        for chunk_idx, chunk in enumerate(chunked(df.to_dict("records"), batch_size)):
            chunk_df = pd.DataFrame(chunk)
            content_hash = self._compute_hash(chunk)
            chunk_df = self._inject_audit_columns(chunk_df, source_system, content_hash)
            path = self._build_bronze_path(
                source_system, entity, dt,
                batch_id=f"chunk_{chunk_idx:04d}_{content_hash}"
            )
            self.adls.upload_dataframe_parquet(chunk_df, self.bronze_container, path)
            written_paths.append(path)
            total_records += len(chunk_df)

        return {
            "source_system": source_system,
            "entity": entity,
            "total_records": total_records,
            "files_written": len(written_paths),
        }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

@click.command()
@click.option("--source", required=True, help="Source system name (e.g., crm, erp)")
@click.option("--entity", required=True, help="Entity/table to ingest (e.g., orders)")
@click.option("--target", default="bronze", help="Target container")
@click.option("--adls-account", envvar="ADLS_ACCOUNT_NAME", required=True)
@click.option("--adls-key", envvar="ADLS_ACCOUNT_KEY", default=None)
@click.option("--api-url", envvar="API_BASE_URL", default="https://api.example.com")
@click.option("--since", default=None, help="ISO datetime for incremental load")
def main(source, entity, target, adls_account, adls_key, api_url, since):
    """Batch ingestor CLI."""
    from src.utils.logger import configure_logging
    configure_logging()

    since_dt = datetime.fromisoformat(since) if since else None
    adls = ADLSClient(account_name=adls_account, account_key=adls_key)
    api = APISource(base_url=api_url, endpoint=f"/{entity}")
    ingestor = BatchIngestor(adls=adls, bronze_container=target)

    result = ingestor.ingest_from_api(
        api=api, source_system=source, entity=entity, since=since_dt
    )
    click.echo(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
