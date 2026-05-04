"""
src/storage/adls_client.py
--------------------------
Azure Data Lake Storage Gen2 client wrapper.
Supports read/write of Parquet, JSON, Delta, and raw files.
Uses Azure Identity (DefaultAzureCredential) or account key.
"""

from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Optional

import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
)

from src.utils.helpers import Timer, chunked, partition_path
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ADLSClient:
    """
    Wrapper around Azure Data Lake Storage Gen2.

    Supports:
    - Upload / download of files (Parquet, JSON, CSV, raw bytes)
    - Directory listing and deletion
    - Partition-aware reads and writes
    - Checkpointing for exactly-once semantics
    """

    def __init__(
        self,
        account_name: str,
        account_key: Optional[str] = None,
        use_managed_identity: bool = False,
    ) -> None:
        self.account_name = account_name
        account_url = f"https://{account_name}.dfs.core.windows.net"

        if use_managed_identity or account_key is None:
            credential = DefaultAzureCredential()
        else:
            credential = account_key  # type: ignore[assignment]

        self._service = DataLakeServiceClient(account_url, credential=credential)
        logger.info("adls_client_initialized", account=account_name)

    # ------------------------------------------------------------------
    # File System / Container operations
    # ------------------------------------------------------------------

    def get_filesystem(self, container: str) -> FileSystemClient:
        return self._service.get_file_system_client(container)

    def ensure_filesystem(self, container: str) -> FileSystemClient:
        """Create container if it doesn't exist."""
        fs = self.get_filesystem(container)
        try:
            fs.create_file_system()
            logger.info("filesystem_created", container=container)
        except Exception:
            pass  # already exists
        return fs

    def list_paths(
        self,
        container: str,
        directory: str = "",
        recursive: bool = True,
    ) -> list[str]:
        """List all file paths under a directory."""
        fs = self.get_filesystem(container)
        paths = fs.get_paths(path=directory, recursive=recursive)
        return [p.name for p in paths if not p.is_directory]

    # ------------------------------------------------------------------
    # Upload operations
    # ------------------------------------------------------------------

    def upload_bytes(
        self,
        container: str,
        path: str,
        data: bytes,
        overwrite: bool = True,
    ) -> None:
        """Upload raw bytes to ADLS."""
        fs = self.get_filesystem(container)
        file_client = fs.get_file_client(path)
        file_client.upload_data(data, overwrite=overwrite, length=len(data))
        logger.info("uploaded_bytes", container=container, path=path, size=len(data))

    def upload_dataframe_parquet(
        self,
        df: pd.DataFrame,
        container: str,
        path: str,
        compression: str = "snappy",
    ) -> None:
        """Serialize DataFrame to Parquet and upload to ADLS."""
        with Timer(f"upload_parquet:{path}"):
            buffer = io.BytesIO()
            df.to_parquet(buffer, engine="pyarrow", compression=compression, index=False)
            self.upload_bytes(container, path, buffer.getvalue())
        logger.info(
            "uploaded_parquet",
            container=container,
            path=path,
            rows=len(df),
            columns=len(df.columns),
        )

    def upload_json(
        self,
        data: list[dict] | dict,
        container: str,
        path: str,
    ) -> None:
        """Upload JSON data to ADLS."""
        raw = json.dumps(data, default=str, ensure_ascii=False).encode("utf-8")
        self.upload_bytes(container, path, raw)

    def upload_partitioned(
        self,
        df: pd.DataFrame,
        container: str,
        base_path: str,
        partition_col: str,
        dt: Optional[datetime] = None,
    ) -> list[str]:
        """
        Upload a DataFrame split by a partition column.
        Returns list of uploaded paths.
        """
        uploaded: list[str] = []
        for value, partition_df in df.groupby(partition_col):
            part_path = partition_path(base_path, dt or datetime.utcnow())
            file_path = f"{part_path}/{partition_col}={value}/data.parquet"
            self.upload_dataframe_parquet(partition_df, container, file_path)
            uploaded.append(file_path)
        return uploaded

    # ------------------------------------------------------------------
    # Download operations
    # ------------------------------------------------------------------

    def download_bytes(self, container: str, path: str) -> bytes:
        """Download a file as raw bytes."""
        fs = self.get_filesystem(container)
        file_client = fs.get_file_client(path)
        downloader = file_client.download_file()
        data = downloader.readall()
        logger.info("downloaded_bytes", container=container, path=path, size=len(data))
        return data

    def download_dataframe_parquet(self, container: str, path: str) -> pd.DataFrame:
        """Download a Parquet file and return as DataFrame."""
        raw = self.download_bytes(container, path)
        df = pd.read_parquet(io.BytesIO(raw))
        logger.info("downloaded_parquet", container=container, path=path, rows=len(df))
        return df

    def download_json(self, container: str, path: str) -> list | dict:
        """Download and parse a JSON file."""
        raw = self.download_bytes(container, path)
        return json.loads(raw.decode("utf-8"))

    def read_partitions(
        self,
        container: str,
        base_path: str,
        file_pattern: str = "*.parquet",
    ) -> pd.DataFrame:
        """
        Read all Parquet files under a partitioned directory
        and concatenate into a single DataFrame.
        """
        paths = self.list_paths(container, base_path)
        parquet_paths = [p for p in paths if p.endswith(".parquet")]

        if not parquet_paths:
            logger.warning("no_parquet_files_found", base_path=base_path)
            return pd.DataFrame()

        dfs: list[pd.DataFrame] = []
        for path in parquet_paths:
            dfs.append(self.download_dataframe_parquet(container, path))

        result = pd.concat(dfs, ignore_index=True)
        logger.info("read_partitions", base_path=base_path, total_rows=len(result))
        return result

    # ------------------------------------------------------------------
    # Checkpoint helpers
    # ------------------------------------------------------------------

    def save_checkpoint(
        self,
        container: str,
        job_name: str,
        checkpoint: dict,
    ) -> None:
        """Persist a processing checkpoint to ADLS."""
        path = f"_checkpoints/{job_name}/checkpoint.json"
        checkpoint["updated_at"] = datetime.utcnow().isoformat()
        self.upload_json(checkpoint, container, path)
        logger.info("checkpoint_saved", job=job_name)

    def load_checkpoint(
        self,
        container: str,
        job_name: str,
    ) -> Optional[dict]:
        """Load a previously saved checkpoint. Returns None if not found."""
        path = f"_checkpoints/{job_name}/checkpoint.json"
        try:
            return self.download_json(container, path)
        except Exception:
            logger.info("checkpoint_not_found", job=job_name)
            return None

    # ------------------------------------------------------------------
    # Deletion / cleanup
    # ------------------------------------------------------------------

    def delete_path(self, container: str, path: str, recursive: bool = False) -> None:
        """Delete a file or directory from ADLS."""
        fs = self.get_filesystem(container)
        path_client = fs.get_directory_client(path)
        path_client.delete_directory() if recursive else fs.get_file_client(path).delete_file()
        logger.info("deleted_path", container=container, path=path)
