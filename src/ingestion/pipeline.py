from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Callable, Protocol

from .config import Asset, AssetCatalog
from .extract import download_streaming
from .models import DownloadResult, IngestionResult, PreparedFile
from .prepare import prepare_ndjson


class Warehouse(Protocol):
    def begin_attempt(self, run_id: str, batch_id: str | None, asset: Asset, mode: str) -> None: ...
    def attach_file_metadata(self, run_id: str, download: DownloadResult, prepared: PreparedFile) -> None: ...
    def find_success(self, asset: Asset, checksum: str) -> tuple[str, int] | None: ...
    def mark_skipped(self, run_id: str, rows: int, successful_run_id: str) -> None: ...
    def publish(self, run_id: str, asset: Asset, download: DownloadResult, prepared: PreparedFile) -> int: ...
    def mark_failed(self, run_id: str, error: str) -> None: ...


Downloader = Callable[[str, Path], DownloadResult]


def ingest_asset(
    asset: Asset,
    catalog: AssetCatalog,
    warehouse: Warehouse,
    *,
    batch_id: str | None,
    mode: str,
    run_id: str | None = None,
    downloader: Downloader = download_streaming,
) -> IngestionResult:
    if mode not in {"incremental", "backfill"}:
        raise ValueError("mode must be incremental or backfill")
    run_id = run_id or str(uuid.uuid4())
    warehouse.begin_attempt(run_id, batch_id, asset, mode)
    work_dir = Path(tempfile.mkdtemp(prefix=f"transfermarkt-{asset.name}-"))
    try:
        download = downloader(asset.url, work_dir / "original")
        prepared = prepare_ndjson(
            download.path,
            work_dir / "prepared",
            expected_headers=asset.headers,
            chunk_rows=catalog.chunk_rows,
        )
        warehouse.attach_file_metadata(run_id, download, prepared)
        previous = warehouse.find_success(asset, download.sha256)
        if previous:
            previous_run_id, previous_rows = previous
            warehouse.mark_skipped(run_id, previous_rows, previous_run_id)
            return IngestionResult(
                asset.name,
                run_id,
                "SKIPPED",
                download.sha256,
                prepared.rows,
                previous_rows,
                f"Content already loaded by {previous_run_id}",
            )
        loaded = warehouse.publish(run_id, asset, download, prepared)
        return IngestionResult(
            asset.name, run_id, "SUCCESS", download.sha256, prepared.rows, loaded
        )
    except Exception as exc:
        try:
            warehouse.mark_failed(run_id, f"{type(exc).__name__}: {exc}")
        except Exception as control_exc:
            raise RuntimeError(
                f"Ingestion failed ({exc}); additionally could not record failure ({control_exc})"
            ) from exc
        raise
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)

