from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Protocol

from .config import Asset, AssetCatalog
from .extract import HttpSourceClient
from .models import (
    DownloadResult,
    IngestionResult,
    NotModifiedResult,
    PreparedFile,
    RemoteMetadata,
    SourceVersion,
)
from .prepare import prepare_ndjson


class Warehouse(Protocol):
    def begin_attempt(
        self,
        run_id: str,
        batch_id: str | None,
        asset: Asset,
        mode: str,
        logical_date: str | None,
        trigger_source: str,
    ) -> None: ...

    def latest_source_version(self, asset: Asset) -> SourceVersion | None: ...
    def record_probe(self, run_id: str, metadata: RemoteMetadata) -> None: ...
    def record_download(self, run_id: str, download: DownloadResult) -> None: ...
    def attach_file_metadata(self, run_id: str, prepared: PreparedFile) -> None: ...
    def find_success(self, asset: Asset, checksum: str) -> SourceVersion | None: ...
    def mark_skipped_unchanged(
        self,
        run_id: str,
        reference: SourceVersion,
        reason: str,
        http_attempts: int,
    ) -> None: ...

    def publish(
        self,
        run_id: str,
        asset: Asset,
        download: DownloadResult,
        prepared: PreparedFile,
        http_attempts: int,
    ) -> int: ...

    def mark_failed(self, run_id: str, error: str, http_attempts: int) -> None: ...


class SourceClient(Protocol):
    attempts_used: int

    def probe(self, url: str, *, prior_etag: str | None = None) -> RemoteMetadata: ...

    def download(
        self, url: str, target_dir: Path, *, prior_etag: str | None = None
    ) -> DownloadResult | NotModifiedResult: ...

    def close(self) -> None: ...


def _skipped_result(
    asset: Asset,
    run_id: str,
    reference: SourceVersion,
    reason: str,
    attempts: int,
) -> IngestionResult:
    return IngestionResult(
        asset=asset.name,
        run_id=run_id,
        status="SKIPPED_UNCHANGED",
        checksum=reference.checksum,
        rows_read=0,
        rows_loaded=0,
        message=reason,
        http_attempts=attempts,
    )


def ingest_asset(
    asset: Asset,
    catalog: AssetCatalog,
    warehouse: Warehouse,
    *,
    batch_id: str | None,
    mode: str,
    run_id: str | None = None,
    logical_date: str | None = None,
    trigger_source: str = "manual",
    source_client: SourceClient | None = None,
) -> IngestionResult:
    """Probe, conditionally download, and atomically publish one current snapshot."""
    if mode not in {"incremental", "backfill"}:
        raise ValueError("mode must be incremental or backfill")
    run_id = run_id or str(uuid.uuid4())
    warehouse.begin_attempt(
        run_id, batch_id, asset, mode, logical_date, trigger_source
    )
    work_dir = Path(tempfile.mkdtemp(prefix=f"transfermarkt-{asset.name}-"))
    client = source_client or HttpSourceClient(attempts=3)
    owns_client = source_client is None
    try:
        reference = warehouse.latest_source_version(asset)
        prior_etag = reference.etag if reference else None
        probe = client.probe(asset.url, prior_etag=prior_etag)
        warehouse.record_probe(run_id, probe)

        if reference and (
            probe.status_code == 304
            or (prior_etag is not None and probe.etag == prior_etag)
        ):
            reason = (
                "Conditional HEAD returned 304"
                if probe.status_code == 304
                else "Remote ETag matches the last successfully loaded version"
            )
            warehouse.mark_skipped_unchanged(
                run_id, reference, reason, client.attempts_used
            )
            return _skipped_result(
                asset, run_id, reference, reason, client.attempts_used
            )

        # A changed/missing ETag, unsupported HEAD, or first load requires GET.
        # Last-Modified and Content-Length alone are never treated as identity.
        outcome = client.download(
            asset.url,
            work_dir / "original",
            prior_etag=prior_etag,
        )
        if isinstance(outcome, NotModifiedResult):
            if reference is None:
                raise RuntimeError("HTTP 304 received without a successful reference version")
            warehouse.record_probe(run_id, outcome.metadata)
            reason = "Conditional GET returned 304"
            warehouse.mark_skipped_unchanged(
                run_id, reference, reason, client.attempts_used
            )
            return _skipped_result(
                asset, run_id, reference, reason, client.attempts_used
            )

        download = outcome
        warehouse.record_download(run_id, download)
        previous = warehouse.find_success(asset, download.sha256)
        if previous:
            reason = f"Checksum already loaded by run {previous.run_id}"
            warehouse.mark_skipped_unchanged(
                run_id, previous, reason, client.attempts_used
            )
            return _skipped_result(
                asset, run_id, previous, reason, client.attempts_used
            )

        prepared = prepare_ndjson(
            download.path,
            work_dir / "prepared",
            expected_headers=asset.headers,
            chunk_rows=catalog.chunk_rows,
        )
        warehouse.attach_file_metadata(run_id, prepared)
        loaded = warehouse.publish(
            run_id, asset, download, prepared, client.attempts_used
        )
        return IngestionResult(
            asset=asset.name,
            run_id=run_id,
            status="SUCCESS",
            checksum=download.sha256,
            rows_read=prepared.rows,
            rows_loaded=loaded,
            http_attempts=client.attempts_used,
        )
    except Exception as exc:
        try:
            warehouse.mark_failed(
                run_id,
                f"{type(exc).__name__}: {exc}",
                client.attempts_used,
            )
        except Exception as control_exc:
            raise RuntimeError(
                f"Ingestion failed ({exc}); additionally could not record failure ({control_exc})"
            ) from exc
        raise
    finally:
        if owns_client:
            client.close()
        shutil.rmtree(work_dir, ignore_errors=True)
