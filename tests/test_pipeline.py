from __future__ import annotations

import csv
import gzip
import hashlib
import shutil

import pytest

from ingestion.config import Asset, AssetCatalog
from ingestion.models import (
    DownloadResult,
    NotModifiedResult,
    RemoteMetadata,
    SourceVersion,
)
from ingestion.pipeline import ingest_asset


class FakeWarehouse:
    def __init__(self):
        self.successes = {}
        self.latest = None
        self.attempts = {}
        self.publish_calls = 0
        self.fail_next_publish = False
        self.raw_versions = set()

    def begin_attempt(
        self, run_id, batch_id, asset, mode, logical_date, trigger_source
    ):
        self.attempts[run_id] = {
            "status": "RUNNING",
            "asset": asset.name,
            "logical_date": logical_date,
            "trigger_source": trigger_source,
        }

    def latest_source_version(self, asset):
        return self.latest

    def record_probe(self, run_id, metadata):
        self.attempts[run_id][metadata.method.lower()] = metadata

    def record_download(self, run_id, download):
        self.attempts[run_id].update(
            checksum=download.sha256,
            download=download,
        )

    def attach_file_metadata(self, run_id, prepared):
        self.attempts[run_id].update(rows=prepared.rows)

    def find_success(self, asset, checksum):
        return self.successes.get((asset.name, asset.url, checksum))

    def mark_skipped_unchanged(self, run_id, reference, reason, http_attempts):
        probe = self.attempts[run_id].get("head") or self.attempts[run_id].get("get")
        download = self.attempts[run_id].get("download")
        etag = download.etag if download and download.etag else (probe.etag if probe else None)
        self.latest = SourceVersion(
            run_id=run_id,
            checksum=reference.checksum,
            rows=reference.rows,
            source_file=reference.source_file,
            etag=etag or reference.etag,
        )
        self.attempts[run_id].update(
            status="SKIPPED_UNCHANGED",
            reason=reason,
            http_attempts=http_attempts,
        )

    def publish(self, run_id, asset, download, prepared, http_attempts):
        self.publish_calls += 1
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise RuntimeError("simulated partial failure")
        key = (asset.name, asset.url, download.sha256)
        version = SourceVersion(
            run_id=run_id,
            checksum=download.sha256,
            rows=prepared.rows,
            source_file=download.source_file,
            etag=download.etag,
            last_modified=download.last_modified,
            content_length=download.content_length,
            captured_at=download.captured_at,
        )
        self.successes[key] = version
        self.latest = version
        self.raw_versions.add(key)
        self.attempts[run_id].update(
            status="SUCCESS", http_attempts=http_attempts
        )
        return prepared.rows

    def mark_failed(self, run_id, error, http_attempts):
        # Never advance self.latest on failure.
        self.attempts[run_id].update(
            status="FAILED", error=error, http_attempts=http_attempts
        )


class FakeSourceClient:
    def __init__(self, probe, outcome=None):
        self.probe_result = probe
        self.outcome = outcome
        self.attempts_used = 0
        self.probe_calls = 0
        self.download_calls = 0
        self.prior_etags = []

    def probe(self, _url, *, prior_etag=None):
        self.probe_calls += 1
        self.attempts_used += 1
        self.prior_etags.append(("HEAD", prior_etag))
        return self.probe_result

    def download(self, _url, _target_dir, *, prior_etag=None):
        self.download_calls += 1
        self.attempts_used += 1
        self.prior_etags.append(("GET", prior_etag))
        if isinstance(self.outcome, DownloadResult):
            return DownloadResult(
                **{**self.outcome.__dict__, "attempts": self.attempts_used}
            )
        return self.outcome

    def close(self):
        pass


def _fixture(tmp_path, suffix="v1", etag='"v1"'):
    source = tmp_path / f"source-{suffix}.csv.gz"
    with gzip.open(source, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "value"])
        writer.writerow(["1", f"value-{suffix}"])
        writer.writerow(["2", ""])
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    target = tmp_path / f"download-{suffix}.csv.gz"
    shutil.copyfile(source, target)
    download = DownloadResult(
        target,
        sha,
        target.stat().st_size,
        "sample.csv.gz",
        etag=etag,
        last_modified="Fri, 02 Oct 2026 10:00:00 GMT",
        content_length=target.stat().st_size,
        captured_at="2026-10-02T10:00:01+00:00",
    )
    asset = Asset("sample", "https://example.test/sample.csv.gz", ("id", "value"))
    catalog = AssetCatalog({"sample": asset}, chunk_rows=1)
    return asset, catalog, download


def _probe(status=200, etag='"v1"', size=123):
    return RemoteMetadata(
        url="https://example.test/sample.csv.gz",
        status_code=status,
        etag=etag,
        last_modified="Fri, 02 Oct 2026 10:00:00 GMT",
        content_length=size,
        checked_at="2026-10-02T10:00:00+00:00",
        attempts=1,
    )


def _ingest(asset, catalog, warehouse, client, run_id, mode="incremental"):
    return ingest_asset(
        asset,
        catalog,
        warehouse,
        batch_id="batch-1",
        mode=mode,
        run_id=run_id,
        logical_date="2026-09-01T06:00:00-05:00",
        trigger_source="monthly_first_day" if mode == "backfill" else "manual",
        source_client=client,
    )


def test_first_load_downloads_and_records_logical_date(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    client = FakeSourceClient(_probe(etag='"v1"'), download)

    result = _ingest(asset, catalog, warehouse, client, "run-1")

    assert result.status == "SUCCESS"
    assert client.download_calls == 1
    assert warehouse.attempts["run-1"]["logical_date"].startswith("2026-09")
    assert warehouse.publish_calls == 1


def test_equal_etag_skips_without_download(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    _ingest(asset, catalog, warehouse, FakeSourceClient(_probe(), download), "run-1")
    second = FakeSourceClient(_probe(etag='"v1"'))

    result = _ingest(asset, catalog, warehouse, second, "run-2")

    assert result.status == "SKIPPED_UNCHANGED"
    assert second.download_calls == 0
    assert second.prior_etags == [("HEAD", '"v1"')]
    assert warehouse.publish_calls == 1


def test_conditional_head_304_skips_without_download(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    _ingest(asset, catalog, warehouse, FakeSourceClient(_probe(), download), "run-1")
    second = FakeSourceClient(_probe(status=304, etag='"v1"'))

    result = _ingest(asset, catalog, warehouse, second, "run-2")

    assert result.status == "SKIPPED_UNCHANGED"
    assert second.download_calls == 0


def test_changed_etag_loads_only_new_version(tmp_path):
    asset, catalog, download_v1 = _fixture(tmp_path, "v1", '"v1"')
    _, _, download_v2 = _fixture(tmp_path, "v2", '"v2"')
    warehouse = FakeWarehouse()
    _ingest(asset, catalog, warehouse, FakeSourceClient(_probe(), download_v1), "run-1")

    result = _ingest(
        asset,
        catalog,
        warehouse,
        FakeSourceClient(_probe(etag='"v2"'), download_v2),
        "run-2",
    )

    assert result.status == "SUCCESS"
    assert warehouse.publish_calls == 2
    assert len(warehouse.raw_versions) == 2


def test_conditional_get_304_skips(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    _ingest(asset, catalog, warehouse, FakeSourceClient(_probe(), download), "run-1")
    get_304 = NotModifiedResult(
        RemoteMetadata(
            asset.url,
            304,
            '"v1"',
            None,
            None,
            "2026-10-02T10:00:00+00:00",
            2,
            method="GET",
        )
    )
    second = FakeSourceClient(_probe(etag='"changed-between-head-and-get"'), get_304)

    result = _ingest(asset, catalog, warehouse, second, "run-2")

    assert result.status == "SKIPPED_UNCHANGED"
    assert second.download_calls == 1
    assert warehouse.publish_calls == 1


def test_missing_validators_downloads_and_uses_checksum(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    _ingest(asset, catalog, warehouse, FakeSourceClient(_probe(), download), "run-1")
    no_validators = RemoteMetadata(
        asset.url,
        200,
        None,
        "Fri, 02 Oct 2026 10:00:00 GMT",
        download.size_bytes,
        "2026-10-02T11:00:00+00:00",
        1,
    )
    same_without_etag = DownloadResult(
        **{**download.__dict__, "etag": None, "last_modified": None}
    )
    client = FakeSourceClient(no_validators, same_without_etag)

    result = _ingest(asset, catalog, warehouse, client, "run-2")

    assert client.download_calls == 1
    assert result.status == "SKIPPED_UNCHANGED"
    assert warehouse.publish_calls == 1


def test_failed_publication_does_not_advance_checkpoint_and_recovery_is_idempotent(tmp_path):
    asset, catalog, download = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    warehouse.fail_next_publish = True

    with pytest.raises(RuntimeError, match="simulated partial failure"):
        _ingest(
            asset,
            catalog,
            warehouse,
            FakeSourceClient(_probe(), download),
            "run-failed",
            mode="backfill",
        )
    assert warehouse.latest is None
    assert warehouse.attempts["run-failed"]["status"] == "FAILED"

    recovered = _ingest(
        asset,
        catalog,
        warehouse,
        FakeSourceClient(_probe(), download),
        "run-retry",
        mode="backfill",
    )
    unchanged = _ingest(
        asset,
        catalog,
        warehouse,
        FakeSourceClient(_probe()),
        "run-after-recovery",
        mode="backfill",
    )

    assert recovered.status == "SUCCESS"
    assert unchanged.status == "SKIPPED_UNCHANGED"
    assert len(warehouse.raw_versions) == 1
