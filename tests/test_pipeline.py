from __future__ import annotations

import csv
import gzip
import hashlib
import shutil

import pytest

from ingestion.config import Asset, AssetCatalog
from ingestion.models import DownloadResult
from ingestion.pipeline import ingest_asset


class FakeWarehouse:
    def __init__(self):
        self.successes = {}
        self.attempts = {}
        self.publish_calls = 0
        self.fail_next_publish = False

    def begin_attempt(self, run_id, batch_id, asset, mode):
        self.attempts[run_id] = {"status": "RUNNING", "asset": asset.name}

    def attach_file_metadata(self, run_id, download, prepared):
        self.attempts[run_id].update(checksum=download.sha256, rows=prepared.rows)

    def find_success(self, asset, checksum):
        return self.successes.get((asset.name, checksum))

    def mark_skipped(self, run_id, rows, successful_run_id):
        self.attempts[run_id]["status"] = "SKIPPED"

    def publish(self, run_id, asset, download, prepared):
        self.publish_calls += 1
        if self.fail_next_publish:
            self.fail_next_publish = False
            raise RuntimeError("simulated partial failure")
        self.successes[(asset.name, download.sha256)] = (run_id, prepared.rows)
        self.attempts[run_id]["status"] = "SUCCESS"
        return prepared.rows

    def mark_failed(self, run_id, error):
        self.attempts[run_id]["status"] = "FAILED"
        self.attempts[run_id]["error"] = error


def _fixture(tmp_path):
    source = tmp_path / "source.csv.gz"
    with gzip.open(source, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "value"])
        writer.writerow(["1", "á"])
        writer.writerow(["2", ""])
    sha = hashlib.sha256(source.read_bytes()).hexdigest()

    def downloader(_url, target_dir):
        target_dir.mkdir(parents=True)
        target = target_dir / "sample.csv.gz"
        shutil.copyfile(source, target)
        return DownloadResult(target, sha, target.stat().st_size, target.name)

    asset = Asset("sample", "https://example.test/sample.csv.gz", ("id", "value"))
    catalog = AssetCatalog({"sample": asset}, chunk_rows=1)
    return asset, catalog, downloader


def test_second_identical_snapshot_is_skipped_without_duplicate_publish(tmp_path):
    asset, catalog, downloader = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    first = ingest_asset(
        asset, catalog, warehouse, batch_id="batch-1", mode="incremental",
        run_id="run-1", downloader=downloader
    )
    second = ingest_asset(
        asset, catalog, warehouse, batch_id="batch-2", mode="incremental",
        run_id="run-2", downloader=downloader
    )
    assert first.status == "SUCCESS"
    assert second.status == "SKIPPED"
    assert warehouse.publish_calls == 1
    assert len(warehouse.successes) == 1


def test_failed_publication_is_recorded_and_a_retry_recovers(tmp_path):
    asset, catalog, downloader = _fixture(tmp_path)
    warehouse = FakeWarehouse()
    warehouse.fail_next_publish = True
    with pytest.raises(RuntimeError, match="simulated partial failure"):
        ingest_asset(
            asset, catalog, warehouse, batch_id="batch-1", mode="backfill",
            run_id="run-failed", downloader=downloader
        )
    recovered = ingest_asset(
        asset, catalog, warehouse, batch_id="batch-1", mode="backfill",
        run_id="run-retry", downloader=downloader
    )
    assert warehouse.attempts["run-failed"]["status"] == "FAILED"
    assert recovered.status == "SUCCESS"
    assert warehouse.publish_calls == 2

