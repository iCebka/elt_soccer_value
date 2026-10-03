from __future__ import annotations

import csv
import gzip
import json

import pytest

from ingestion.prepare import CsvFormatError, prepare_ndjson


def _gzip_csv(path, headers, rows):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(headers)
        writer.writerows(rows)


def test_preserves_quotes_commas_newlines_unicode_and_empty_strings(tmp_path):
    source = tmp_path / "edge.csv.gz"
    headers = ("id", "name", "notes", "empty")
    _gzip_csv(
        source,
        headers,
        [
            ("1", "García, José", "línea uno\nlínea dos", ""),
            ("2", 'Ada "The Ten"', "café", ""),
        ],
    )

    result = prepare_ndjson(
        source, tmp_path / "out", expected_headers=headers, chunk_rows=1
    )

    assert result.rows == 2
    assert len(result.files) == 2
    assert result.schema_changed is False
    records = [
        json.loads(line)
        for file in result.files
        for line in file.read_text(encoding="utf-8").splitlines()
    ]
    assert records[0]["raw_record"]["name"] == "García, José"
    assert records[0]["raw_record"]["notes"] == "línea uno\nlínea dos"
    assert records[0]["raw_record"]["empty"] == ""
    assert records[1]["raw_record"]["name"] == 'Ada "The Ten"'
    assert [record["source_row_number"] for record in records] == [1, 2]


def test_records_schema_drift_without_dropping_new_columns(tmp_path):
    source = tmp_path / "drift.csv.gz"
    _gzip_csv(source, ("id", "new_column"), [("1", "kept")])
    result = prepare_ndjson(
        source, tmp_path / "out", expected_headers=("id",), chunk_rows=100
    )
    payload = json.loads(result.files[0].read_text(encoding="utf-8"))
    assert result.schema_changed is True
    assert result.headers == ("id", "new_column")
    assert payload["raw_record"]["new_column"] == "kept"


def test_rejects_invalid_gzip(tmp_path):
    source = tmp_path / "bad.csv.gz"
    source.write_bytes(b"not gzip")
    with pytest.raises(CsvFormatError):
        prepare_ndjson(source, tmp_path / "out", expected_headers=("id",), chunk_rows=10)

