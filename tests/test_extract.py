from __future__ import annotations

import gzip

import pytest

from ingestion.extract import PermanentDownloadError, download_streaming


class FakeResponse:
    def __init__(self, status_code, body=b"", headers=None):
        self.status_code = status_code
        self.body = body
        self.headers = headers or {}

    def iter_content(self, chunk_size):
        yield self.body

    def close(self):
        pass


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        return self.responses.pop(0)


def test_429_honors_retry_after_then_succeeds(tmp_path, monkeypatch):
    waits = []
    monkeypatch.setattr("ingestion.extract.time.sleep", waits.append)
    body = gzip.compress(b'id,name\n1,"Garc\xc3\xada"\n')
    session = FakeSession(
        [FakeResponse(429, headers={"Retry-After": "3"}), FakeResponse(200, body)]
    )
    result = download_streaming(
        "https://example.test/sample.csv.gz", tmp_path, attempts=2, session=session
    )
    assert session.calls == 2
    assert waits == [3.0]
    assert result.path.read_bytes() == body


def test_404_is_permanent_and_not_retried(tmp_path):
    session = FakeSession([FakeResponse(404)])
    with pytest.raises(PermanentDownloadError, match="HTTP 404"):
        download_streaming(
            "https://example.test/missing.csv.gz", tmp_path, attempts=4, session=session
        )
    assert session.calls == 1

