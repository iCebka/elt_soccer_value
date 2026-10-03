from __future__ import annotations

import gzip

import pytest
import requests

from ingestion.extract import (
    HttpSourceClient,
    PermanentDownloadError,
    TransientDownloadError,
    download_streaming,
)
from ingestion.models import NotModifiedResult


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
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def close(self):
        pass


def test_429_honors_retry_after_then_succeeds(tmp_path):
    waits = []
    body = gzip.compress(b'id,name\n1,"Garc\xc3\xada"\n')
    session = FakeSession(
        [FakeResponse(429, headers={"Retry-After": "3"}), FakeResponse(200, body)]
    )
    client = HttpSourceClient(attempts=3, session=session, sleep=waits.append)
    result = client.download("https://example.test/sample.csv.gz", tmp_path)
    assert len(session.calls) == 2
    assert waits == [3.0]
    assert result.path.read_bytes() == body


def test_404_is_permanent_and_not_retried(tmp_path):
    session = FakeSession([FakeResponse(404)])
    with pytest.raises(PermanentDownloadError, match="HTTP 404"):
        download_streaming(
            "https://example.test/missing.csv.gz", tmp_path, attempts=3, session=session
        )
    assert len(session.calls) == 1


def test_timeout_retries_with_ten_second_initial_backoff(tmp_path):
    waits = []
    body = gzip.compress(b"id,name\n1,ok\n")
    session = FakeSession([requests.Timeout("slow"), FakeResponse(200, body)])
    client = HttpSourceClient(attempts=3, session=session, sleep=waits.append)

    result = client.download("https://example.test/sample.csv.gz", tmp_path)

    assert result.path.read_bytes() == body
    assert len(session.calls) == 2
    assert waits == [10.0]


def test_conditional_head_retains_opaque_etag_and_accepts_304():
    opaque = 'W/"a-b-c"'
    session = FakeSession([FakeResponse(304)])
    client = HttpSourceClient(attempts=3, session=session, sleep=lambda _delay: None)
    metadata = client.probe("https://example.test/sample.csv.gz", prior_etag=opaque)

    assert metadata.status_code == 304
    assert metadata.etag == opaque
    assert session.calls[0][2]["headers"]["If-None-Match"] == opaque


def test_conditional_get_304_has_no_download(tmp_path):
    opaque = '"v1"'
    session = FakeSession([FakeResponse(304, headers={"ETag": opaque})])
    client = HttpSourceClient(attempts=3, session=session, sleep=lambda _delay: None)
    result = client.download(
        "https://example.test/sample.csv.gz", tmp_path, prior_etag=opaque
    )

    assert isinstance(result, NotModifiedResult)
    assert not list(tmp_path.glob("*.csv.gz"))
    assert session.calls[0][2]["headers"]["If-None-Match"] == opaque


def test_head_and_get_share_three_attempt_budget(tmp_path):
    waits = []
    session = FakeSession(
        [
            FakeResponse(200, headers={"ETag": '"v2"'}),
            FakeResponse(500),
            FakeResponse(503),
        ]
    )
    client = HttpSourceClient(attempts=3, session=session, sleep=waits.append)
    client.probe("https://example.test/sample.csv.gz", prior_etag='"v1"')
    with pytest.raises(TransientDownloadError, match="3 total attempts"):
        client.download(
            "https://example.test/sample.csv.gz", tmp_path, prior_etag='"v1"'
        )

    assert len(session.calls) == 3
    assert waits == [10.0]
