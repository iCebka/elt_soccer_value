from __future__ import annotations

import email.utils
import gzip
import hashlib
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests

from .models import DownloadResult, NotModifiedResult, RemoteMetadata


class PermanentDownloadError(RuntimeError):
    pass


class TransientDownloadError(RuntimeError):
    pass


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        value = response.headers.get("Retry-After")
        if value:
            if value.isdigit():
                return min(float(value), 120.0)
            try:
                parsed = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                parsed = None
            if parsed is not None:
                return max(0.0, min(parsed.timestamp() - time.time(), 120.0))
    return min(10.0 * (2 ** (attempt - 1)), 120.0)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _content_length(headers: requests.structures.CaseInsensitiveDict) -> int | None:
    value = headers.get("Content-Length")
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


class HttpSourceClient:
    """HEAD + conditional GET client with one bounded retry budget per ingestion."""

    def __init__(
        self,
        *,
        attempts: int = 3,
        session: requests.Session | None = None,
        sleep=time.sleep,
    ):
        if attempts < 1:
            raise ValueError("attempts must be at least 1")
        self.max_attempts = attempts
        self.attempts_used = 0
        self._owned_session = session is None
        self.session = session or requests.Session()
        self._sleep = sleep

    def close(self) -> None:
        if self._owned_session:
            self.session.close()

    def __enter__(self) -> "HttpSourceClient":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    @staticmethod
    def _metadata(
        url: str, response: requests.Response, checked_at: str, attempts: int, method: str
    ) -> RemoteMetadata:
        return RemoteMetadata(
            url=url,
            status_code=response.status_code,
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
            content_length=_content_length(response.headers),
            checked_at=checked_at,
            attempts=attempts,
            method=method,
        )

    def _request(self, method: str, url: str, **kwargs) -> tuple[requests.Response, str]:
        operation_attempt = 0
        while self.attempts_used < self.max_attempts:
            self.attempts_used += 1
            operation_attempt += 1
            total_attempt = self.attempts_used
            response: requests.Response | None = None
            try:
                response = self.session.request(method, url, **kwargs)
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    if total_attempt >= self.max_attempts:
                        response.close()
                        raise TransientDownloadError(
                            f"HTTP {response.status_code} for {url} after "
                            f"{self.max_attempts} total attempts"
                        )
                    delay = _retry_delay(response, operation_attempt)
                    response.close()
                    self._sleep(delay)
                    continue
                return response, _utc_now()
            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                if response is not None:
                    response.close()
                if total_attempt >= self.max_attempts:
                    raise TransientDownloadError(
                        f"HTTP request failed for {url} after "
                        f"{self.max_attempts} total attempts: {type(exc).__name__}"
                    ) from exc
                self._sleep(_retry_delay(response, operation_attempt))
        raise TransientDownloadError(
            f"HTTP retry budget exhausted for {url} after {self.max_attempts} total attempts"
        )

    def probe(self, url: str, *, prior_etag: str | None = None) -> RemoteMetadata:
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        if prior_etag is not None:
            # ETags are opaque: retain weak prefixes and quotes exactly as received.
            headers["If-None-Match"] = prior_etag
        response, checked_at = self._request(
            "HEAD", url, headers=headers, allow_redirects=True, timeout=(15, 30)
        )
        try:
            metadata = self._metadata(
                url, response, checked_at, self.attempts_used, "HEAD"
            )
            if response.status_code == 304:
                if prior_etag is None:
                    raise PermanentDownloadError(
                        f"Unexpected HTTP 304 without If-None-Match for {url}"
                    )
                if metadata.etag is None:
                    metadata = RemoteMetadata(
                        **{**metadata.__dict__, "etag": prior_etag}
                    )
                return metadata
            if response.status_code in {405, 501}:
                # The server does not support HEAD; GET + SHA-256 is conservative.
                return metadata
            if not 200 <= response.status_code < 300:
                raise PermanentDownloadError(f"HTTP {response.status_code} for {url}")
            return metadata
        finally:
            response.close()

    def download(
        self,
        url: str,
        target_dir: Path,
        *,
        prior_etag: str | None = None,
    ) -> DownloadResult | NotModifiedResult:
        target_dir.mkdir(parents=True, exist_ok=True)
        filename = Path(urlparse(url).path).name
        if not filename.endswith(".csv.gz"):
            raise PermanentDownloadError(f"URL does not identify a .csv.gz file: {url}")
        final_path = target_dir / filename
        partial_path = final_path.with_suffix(final_path.suffix + ".part")
        headers = {"Cache-Control": "no-cache", "Pragma": "no-cache"}
        if prior_etag is not None:
            headers["If-None-Match"] = prior_etag

        stream_failures = 0
        while True:
            response: requests.Response | None = None
            try:
                response, captured_at = self._request(
                    "GET",
                    url,
                    headers=headers,
                    stream=True,
                    allow_redirects=True,
                    timeout=(15, 120),
                )
                metadata = self._metadata(
                    url, response, captured_at, self.attempts_used, "GET"
                )
                if response.status_code == 304:
                    if prior_etag is None:
                        raise PermanentDownloadError(
                            f"Unexpected HTTP 304 without If-None-Match for {url}"
                        )
                    if metadata.etag is None:
                        metadata = RemoteMetadata(
                            **{**metadata.__dict__, "etag": prior_etag}
                        )
                    return NotModifiedResult(metadata)
                if not 200 <= response.status_code < 300:
                    raise PermanentDownloadError(
                        f"HTTP {response.status_code} for {url}"
                    )
                digest = hashlib.sha256()
                size = 0
                with partial_path.open("wb") as handle:
                    for block in response.iter_content(chunk_size=1024 * 1024):
                        if block:
                            handle.write(block)
                            digest.update(block)
                            size += len(block)
                if size < 2:
                    raise PermanentDownloadError(f"Empty or truncated response from {url}")
                with partial_path.open("rb") as handle:
                    if handle.read(2) != b"\x1f\x8b":
                        raise PermanentDownloadError(f"Response is not gzip content: {url}")
                partial_path.replace(final_path)
                completed_at = _utc_now()
                return DownloadResult(
                    final_path,
                    digest.hexdigest(),
                    size,
                    filename,
                    etag=metadata.etag,
                    last_modified=metadata.last_modified,
                    content_length=metadata.content_length,
                    captured_at=completed_at,
                    status_code=response.status_code,
                    attempts=self.attempts_used,
                )
            except PermanentDownloadError:
                partial_path.unlink(missing_ok=True)
                raise
            except (
                requests.Timeout,
                requests.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
            ) as exc:
                # A streamed body can fail after headers; retry the complete GET only
                # while the same three-request budget still has capacity.
                partial_path.unlink(missing_ok=True)
                if self.attempts_used >= self.max_attempts:
                    raise TransientDownloadError(
                        f"Download failed for {url} after "
                        f"{self.max_attempts} total attempts: {type(exc).__name__}"
                    ) from exc
                stream_failures += 1
                self._sleep(_retry_delay(response, stream_failures))
            finally:
                if response is not None:
                    response.close()


def download_streaming(
    url: str,
    target_dir: Path,
    *,
    attempts: int = 3,
    session: requests.Session | None = None,
) -> DownloadResult:
    """Compatibility helper for an unconditional GET with bounded retries."""
    with HttpSourceClient(attempts=attempts, session=session) as client:
        result = client.download(url, target_dir)
    if isinstance(result, NotModifiedResult):
        raise AssertionError("an unconditional GET cannot return 304")
    return result


def validate_complete_gzip(path: Path) -> None:
    """Read the full stream so CRC/truncation errors are detected."""
    with gzip.open(path, "rb") as handle:
        while handle.read(1024 * 1024):
            pass

