from __future__ import annotations

import email.utils
import gzip
import hashlib
import time
from pathlib import Path
from urllib.parse import urlparse

import requests

from .models import DownloadResult


class PermanentDownloadError(RuntimeError):
    pass


class TransientDownloadError(RuntimeError):
    pass


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None and response.status_code == 429:
        value = response.headers.get("Retry-After")
        if value:
            if value.isdigit():
                return min(float(value), 300.0)
            parsed = email.utils.parsedate_to_datetime(value)
            return max(0.0, min(parsed.timestamp() - time.time(), 300.0))
    return min(2 ** (attempt - 1), 30.0)


def download_streaming(
    url: str,
    target_dir: Path,
    *,
    attempts: int = 4,
    session: requests.Session | None = None,
) -> DownloadResult:
    """Download a gzip snapshot with bounded retries and compute its original SHA-256."""
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = Path(urlparse(url).path).name
    if not filename.endswith(".csv.gz"):
        raise PermanentDownloadError(f"URL does not identify a .csv.gz file: {url}")
    final_path = target_dir / filename
    partial_path = final_path.with_suffix(final_path.suffix + ".part")
    owned_session = session is None
    http = session or requests.Session()
    try:
        for attempt in range(1, attempts + 1):
            response: requests.Response | None = None
            try:
                response = http.get(url, stream=True, timeout=(15, 180))
                if response.status_code in {404, 410}:
                    raise PermanentDownloadError(f"HTTP {response.status_code} for {url}")
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    raise TransientDownloadError(f"HTTP {response.status_code} for {url}")
                if not 200 <= response.status_code < 300:
                    raise PermanentDownloadError(f"HTTP {response.status_code} for {url}")
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
                return DownloadResult(final_path, digest.hexdigest(), size, filename)
            except PermanentDownloadError:
                partial_path.unlink(missing_ok=True)
                raise
            except (requests.Timeout, requests.ConnectionError, TransientDownloadError) as exc:
                partial_path.unlink(missing_ok=True)
                if attempt == attempts:
                    raise TransientDownloadError(
                        f"Download failed after {attempts} attempts: {url}: {exc}"
                    ) from exc
                time.sleep(_retry_delay(response, attempt))
            finally:
                if response is not None:
                    response.close()
    finally:
        if owned_session:
            http.close()
    raise AssertionError("unreachable")


def validate_complete_gzip(path: Path) -> None:
    """Read the full stream so CRC/truncation errors are detected."""
    with gzip.open(path, "rb") as handle:
        while handle.read(1024 * 1024):
            pass

