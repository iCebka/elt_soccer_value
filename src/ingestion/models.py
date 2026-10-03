from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RemoteMetadata:
    url: str
    status_code: int
    etag: str | None
    last_modified: str | None
    content_length: int | None
    checked_at: str
    attempts: int
    method: str = "HEAD"


@dataclass(frozen=True)
class NotModifiedResult:
    metadata: RemoteMetadata


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    size_bytes: int
    source_file: str
    etag: str | None = None
    last_modified: str | None = None
    content_length: int | None = None
    captured_at: str | None = None
    status_code: int = 200
    attempts: int = 1


@dataclass(frozen=True)
class SourceVersion:
    run_id: str
    checksum: str
    rows: int
    source_file: str | None = None
    etag: str | None = None
    last_modified: str | None = None
    content_length: int | None = None
    captured_at: str | None = None


@dataclass(frozen=True)
class PreparedFile:
    files: tuple[Path, ...]
    headers: tuple[str, ...]
    rows: int
    schema_changed: bool


@dataclass(frozen=True)
class IngestionResult:
    asset: str
    run_id: str
    status: str
    checksum: str | None
    rows_read: int
    rows_loaded: int
    message: str | None = None
    http_attempts: int = 0

