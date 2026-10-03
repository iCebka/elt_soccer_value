from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DownloadResult:
    path: Path
    sha256: str
    size_bytes: int
    source_file: str


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

