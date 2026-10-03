from __future__ import annotations

import csv
import gzip
import json
from pathlib import Path

from .models import PreparedFile


class CsvFormatError(RuntimeError):
    pass


def prepare_ndjson(
    gzip_path: Path,
    output_dir: Path,
    *,
    expected_headers: tuple[str, ...],
    chunk_rows: int,
) -> PreparedFile:
    """Convert a complete CSV snapshot to chunked NDJSON without typing its fields."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_files: list[Path] = []
    output = None
    rows = 0
    try:
        with gzip.open(gzip_path, mode="rt", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            if reader.fieldnames is None:
                raise CsvFormatError("CSV has no header")
            headers = tuple(reader.fieldnames)
            if any(not header for header in headers) or len(headers) != len(set(headers)):
                raise CsvFormatError(f"CSV has empty or duplicate headers: {headers!r}")
            for row_number, record in enumerate(reader, start=1):
                if None in record:
                    raise CsvFormatError(
                        f"CSV record {row_number} contains more fields than its header"
                    )
                if any(value is None for value in record.values()):
                    raise CsvFormatError(
                        f"CSV record {row_number} contains fewer fields than its header"
                    )
                if output is None or (row_number - 1) % chunk_rows == 0:
                    if output is not None:
                        output.close()
                    path = output_dir / f"part-{len(output_files):05d}.ndjson"
                    output_files.append(path)
                    output = path.open("w", encoding="utf-8", newline="\n")
                json.dump(
                    {"raw_record": record, "source_row_number": row_number},
                    output,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                output.write("\n")
                rows = row_number
    except (gzip.BadGzipFile, EOFError, UnicodeDecodeError, csv.Error) as exc:
        raise CsvFormatError(f"Invalid gzip/CSV content in {gzip_path.name}: {exc}") from exc
    finally:
        if output is not None:
            output.close()
    # An empty but valid CSV still needs one load file so the snapshot can be recorded.
    if not output_files:
        path = output_dir / "part-00000.ndjson"
        path.touch()
        output_files.append(path)
    return PreparedFile(
        files=tuple(output_files),
        headers=headers,
        rows=rows,
        schema_changed=headers != expected_headers,
    )
