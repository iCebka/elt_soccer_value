from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

from .config import AssetCatalog, SnowflakeSettings
from .extract import download_streaming
from .pipeline import ingest_asset
from .prepare import prepare_ndjson
from .snowflake import SnowflakeWarehouse


def _redact_secrets(message: str) -> str:
    for name in ("SNOWFLAKE_PASSWORD", "KESTRA_PASSWORD", "KESTRA_API_TOKEN"):
        secret = os.getenv(name)
        if secret:
            message = message.replace(secret, "[REDACTED]")
    return message


def _format_error(exc: Exception) -> str:
    """Render an operational error without leaking configured secrets."""
    message = _redact_secrets(str(exc))
    return f"ERROR {type(exc).__name__}: {message}"


def _assets(value: str) -> list[str]:
    parsed = json.loads(value)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise argparse.ArgumentTypeError("assets must be a JSON array of strings")
    return parsed


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Transfermarkt to Snowflake Bronze")
    root.add_argument("--config", default="config/assets.yml")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("check", help="Check credentials and create/verify Bronze objects")
    audit = commands.add_parser("audit", help="Print recent Bronze audit rows as JSON")
    audit.add_argument("--limit", type=int, default=25)
    inspect = commands.add_parser(
        "inspect-source", help="Download and fully validate one real source without loading it"
    )
    inspect.add_argument("--asset", required=True)
    ingest = commands.add_parser("ingest", help="Ingest one complete asset snapshot")
    ingest.add_argument("--asset", required=True)
    ingest.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    ingest.add_argument("--batch-id")
    ingest.add_argument("--run-id")
    ingest.add_argument("--logical-date")
    ingest.add_argument("--trigger-source", default="manual")
    start = commands.add_parser("batch-start")
    start.add_argument("--batch-id", required=True)
    start.add_argument("--mode", choices=["incremental", "backfill"], required=True)
    start.add_argument("--assets", required=True, type=_assets)
    start.add_argument("--logical-date")
    start.add_argument("--trigger-source", default="manual")
    finish = commands.add_parser("batch-finish")
    finish.add_argument("--batch-id", required=True)
    finish.add_argument("--assets", required=True, type=_assets)
    finish.add_argument("--output", type=Path)
    finish.add_argument("--no-fail", action="store_true")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    catalog = AssetCatalog.load(args.config)
    if args.command == "inspect-source":
        asset = catalog.require(args.asset)
        work_dir = Path(tempfile.mkdtemp(prefix=f"transfermarkt-inspect-{asset.name}-"))
        try:
            download = download_streaming(asset.url, work_dir / "original")
            prepared = prepare_ndjson(
                download.path,
                work_dir / "prepared",
                expected_headers=asset.headers,
                chunk_rows=catalog.chunk_rows,
            )
            print(
                json.dumps(
                    {
                        "status": "SOURCE_VALIDATED_NOT_LOADED",
                        "asset": asset.name,
                        "url": asset.url,
                        "source_file": download.source_file,
                        "sha256": download.sha256,
                        "size_bytes": download.size_bytes,
                        "rows": prepared.rows,
                        "headers": prepared.headers,
                        "schema_changed": prepared.schema_changed,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)
    settings = SnowflakeSettings.from_env()
    if hasattr(args, "assets"):
        for name in args.assets:
            catalog.require(name)
    with SnowflakeWarehouse(settings, catalog) as warehouse:
        if args.command == "check":
            warehouse.check_connection()
        warehouse.ensure_objects()
        if args.command == "check":
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "connection_check": "SELECT 1",
                        "schema": settings.qualified_schema,
                    }
                )
            )
            return 0
        if args.command == "audit":
            rendered = json.dumps(
                warehouse.recent_audit(args.limit),
                ensure_ascii=False,
                indent=2,
                default=str,
            )
            print(_redact_secrets(rendered))
            return 0
        if args.command == "batch-start":
            warehouse.start_batch(
                args.batch_id,
                args.mode,
                args.assets,
                args.logical_date,
                args.trigger_source,
            )
            print(json.dumps({"batch_id": args.batch_id, "status": "RUNNING"}))
            return 0
        if args.command == "batch-finish":
            summary = warehouse.finish_batch(args.batch_id, args.assets)
            rendered = json.dumps(summary, ensure_ascii=False, indent=2)
            if args.output:
                args.output.write_text(rendered + "\n", encoding="utf-8")
            print(rendered)
            print("::" + json.dumps({"outputs": summary}, separators=(",", ":")) + "::")
            return 0 if summary["status"] == "SUCCESS" or args.no_fail else 2
        result = ingest_asset(
            catalog.require(args.asset),
            catalog,
            warehouse,
            batch_id=args.batch_id,
            mode=args.mode,
            run_id=args.run_id or str(uuid.uuid4()),
            logical_date=args.logical_date,
            trigger_source=args.trigger_source,
        )
        print(json.dumps(result.__dict__, ensure_ascii=False))
        print(
            "::"
            + json.dumps({"outputs": result.__dict__}, ensure_ascii=False, separators=(",", ":"))
            + "::"
        )
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(_format_error(exc), file=sys.stderr)
        raise SystemExit(1)
