from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from .config import SnowflakeSettings, identifier
from .snowflake import connect_snowflake


REQUIRED_ASSETS = (
    "appearances",
    "club_games",
    "clubs",
    "competitions",
    "countries",
    "game_events",
    "game_lineups",
    "games",
    "national_teams",
    "player_valuations",
    "players",
    "transfers",
)

FINAL_MODELS = (
    "silver_players_current",
    "silver_games_enriched",
    "silver_player_match",
    "silver_player_valuations_enriched",
)

FINAL_MODEL_KEYS = {
    "silver_players_current": ("PLAYER_ID",),
    "silver_games_enriched": ("GAME_ID",),
    "silver_player_match": ("APPEARANCE_ID",),
    "silver_player_valuations_enriched": ("PLAYER_ID", "VALUATION_DATE"),
}

RUNS_TABLE = "TRANSFERMARKT_SILVER_RUNS"
SOURCES_TABLE = "TRANSFERMARKT_SILVER_RUN_SOURCES"
CHECKPOINT_TABLE = "TRANSFERMARKT_SILVER_CHECKPOINT"
QUARANTINE_TABLE = "BASE_TM__QUARANTINE"
RECONCILIATION_TABLE = "BASE_TM__RECONCILIATION"

FINGERPRINT_ROOT_FILES = (
    "dbt_project.yml",
    "profiles.yml",
    "packages.yml",
    "packages.lock",
    "dependencies.yml",
)
FINGERPRINT_DIRECTORIES = (
    "models",
    "macros",
    "seeds",
    "tests",
    "snapshots",
    "analyses",
)
FINGERPRINT_SUFFIXES = (".sql", ".yml", ".yaml", ".csv")
ARCHIVE_EXCLUDED_PARTS = {"target", "logs", "dbt_packages", "__pycache__"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def redact(text: str) -> str:
    for name in ("SNOWFLAKE_PASSWORD", "KESTRA_PASSWORD", "KESTRA_API_TOKEN"):
        secret = os.getenv(name)
        if secret:
            text = text.replace(secret, "[REDACTED]")
    return text


def json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def is_processing_column(name: str) -> bool:
    """Exclude processing and source-lineage metadata from business signatures."""
    upper = name.upper()
    return upper == "MANIFEST_RESOLVED_AT" or upper.endswith(
        (
            "_PROCESSED_AT",
            "_SOURCE_VERSION",
            "_SOURCE_VERSION_CHECKSUM",
            "_SOURCE_CAPTURED_AT",
            "_BRONZE_INGESTION_RUN_ID",
        )
    )


def transformation_files(project_dir: Path) -> list[Path]:
    files: set[Path] = set()
    for name in FINGERPRINT_ROOT_FILES:
        candidate = project_dir / name
        if candidate.is_file():
            files.add(candidate)
    for directory in FINGERPRINT_DIRECTORIES:
        root = project_dir / directory
        if root.is_dir():
            files.update(
                path
                for path in root.rglob("*")
                if path.is_file() and path.suffix.lower() in FINGERPRINT_SUFFIXES
            )
    return sorted(files, key=lambda path: path.relative_to(project_dir).as_posix())


def transformation_fingerprint(project_dir: Path) -> str:
    """Hash file paths and bytes, so uncommitted local changes are visible."""
    digest = hashlib.sha256()
    files = transformation_files(project_dir)
    if not files:
        raise ValueError(f"No dbt transformation files found under {project_dir}")
    for path in files:
        relative = path.relative_to(project_dir).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def archive_project(project_dir: Path, destination: Path) -> None:
    """Create a deterministic, immutable copy of the exact project to execute."""
    project_dir = project_dir.resolve()
    files = sorted(
        (
            path
            for path in project_dir.rglob("*")
            if path.is_file()
            and not path.is_symlink()
            and not ARCHIVE_EXCLUDED_PARTS.intersection(path.relative_to(project_dir).parts)
            and path.name != destination.name
        ),
        key=lambda path: path.relative_to(project_dir).as_posix(),
    )
    if not files:
        raise ValueError(f"No files found to archive under {project_dir}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(mode="w", fileobj=compressed) as archive:
                for path in files:
                    relative = path.relative_to(project_dir).as_posix()
                    info = archive.gettarinfo(str(path), arcname=relative)
                    info.mtime = 0
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)


def effective_schemas(target: str, environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    values = environ or os.environ
    if target == "prod":
        staging = values.get("SNOWFLAKE_STAGING_SCHEMA", "STAGING")
        silver = values.get("SNOWFLAKE_SILVER_SCHEMA", "SILVER")
    elif target in {"dev", "test"}:
        prefix = values.get(
            "DBT_DEV_SCHEMA" if target == "dev" else "DBT_TEST_SCHEMA",
            "DBT_DEV" if target == "dev" else "DBT_TEST",
        )
        staging = f"{prefix}_STAGING"
        silver = f"{prefix}_SILVER"
    else:
        raise ValueError("dbt target must be dev, test, or prod")
    # Validate with the same conservative identifier contract used by ingestion.
    identifier(staging)
    identifier(silver)
    return staging.upper(), silver.upper()


def parse_fixed_manifest(value: str | None) -> dict[str, dict] | None:
    if not value or not value.strip() or value.strip() == "{}":
        return None
    parsed = json.loads(value)
    if isinstance(parsed, dict) and "manifest" in parsed:
        parsed = parsed["manifest"]
    if not isinstance(parsed, dict):
        raise ValueError("source_manifest must be a JSON object")
    missing = sorted(set(REQUIRED_ASSETS) - set(parsed))
    extra = sorted(set(parsed) - set(REQUIRED_ASSETS))
    if missing or extra:
        raise ValueError(f"Invalid source_manifest assets; missing={missing}, extra={extra}")
    normalized: dict[str, dict] = {}
    for asset in REQUIRED_ASSETS:
        entry = parsed[asset]
        if not isinstance(entry, dict):
            raise ValueError(f"Manifest entry for {asset} must be an object")
        run_id = entry.get("ingestion_run_id")
        checksum = entry.get("source_file_sha256")
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError(f"Manifest entry for {asset} requires ingestion_run_id")
        if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-fA-F]{64}", checksum):
            raise ValueError(f"Manifest entry for {asset} requires a 64-character SHA-256")
        normalized[asset] = {
            "ingestion_run_id": run_id,
            "source_file_sha256": checksum.lower(),
        }
    return normalized


def manifest_fingerprint(manifest: Mapping[str, Mapping[str, object]]) -> str:
    identity = {
        asset: {
            "ingestion_run_id": entry["ingestion_run_id"],
            "source_file_sha256": entry["source_file_sha256"],
        }
        for asset, entry in sorted(manifest.items())
    }
    return sha256_bytes(json_text(identity).encode("utf-8"))


def decide_run(
    *,
    force: bool,
    missing_tables: Sequence[str],
    checkpoint: Mapping[str, object] | None,
    current_manifest_fingerprint: str,
    current_transformation_fingerprint: str,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    if force:
        reasons.append("force_requested")
    if missing_tables:
        reasons.append("required_outputs_missing:" + ",".join(sorted(missing_tables)))
    if checkpoint is None:
        reasons.append("no_successful_silver_checkpoint")
    else:
        if checkpoint.get("manifest_fingerprint") != current_manifest_fingerprint:
            reasons.append("bronze_versions_pending")
        if checkpoint.get("transformation_fingerprint") != current_transformation_fingerprint:
            reasons.append("transformation_changed")
    return bool(reasons), reasons or ["already_validated"]


def validate_dbt_artifacts(
    dbt_execution: Mapping[str, object],
    run_results: Mapping[str, object],
) -> dict:
    errors: list[str] = []
    if dbt_execution.get("status") != "SUCCESS" or int(dbt_execution.get("dbt_exit_code", 1)) != 0:
        errors.append(
            "dbt build failed "
            f"({dbt_execution.get('failure_classification', 'unclassified')})"
        )
    status_counts: dict[str, int] = {}
    successful_models: set[str] = set()
    for result in run_results.get("results", []):
        status = str(result.get("status", "unknown")).lower()
        status_counts[status] = status_counts.get(status, 0) + 1
        unique_id = str(result.get("unique_id", ""))
        if status == "success" and unique_id.startswith("model."):
            successful_models.add(unique_id.rsplit(".", 1)[-1])
        if status in {"error", "fail", "runtime error"}:
            errors.append(f"critical dbt result {unique_id or '<unknown>'}: {status}")
    missing_models = sorted(set(FINAL_MODELS) - successful_models)
    if missing_models:
        errors.append("required final models not successful: " + ", ".join(missing_models))
    return {
        "valid": not errors,
        "errors": errors,
        "status_counts": status_counts,
        "warnings": status_counts.get("warn", 0),
        "required_models": sorted(successful_models.intersection(FINAL_MODELS)),
    }


@dataclass(frozen=True)
class SourceVersion:
    asset: str
    ingestion_run_id: str
    source_file_sha256: str
    source_version: str | None
    source_file: str | None
    source_captured_at: object | None
    source_checked_at: object | None
    bronze_finished_at: object | None
    bronze_rows_loaded: int | None

    def as_dict(self) -> dict:
        return {
            "ingestion_run_id": self.ingestion_run_id,
            "source_file_sha256": self.source_file_sha256,
            "source_version": self.source_version,
            "source_file": self.source_file,
            "source_captured_at": self.source_captured_at,
            "source_checked_at": self.source_checked_at,
            "bronze_finished_at": self.bronze_finished_at,
            "bronze_rows_loaded": self.bronze_rows_loaded,
        }


class SilverControl:
    def __init__(self, settings: SnowflakeSettings, target: str):
        self.settings = settings
        self.target = target
        self.staging_schema, self.silver_schema = effective_schemas(target)
        self.connection = connect_snowflake(
            settings,
            application="transfermarkt_silver_orchestration",
        )

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "SilverControl":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    @property
    def database(self) -> str:
        return identifier(self.settings.database)

    @property
    def bronze_schema(self) -> str:
        return f"{self.database}.{identifier(self.settings.schema)}"

    @property
    def silver_qschema(self) -> str:
        return f"{self.database}.{identifier(self.silver_schema)}"

    def qname(self, name: str) -> str:
        return f"{self.silver_qschema}.{identifier(name)}"

    def _execute(self, cursor, sql: str, params=None):
        return cursor.execute(sql, params) if params is not None else cursor.execute(sql)

    def _fetch_dicts(self, cursor, sql: str, params=None) -> list[dict]:
        self._execute(cursor, sql, params)
        names = [str(item[0]).lower() for item in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def ensure_objects(self) -> None:
        with self.connection.cursor() as cursor:
            for sql in (
                f"USE ROLE {identifier(self.settings.role)}",
                f"USE WAREHOUSE {identifier(self.settings.warehouse)}",
                f"USE DATABASE {self.database}",
                f"CREATE SCHEMA IF NOT EXISTS {identifier(self.staging_schema)}",
                f"CREATE SCHEMA IF NOT EXISTS {identifier(self.silver_schema)}",
            ):
                self._execute(cursor, sql)
            self._execute(cursor, "SELECT 1")
            if int(cursor.fetchone()[0]) != 1:
                raise RuntimeError("Snowflake SELECT 1 returned an unexpected result")
            self._execute(
                cursor,
                f"""CREATE TABLE IF NOT EXISTS {self.qname(RUNS_TABLE)} (
                    EXECUTION_ID VARCHAR NOT NULL,
                    BRONZE_BATCH_ID VARCHAR,
                    EXECUTION_MODE VARCHAR NOT NULL,
                    DBT_TARGET VARCHAR NOT NULL,
                    FORCE_REQUESTED BOOLEAN NOT NULL,
                    STATUS VARCHAR NOT NULL,
                    SHOULD_RUN BOOLEAN NOT NULL,
                    DECISION_REASONS VARIANT NOT NULL,
                    INPUT_MANIFEST VARIANT NOT NULL,
                    MANIFEST_FINGERPRINT VARCHAR NOT NULL,
                    TRANSFORMATION_FINGERPRINT VARCHAR NOT NULL,
                    STARTED_AT TIMESTAMP_TZ NOT NULL,
                    FINISHED_AT TIMESTAMP_TZ,
                    DBT_EXIT_CODE NUMBER,
                    DBT_ATTEMPTS NUMBER,
                    DBT_RESULT_SUMMARY VARIANT,
                    MODEL_COUNTS VARIANT,
                    ARTIFACTS VARIANT,
                    ERROR VARCHAR
                )""",
            )
            self._execute(
                cursor,
                f"""CREATE TABLE IF NOT EXISTS {self.qname(SOURCES_TABLE)} (
                    EXECUTION_ID VARCHAR NOT NULL,
                    DBT_TARGET VARCHAR NOT NULL,
                    ASSET VARCHAR NOT NULL,
                    BRONZE_INGESTION_RUN_ID VARCHAR NOT NULL,
                    SOURCE_FILE_SHA256 VARCHAR NOT NULL,
                    SOURCE_VERSION VARCHAR,
                    SOURCE_FILE VARCHAR,
                    SOURCE_CAPTURED_AT TIMESTAMP_TZ,
                    SOURCE_CHECKED_AT TIMESTAMP_TZ,
                    BRONZE_FINISHED_AT TIMESTAMP_TZ,
                    BRONZE_ROWS_LOADED NUMBER,
                    RECORDED_AT TIMESTAMP_TZ NOT NULL
                )""",
            )
            self._execute(
                cursor,
                f"""CREATE TABLE IF NOT EXISTS {self.qname(CHECKPOINT_TABLE)} (
                    DBT_TARGET VARCHAR NOT NULL,
                    EXECUTION_ID VARCHAR NOT NULL,
                    MANIFEST_FINGERPRINT VARCHAR NOT NULL,
                    TRANSFORMATION_FINGERPRINT VARCHAR NOT NULL,
                    INPUT_MANIFEST VARIANT NOT NULL,
                    VALIDATED_AT TIMESTAMP_TZ NOT NULL
                )""",
            )
        self.connection.commit()

    def assert_no_concurrent_run(self, execution_id: str) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""SELECT EXECUTION_ID
                    FROM {self.qname(RUNS_TABLE)}
                    WHERE DBT_TARGET = %s AND STATUS = 'RUNNING'
                      AND EXECUTION_ID <> %s
                      AND STARTED_AT >= DATEADD('hour', -12, CURRENT_TIMESTAMP())
                    LIMIT 1""",
                (self.target, execution_id),
            )
            row = cursor.fetchone()
        if row:
            raise RuntimeError(
                f"Silver target {self.target} already has active execution {row[0]}"
            )

    def resolve_manifest(self, fixed: Mapping[str, Mapping[str, str]] | None) -> dict[str, SourceVersion]:
        columns = """ASSET, RUN_ID, SOURCE_FILE_SHA256, SOURCE_VERSION,
                     SOURCE_FILE, CAPTURED_AT, SOURCE_CHECKED_AT, FINISHED_AT,
                     ROWS_LOADED"""
        fixed_columns = """F.ASSET, F.RUN_ID, F.SOURCE_FILE_SHA256, F.SOURCE_VERSION,
                           F.SOURCE_FILE, F.CAPTURED_AT, F.SOURCE_CHECKED_AT,
                           F.FINISHED_AT, F.ROWS_LOADED"""
        with self.connection.cursor() as cursor:
            if fixed:
                values_sql = ",".join(["(%s,%s,%s)"] * len(REQUIRED_ASSETS))
                params: list[str] = []
                for asset in REQUIRED_ASSETS:
                    params.extend(
                        [
                            asset,
                            str(fixed[asset]["ingestion_run_id"]),
                            str(fixed[asset]["source_file_sha256"]),
                        ]
                    )
                sql = f"""WITH REQUESTED(ASSET, RUN_ID, CHECKSUM) AS (
                            SELECT COLUMN1, COLUMN2, COLUMN3 FROM VALUES {values_sql}
                        )
                        SELECT {fixed_columns}
                        FROM {self.bronze_schema}.{identifier('TRANSFERMARKT_INGESTION_FILES')} F
                        INNER JOIN REQUESTED R
                          ON F.ASSET = R.ASSET
                         AND F.RUN_ID = R.RUN_ID
                         AND F.SOURCE_FILE_SHA256 = R.CHECKSUM
                        WHERE F.STATUS = 'SUCCESS'
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY F.ASSET ORDER BY F.FINISHED_AT DESC, F.RUN_ID DESC
                        ) = 1"""
                rows = self._fetch_dicts(cursor, sql, tuple(params))
            else:
                placeholders = ",".join(["%s"] * len(REQUIRED_ASSETS))
                sql = f"""SELECT {columns}
                        FROM {self.bronze_schema}.{identifier('TRANSFERMARKT_INGESTION_FILES')}
                        WHERE STATUS = 'SUCCESS' AND SOURCE_FILE_SHA256 IS NOT NULL
                          AND ASSET IN ({placeholders})
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY ASSET ORDER BY FINISHED_AT DESC, RUN_ID DESC
                        ) = 1"""
                rows = self._fetch_dicts(cursor, sql, REQUIRED_ASSETS)
        resolved = {
            str(row["asset"]): SourceVersion(
                asset=str(row["asset"]),
                ingestion_run_id=str(row["run_id"]),
                source_file_sha256=str(row["source_file_sha256"]).lower(),
                source_version=str(row["source_version"]) if row["source_version"] is not None else None,
                source_file=str(row["source_file"]) if row["source_file"] is not None else None,
                source_captured_at=row["captured_at"],
                source_checked_at=row["source_checked_at"],
                bronze_finished_at=row["finished_at"],
                bronze_rows_loaded=int(row["rows_loaded"]) if row["rows_loaded"] is not None else None,
            )
            for row in rows
        }
        missing = sorted(set(REQUIRED_ASSETS) - set(resolved))
        if missing:
            requested = "requested preserved versions" if fixed else "latest successful versions"
            raise RuntimeError(f"Bronze is missing {requested} for: {', '.join(missing)}")
        self.validate_raw_versions(resolved)
        return resolved

    def validate_raw_versions(self, manifest: Mapping[str, SourceVersion]) -> None:
        with self.connection.cursor() as cursor:
            for asset in REQUIRED_ASSETS:
                entry = manifest[asset]
                raw_table = identifier(f"{asset}_RAW")
                self._execute(
                    cursor,
                    f"""SELECT COUNT(*)
                        FROM {self.bronze_schema}.{raw_table}
                        WHERE INGESTION_RUN_ID = %s AND SOURCE_FILE_SHA256 = %s""",
                    (entry.ingestion_run_id, entry.source_file_sha256),
                )
                rows = int(cursor.fetchone()[0])
                if rows <= 0:
                    raise RuntimeError(f"Preserved Bronze rows are absent for {asset}")
                if entry.bronze_rows_loaded is not None and rows != entry.bronze_rows_loaded:
                    raise RuntimeError(
                        f"Bronze row reconciliation failed for {asset}: "
                        f"audit={entry.bronze_rows_loaded}, raw={rows}"
                    )

    def missing_final_tables(self) -> list[str]:
        placeholders = ",".join(["%s"] * len(FINAL_MODELS))
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""SELECT TABLE_NAME
                    FROM {self.database}.INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                      AND TABLE_NAME IN ({placeholders})""",
                (self.silver_schema, *(name.upper() for name in FINAL_MODELS)),
            )
            existing = {str(row[0]).lower() for row in cursor.fetchall()}
        return sorted(set(FINAL_MODELS) - existing)

    def checkpoint(self) -> dict | None:
        with self.connection.cursor() as cursor:
            rows = self._fetch_dicts(
                cursor,
                f"""SELECT EXECUTION_ID, MANIFEST_FINGERPRINT,
                           TRANSFORMATION_FINGERPRINT, VALIDATED_AT
                    FROM {self.qname(CHECKPOINT_TABLE)}
                    WHERE DBT_TARGET = %s
                    QUALIFY ROW_NUMBER() OVER (ORDER BY VALIDATED_AT DESC, EXECUTION_ID DESC) = 1""",
                (self.target,),
            )
        return rows[0] if rows else None

    def recent_audit(self, limit: int = 10) -> dict[str, list[dict]]:
        if not 1 <= limit <= 100:
            raise ValueError("audit limit must be between 1 and 100")
        with self.connection.cursor() as cursor:
            runs = self._fetch_dicts(
                cursor,
                f"""SELECT EXECUTION_ID, BRONZE_BATCH_ID, EXECUTION_MODE,
                           DBT_TARGET, FORCE_REQUESTED, STATUS, SHOULD_RUN,
                           DECISION_REASONS, MANIFEST_FINGERPRINT,
                           TRANSFORMATION_FINGERPRINT, STARTED_AT, FINISHED_AT,
                           DBT_EXIT_CODE, DBT_ATTEMPTS, DBT_RESULT_SUMMARY,
                           MODEL_COUNTS, ARRAY_SIZE(ARTIFACTS) AS ARTIFACT_COUNT,
                           ERROR
                    FROM {self.qname(RUNS_TABLE)}
                    WHERE DBT_TARGET = %s
                    ORDER BY STARTED_AT DESC
                    LIMIT {limit}""",
                (self.target,),
            )
            checkpoints = self._fetch_dicts(
                cursor,
                f"""SELECT DBT_TARGET, EXECUTION_ID, MANIFEST_FINGERPRINT,
                           TRANSFORMATION_FINGERPRINT, VALIDATED_AT
                    FROM {self.qname(CHECKPOINT_TABLE)}
                    WHERE DBT_TARGET = %s
                    ORDER BY VALIDATED_AT DESC""",
                (self.target,),
            )
            sources = self._fetch_dicts(
                cursor,
                f"""SELECT EXECUTION_ID, ASSET, BRONZE_INGESTION_RUN_ID,
                           SOURCE_FILE_SHA256, SOURCE_VERSION, SOURCE_FILE,
                           SOURCE_CAPTURED_AT, SOURCE_CHECKED_AT,
                           BRONZE_FINISHED_AT, BRONZE_ROWS_LOADED
                    FROM {self.qname(SOURCES_TABLE)}
                    WHERE DBT_TARGET = %s
                      AND EXECUTION_ID IN (
                          SELECT EXECUTION_ID FROM {self.qname(RUNS_TABLE)}
                          WHERE DBT_TARGET = %s
                          ORDER BY STARTED_AT DESC LIMIT {limit}
                      )
                    ORDER BY EXECUTION_ID, ASSET""",
                (self.target, self.target),
            )
        return {"runs": runs, "checkpoints": checkpoints, "sources": sources}

    def business_signatures(self) -> list[dict]:
        """Return order-independent hashes excluding Silver processing timestamps."""
        results: list[dict] = []
        with self.connection.cursor() as cursor:
            for model, key_columns in FINAL_MODEL_KEYS.items():
                columns = self._fetch_dicts(
                    cursor,
                    f"""SELECT COLUMN_NAME
                        FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                        ORDER BY ORDINAL_POSITION""",
                    (self.silver_schema, model.upper()),
                )
                names = [str(row["column_name"]).upper() for row in columns]
                missing_keys = sorted(set(key_columns) - set(names))
                if missing_keys:
                    raise RuntimeError(
                        f"{model} is missing signature keys: {', '.join(missing_keys)}"
                    )
                business_columns = [name for name in names if not is_processing_column(name)]
                if not business_columns:
                    raise RuntimeError(f"{model} has no business columns to sign")
                object_parts: list[str] = []
                for name in business_columns:
                    quoted = identifier(name)
                    object_parts.extend((f"'{name}'", quoted))
                row_expression = (
                    "HASH(TO_JSON(OBJECT_CONSTRUCT_KEEP_NULL("
                    + ", ".join(object_parts)
                    + ")))"
                )
                keys = ", ".join(identifier(name) for name in key_columns)
                rows = self._fetch_dicts(
                    cursor,
                    f"""SELECT COUNT(*) AS ROW_COUNT,
                               (SELECT COUNT(*) FROM (
                                    SELECT {keys} FROM {self.qname(model)}
                                    GROUP BY {keys}
                                )) AS DISTINCT_KEY_COUNT,
                               HASH_AGG({row_expression}) AS BUSINESS_HASH
                        FROM {self.qname(model)}""",
                )
                row = rows[0]
                row_count = int(row["row_count"])
                distinct_keys = int(row["distinct_key_count"])
                results.append(
                    {
                        "model": model,
                        "key_columns": [name.lower() for name in key_columns],
                        "row_count": row_count,
                        "distinct_key_count": distinct_keys,
                        "duplicate_key_rows": row_count - distinct_keys,
                        "business_hash": str(row["business_hash"]),
                        "excluded_columns": [
                            name.lower() for name in names if is_processing_column(name)
                        ],
                    }
                )
        return results

    def delivery_metrics(self) -> dict:
        """Read-only acceptance metrics for the four delivered Silver models."""
        grouped = {
            "players_resolution": (
                "silver_players_current",
                (
                    "BIRTH_COUNTRY_RESOLUTION_STATUS",
                    "CITIZENSHIP_COUNTRY_RESOLUTION_STATUS",
                    "CURRENT_NATIONAL_TEAM_RESOLUTION_STATUS",
                    "NATIONAL_TEAM_COUNTRY_RESOLUTION_STATUS",
                    "CURRENT_CLUB_RESOLUTION_STATUS",
                ),
            ),
            "games_resolution": (
                "silver_games_enriched",
                (
                    "COMPETITION_RESOLUTION_STATUS",
                    "HOME_CLUB_RESOLUTION_STATUS",
                    "AWAY_CLUB_RESOLUTION_STATUS",
                ),
            ),
            "player_match_context": (
                "silver_player_match",
                ("GAME_JOIN_STATUS", "CLUB_GAME_JOIN_STATUS", "TEAM_CONTEXT_STATUS"),
            ),
            "valuations_context": (
                "silver_player_valuations_enriched",
                ("PLAYER_PROFILE_JOIN_STATUS", "AGE_AT_VALUATION_STATUS"),
            ),
        }
        report: dict[str, object] = {
            "target": self.target,
            "staging_schema": self.staging_schema,
            "silver_schema": self.silver_schema,
            "signatures": self.business_signatures(),
        }
        with self.connection.cursor() as cursor:
            reconciliation = self._fetch_dicts(
                cursor,
                f"""SELECT COUNT(*) AS ASSET_COUNT,
                           SUM(INPUT_ROWS) AS INPUT_ROWS,
                           SUM(ACCEPTED_ROWS) AS ACCEPTED_ROWS,
                           SUM(IDENTICAL_DUPLICATE_ROWS) AS IDENTICAL_DUPLICATE_ROWS,
                           SUM(REJECTED_ROWS) AS REJECTED_ROWS,
                           SUM(CONFLICTING_ROWS) AS CONFLICTING_ROWS,
                           COUNT_IF(INPUT_ROWS <> ACCEPTED_ROWS
                               + IDENTICAL_DUPLICATE_ROWS + REJECTED_ROWS)
                               AS UNBALANCED_ASSETS
                    FROM {self.qname(RECONCILIATION_TABLE)}""",
            )[0]
            report["reconciliation"] = reconciliation
            self._execute(cursor, f"SELECT COUNT(*) FROM {self.qname(QUARANTINE_TABLE)}")
            report["quarantine_rows"] = int(cursor.fetchone()[0])

            status_counts: dict[str, list[dict]] = {}
            for section, (model, status_columns) in grouped.items():
                section_rows: list[dict] = []
                for column in status_columns:
                    rows = self._fetch_dicts(
                        cursor,
                        f"""SELECT %s AS METRIC, {identifier(column)} AS STATUS,
                                   COUNT(*) AS ROW_COUNT
                            FROM {self.qname(model)}
                            GROUP BY {identifier(column)}
                            ORDER BY {identifier(column)}""",
                        (column.lower(),),
                    )
                    section_rows.extend(rows)
                status_counts[section] = section_rows
            report["status_counts"] = status_counts

            report["role_checks"] = self._fetch_dicts(
                cursor,
                f"""SELECT
                        COUNT_IF(BIRTH_COUNTRY_ID IS NOT NULL
                            AND CITIZENSHIP_COUNTRY_ID IS NOT NULL
                            AND BIRTH_COUNTRY_ID <> CITIZENSHIP_COUNTRY_ID)
                            AS BIRTH_DIFFERS_FROM_CITIZENSHIP,
                        COUNT_IF(CURRENT_NATIONAL_TEAM_ID IS NULL)
                            AS PLAYERS_WITHOUT_NATIONAL_TEAM,
                        COUNT_IF(CURRENT_CLUB_ID IS NULL) AS PLAYERS_WITHOUT_CURRENT_CLUB
                    FROM {self.qname('silver_players_current')}""",
            )[0]
            report["valuation_history"] = self._fetch_dicts(
                cursor,
                f"""SELECT COUNT(DISTINCT PLAYER_ID) AS PLAYERS,
                           COUNT_IF(HISTORICAL_MARKET_VALUE_EUR IS NULL) AS NULL_AMOUNTS,
                           COUNT_IF(HISTORICAL_MARKET_VALUE_EUR < 0) AS NEGATIVE_AMOUNTS,
                           COUNT_IF(HISTORICAL_MARKET_VALUE_EUR = 0) AS ZERO_AMOUNTS,
                           MIN(HISTORICAL_MARKET_VALUE_EUR) AS MIN_AMOUNT_EUR,
                           MAX(HISTORICAL_MARKET_VALUE_EUR) AS MAX_AMOUNT_EUR,
                           SUM(HISTORICAL_MARKET_VALUE_EUR) AS SUM_AMOUNT_EUR
                    FROM {self.qname('silver_player_valuations_enriched')}""",
            )[0]
            report["valuation_multiplicity"] = self._fetch_dicts(
                cursor,
                f"""SELECT COUNT_IF(VALUATION_COUNT > 1) AS PLAYERS_WITH_HISTORY,
                           MAX(VALUATION_COUNT) AS MAX_VALUATIONS_PER_PLAYER
                    FROM (
                        SELECT PLAYER_ID, COUNT(*) AS VALUATION_COUNT
                        FROM {self.qname('silver_player_valuations_enriched')}
                        GROUP BY PLAYER_ID
                    )""",
            )[0]
        return report

    def record_plan(
        self,
        *,
        execution_id: str,
        bronze_batch_id: str | None,
        mode: str,
        force: bool,
        should_run: bool,
        reasons: Sequence[str],
        manifest: Mapping[str, SourceVersion],
        manifest_hash: str,
        transformation_hash: str,
    ) -> None:
        manifest_document = {asset: manifest[asset].as_dict() for asset in REQUIRED_ASSETS}
        status = "RUNNING" if should_run else "SKIPPED"
        with self.connection.cursor() as cursor:
            self._execute(cursor, f"DELETE FROM {self.qname(SOURCES_TABLE)} WHERE EXECUTION_ID = %s", (execution_id,))
            self._execute(cursor, f"DELETE FROM {self.qname(RUNS_TABLE)} WHERE EXECUTION_ID = %s", (execution_id,))
            self._execute(
                cursor,
                f"""INSERT INTO {self.qname(RUNS_TABLE)}
                    (EXECUTION_ID, BRONZE_BATCH_ID, EXECUTION_MODE, DBT_TARGET,
                     FORCE_REQUESTED, STATUS, SHOULD_RUN, DECISION_REASONS,
                     INPUT_MANIFEST, MANIFEST_FINGERPRINT,
                     TRANSFORMATION_FINGERPRINT, STARTED_AT, FINISHED_AT)
                    SELECT %s, NULLIF(%s, ''), %s, %s, %s, %s, %s,
                           PARSE_JSON(%s), PARSE_JSON(%s), %s, %s,
                           CURRENT_TIMESTAMP(),
                           IFF(%s = 'SKIPPED', CURRENT_TIMESTAMP(), NULL)""",
                (
                    execution_id,
                    bronze_batch_id or "",
                    mode,
                    self.target,
                    force,
                    status,
                    should_run,
                    json_text(list(reasons)),
                    json_text(manifest_document),
                    manifest_hash,
                    transformation_hash,
                    status,
                ),
            )
            for asset in REQUIRED_ASSETS:
                entry = manifest[asset]
                self._execute(
                    cursor,
                    f"""INSERT INTO {self.qname(SOURCES_TABLE)}
                        (EXECUTION_ID, DBT_TARGET, ASSET,
                         BRONZE_INGESTION_RUN_ID, SOURCE_FILE_SHA256,
                         SOURCE_VERSION, SOURCE_FILE, SOURCE_CAPTURED_AT,
                         SOURCE_CHECKED_AT, BRONZE_FINISHED_AT,
                         BRONZE_ROWS_LOADED, RECORDED_AT)
                        SELECT %s, %s, %s, %s, %s, %s, %s,
                               TRY_TO_TIMESTAMP_TZ(%s::VARCHAR),
                               TRY_TO_TIMESTAMP_TZ(%s::VARCHAR),
                               TRY_TO_TIMESTAMP_TZ(%s::VARCHAR), %s,
                               CURRENT_TIMESTAMP()""",
                    (
                        execution_id,
                        self.target,
                        asset,
                        entry.ingestion_run_id,
                        entry.source_file_sha256,
                        entry.source_version,
                        entry.source_file,
                        str(entry.source_captured_at) if entry.source_captured_at is not None else None,
                        str(entry.source_checked_at) if entry.source_checked_at is not None else None,
                        str(entry.bronze_finished_at) if entry.bronze_finished_at is not None else None,
                        entry.bronze_rows_loaded,
                    ),
                )
        self.connection.commit()

    def model_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self.connection.cursor() as cursor:
            for model in (*FINAL_MODELS, QUARANTINE_TABLE, RECONCILIATION_TABLE):
                self._execute(cursor, f"SELECT COUNT(*) FROM {self.qname(model)}")
                counts[model.lower()] = int(cursor.fetchone()[0])
        return counts

    def quarantine_evidence(self) -> dict:
        with self.connection.cursor() as cursor:
            by_asset = self._fetch_dicts(
                cursor,
                f"""SELECT ASSET_NAME, COUNT(*) AS REJECTED_ROWS
                    FROM {self.qname(QUARANTINE_TABLE)}
                    GROUP BY ASSET_NAME ORDER BY ASSET_NAME""",
            )
            by_reason = self._fetch_dicts(
                cursor,
                f"""SELECT Q.ASSET_NAME, F.VALUE::VARCHAR AS REASON,
                           COUNT(*) AS REJECTED_ROWS
                    FROM {self.qname(QUARANTINE_TABLE)} Q,
                         LATERAL FLATTEN(INPUT => Q.REJECTION_REASONS) F
                    GROUP BY Q.ASSET_NAME, F.VALUE::VARCHAR
                    ORDER BY Q.ASSET_NAME, F.VALUE::VARCHAR""",
            )
        return {"by_asset": by_asset, "by_reason": by_reason}

    def finish(
        self,
        *,
        execution_id: str,
        success: bool,
        dbt_execution: Mapping[str, object],
        result_summary: Mapping[str, object],
        model_counts: Mapping[str, int],
        artifact_metadata: Sequence[Mapping[str, object]],
        error: str | None,
        plan: Mapping[str, object],
    ) -> None:
        status = "SUCCESS" if success else "FAILED"
        safe_error = redact(error or "")[:8000] or None
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(RUNS_TABLE)}
                    SET STATUS = %s, FINISHED_AT = CURRENT_TIMESTAMP(),
                        DBT_EXIT_CODE = %s, DBT_ATTEMPTS = %s,
                        DBT_RESULT_SUMMARY = PARSE_JSON(%s),
                        MODEL_COUNTS = PARSE_JSON(%s),
                        ARTIFACTS = PARSE_JSON(%s), ERROR = %s
                    WHERE EXECUTION_ID = %s AND STATUS = 'RUNNING'""",
                (
                    status,
                    dbt_execution.get("dbt_exit_code"),
                    dbt_execution.get("attempt_count"),
                    json_text(result_summary),
                    json_text(dict(model_counts)),
                    json_text(list(artifact_metadata)),
                    safe_error,
                    execution_id,
                ),
            )
            if success:
                self._execute(
                    cursor,
                    f"DELETE FROM {self.qname(CHECKPOINT_TABLE)} WHERE DBT_TARGET = %s",
                    (self.target,),
                )
                self._execute(
                    cursor,
                    f"""INSERT INTO {self.qname(CHECKPOINT_TABLE)}
                        (DBT_TARGET, EXECUTION_ID, MANIFEST_FINGERPRINT,
                         TRANSFORMATION_FINGERPRINT, INPUT_MANIFEST, VALIDATED_AT)
                        SELECT %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()""",
                    (
                        self.target,
                        execution_id,
                        plan["manifest_fingerprint"],
                        plan["transformation_fingerprint"],
                        json_text(plan["manifest"]),
                    ),
                )
        self.connection.commit()

    def mark_unexpected_failure(self, execution_id: str, error: str) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(RUNS_TABLE)}
                    SET STATUS = 'FAILED', FINISHED_AT = CURRENT_TIMESTAMP(), ERROR = %s
                    WHERE EXECUTION_ID = %s AND STATUS = 'RUNNING'""",
                (redact(error)[:8000], execution_id),
            )
        self.connection.commit()


def resolve_target(value: str | None) -> str:
    target = (value or os.getenv("DBT_TARGET", "dev")).strip().lower()
    if target not in {"dev", "test", "prod"}:
        raise ValueError("dbt target must be dev, test, or prod")
    return target


def bool_value(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1", "yes"}:
        return True
    if normalized in {"false", "0", "no"}:
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def artifact_metadata(artifact_dir: Path) -> list[dict]:
    return [
        {
            "path": path.relative_to(artifact_dir).as_posix(),
            "size_bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        }
        for path in sorted(artifact_dir.rglob("*"))
        if path.is_file()
    ]


def find_artifact(artifact_dir: Path, name: str) -> Path:
    matches = sorted(artifact_dir.rglob(name), key=lambda path: len(path.parts))
    if not matches:
        raise FileNotFoundError(f"Required dbt artifact is unavailable: {name}")
    return matches[0]


def command_plan(args) -> int:
    target = resolve_target(args.target)
    fixed = parse_fixed_manifest(args.source_manifest)
    if args.mode == "backfill" and fixed is None:
        raise ValueError("Silver backfill requires an explicit preserved source_manifest")
    project_dir = args.project_dir.resolve()
    transformation_hash = transformation_fingerprint(project_dir)
    archive_project(project_dir, args.archive)
    settings = SnowflakeSettings.from_env()
    with SilverControl(settings, target) as control:
        control.ensure_objects()
        control.assert_no_concurrent_run(args.execution_id)
        sources = control.resolve_manifest(fixed)
        manifest = {asset: sources[asset].as_dict() for asset in REQUIRED_ASSETS}
        manifest_hash = manifest_fingerprint(manifest)
        missing_tables = control.missing_final_tables()
        checkpoint = control.checkpoint()
        should_run, reasons = decide_run(
            force=args.force,
            missing_tables=missing_tables,
            checkpoint=checkpoint,
            current_manifest_fingerprint=manifest_hash,
            current_transformation_fingerprint=transformation_hash,
        )
        control.record_plan(
            execution_id=args.execution_id,
            bronze_batch_id=args.bronze_batch_id,
            mode=args.mode,
            force=args.force,
            should_run=should_run,
            reasons=reasons,
            manifest=sources,
            manifest_hash=manifest_hash,
            transformation_hash=transformation_hash,
        )
    staging_schema, silver_schema = effective_schemas(target)
    plan = {
        "execution_id": args.execution_id,
        "bronze_batch_id": args.bronze_batch_id or None,
        "mode": args.mode,
        "force": args.force,
        "target": target,
        "staging_schema": staging_schema,
        "silver_schema": silver_schema,
        "should_run": should_run,
        "decision_reasons": reasons,
        "missing_final_tables": missing_tables,
        "manifest": manifest,
        "manifest_fingerprint": manifest_hash,
        "transformation_fingerprint": transformation_hash,
        "planned_at": utc_now(),
    }
    args.output.write_text(
        json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    outputs = {
        "execution_id": args.execution_id,
        "target": target,
        "should_run": should_run,
        "decision_reasons": reasons,
        "manifest_fingerprint": manifest_hash,
        "transformation_fingerprint": transformation_hash,
        "staging_schema": staging_schema,
        "silver_schema": silver_schema,
    }
    print("::" + json.dumps({"outputs": outputs}, separators=(",", ":")) + "::")
    return 0


def command_finalize(args) -> int:
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    if plan["execution_id"] != args.execution_id:
        raise ValueError("Plan execution_id does not match the Kestra execution")
    target = resolve_target(plan["target"])
    artifact_dir = args.artifact_dir.resolve()
    errors: list[str] = []
    try:
        dbt_execution = json.loads(
            find_artifact(artifact_dir, "dbt_execution.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        dbt_execution = {"status": "FAILED", "dbt_exit_code": 1, "attempt_count": 0}
        errors.append(str(exc))
    try:
        run_results = json.loads(
            find_artifact(artifact_dir, "run_results.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        run_results = {"results": []}
        errors.append(str(exc))
    try:
        find_artifact(artifact_dir, "manifest.json")
    except Exception as exc:
        errors.append(str(exc))
    validation = validate_dbt_artifacts(dbt_execution, run_results)
    errors.extend(validation["errors"])

    counts: dict[str, int] = {}
    evidence: dict = {"available": False, "reason": "not queried"}
    settings = SnowflakeSettings.from_env()
    with SilverControl(settings, target) as control:
        control.ensure_objects()
        if not errors:
            try:
                counts = control.model_counts()
            except Exception as exc:
                errors.append(f"required output verification failed: {exc}")
        try:
            evidence = {"available": True, **control.quarantine_evidence()}
        except Exception as exc:
            evidence = {"available": False, "reason": redact(str(exc))}
        success = not errors
        metadata = artifact_metadata(artifact_dir)
        summary = {
            "status": "SUCCESS" if success else "FAILED",
            "execution_id": args.execution_id,
            "target": target,
            "dbt": dbt_execution,
            "dbt_results": validation,
            "model_counts": counts,
            "errors": [redact(item) for item in errors],
            "checkpoint_advanced": success,
            "non_atomic_build": True,
            "finished_at": utc_now(),
        }
        control.finish(
            execution_id=args.execution_id,
            success=success,
            dbt_execution=dbt_execution,
            result_summary=validation,
            model_counts=counts,
            artifact_metadata=metadata,
            error="; ".join(errors) if errors else None,
            plan=plan,
        )
    args.output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    args.quarantine_output.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    print(
        "::"
        + json.dumps(
            {
                "outputs": {
                    "status": summary["status"],
                    "validated": success,
                    "checkpoint_advanced": success,
                    "dbt_attempts": dbt_execution.get("attempt_count", 0),
                    "warnings": validation["warnings"],
                    "model_counts": counts,
                }
            },
            separators=(",", ":"),
        )
        + "::"
    )
    # Persist summary/evidence first; the following Kestra Fail task propagates failure.
    return 0


def command_fail(args) -> int:
    target = resolve_target(args.target)
    settings = SnowflakeSettings.from_env()
    with SilverControl(settings, target) as control:
        control.ensure_objects()
        control.mark_unexpected_failure(args.execution_id, args.error)
    return 0


def command_audit(args) -> int:
    target = resolve_target(args.target)
    settings = SnowflakeSettings.from_env()
    with SilverControl(settings, target) as control:
        result = control.recent_audit(args.limit)
    print(redact(json.dumps(result, ensure_ascii=False, indent=2, default=str)))
    return 0


def command_verify(args) -> int:
    target = resolve_target(args.target)
    settings = SnowflakeSettings.from_env()
    with SilverControl(settings, target) as control:
        result = control.delivery_metrics()
    print(redact(json.dumps(result, ensure_ascii=False, indent=2, default=str)))
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Transfermarkt Silver orchestration control")
    commands = root.add_subparsers(dest="command", required=True)

    plan = commands.add_parser("plan")
    plan.add_argument("--project-dir", type=Path, required=True)
    plan.add_argument("--execution-id", required=True)
    plan.add_argument("--bronze-batch-id", default="")
    plan.add_argument("--mode", choices=("normal", "backfill"), default="normal")
    plan.add_argument("--target", default="")
    plan.add_argument("--force", type=bool_value, default=False)
    plan.add_argument("--source-manifest", default="")
    plan.add_argument("--output", type=Path, required=True)
    plan.add_argument("--archive", type=Path, required=True)

    finalize = commands.add_parser("finalize")
    finalize.add_argument("--execution-id", required=True)
    finalize.add_argument("--plan", type=Path, required=True)
    finalize.add_argument("--artifact-dir", type=Path, required=True)
    finalize.add_argument("--output", type=Path, required=True)
    finalize.add_argument("--quarantine-output", type=Path, required=True)

    fail = commands.add_parser("fail")
    fail.add_argument("--execution-id", required=True)
    fail.add_argument("--target", default="")
    fail.add_argument("--error", required=True)

    audit = commands.add_parser("audit")
    audit.add_argument("--target", default="")
    audit.add_argument("--limit", type=int, default=10)

    verify = commands.add_parser("verify")
    verify.add_argument("--target", default="")
    return root


def main(argv: Iterable[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.command == "plan":
        return command_plan(args)
    if args.command == "finalize":
        return command_finalize(args)
    if args.command == "fail":
        return command_fail(args)
    if args.command == "audit":
        return command_audit(args)
    return command_verify(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(redact(f"ERROR {type(exc).__name__}: {exc}"), file=sys.stderr)
        raise SystemExit(1)
