from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable

import snowflake.connector
from snowflake.connector.errors import OperationalError

from .config import Asset, AssetCatalog, SnowflakeSettings, identifier
from .models import DownloadResult, PreparedFile


STAGE = "TRANSFERMARKT_BRONZE_STAGE"
FILES_TABLE = "TRANSFERMARKT_INGESTION_FILES"
BATCHES_TABLE = "TRANSFERMARKT_INGESTION_BATCHES"
JSON_FORMAT = "TRANSFERMARKT_NDJSON_FORMAT"


class SnowflakeWarehouse:
    """Snowflake persistence boundary for an atomic asset publication."""

    def __init__(self, settings: SnowflakeSettings, catalog: AssetCatalog):
        self.settings = settings
        self.catalog = catalog
        self.connection = self._connect_with_retry()

    def _connect_with_retry(self):
        kwargs = {
            "account": self.settings.account,
            "user": self.settings.user,
            "password": self.settings.password,
            "authenticator": "snowflake",
            "role": self.settings.role,
            "warehouse": self.settings.warehouse,
            "database": self.settings.database,
            "login_timeout": 30,
            "network_timeout": 120,
            "application": "transfermarkt_bronze_ingestion",
            "autocommit": False,
            "session_parameters": {"TIMEZONE": "UTC"},
        }
        for attempt in range(1, 4):
            try:
                return snowflake.connector.connect(**kwargs)
            except OperationalError:
                if attempt == 3:
                    raise
                time.sleep(2 ** (attempt - 1))
        raise AssertionError("unreachable")

    @property
    def schema(self) -> str:
        return self.settings.qualified_schema

    def qname(self, name: str) -> str:
        return f"{self.schema}.{identifier(name)}"

    def close(self) -> None:
        self.connection.close()

    def check_connection(self) -> None:
        """Verify the authenticated Snowflake session without exposing credentials."""
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            row = cursor.fetchone()
        if not row or int(row[0]) != 1:
            raise RuntimeError("Snowflake connection check returned an unexpected result")

    def __enter__(self) -> "SnowflakeWarehouse":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def ensure_objects(self) -> None:
        role = identifier(self.settings.role)
        warehouse = identifier(self.settings.warehouse)
        database = identifier(self.settings.database)
        schema_name = identifier(self.settings.schema)
        with self.connection.cursor() as cursor:
            cursor.execute(f"USE ROLE {role}")
            cursor.execute(f"USE WAREHOUSE {warehouse}")
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            cursor.execute(f"USE SCHEMA {schema_name}")
            cursor.execute(f"CREATE STAGE IF NOT EXISTS {self.qname(STAGE)}")
            cursor.execute(
                f"CREATE FILE FORMAT IF NOT EXISTS {self.qname(JSON_FORMAT)} "
                "TYPE = JSON COMPRESSION = AUTO STRIP_OUTER_ARRAY = FALSE"
            )
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {self.qname(BATCHES_TABLE)} (
                    BATCH_ID VARCHAR NOT NULL,
                    MODE VARCHAR NOT NULL,
                    REQUESTED_ASSETS VARIANT NOT NULL,
                    STATUS VARCHAR NOT NULL,
                    STARTED_AT TIMESTAMP_TZ NOT NULL,
                    FINISHED_AT TIMESTAMP_TZ,
                    SUMMARY VARIANT,
                    ERROR VARCHAR
                )"""
            )
            cursor.execute(
                f"""CREATE TABLE IF NOT EXISTS {self.qname(FILES_TABLE)} (
                    RUN_ID VARCHAR NOT NULL,
                    BATCH_ID VARCHAR,
                    ASSET VARCHAR NOT NULL,
                    MODE VARCHAR NOT NULL,
                    SOURCE_URL VARCHAR NOT NULL,
                    SOURCE_FILE VARCHAR,
                    SOURCE_FILE_SHA256 VARCHAR,
                    STATUS VARCHAR NOT NULL,
                    STARTED_AT TIMESTAMP_TZ NOT NULL,
                    FINISHED_AT TIMESTAMP_TZ,
                    ROWS_READ NUMBER,
                    ROWS_LOADED NUMBER,
                    OBSERVED_HEADERS VARIANT,
                    SCHEMA_CHANGED BOOLEAN,
                    ORIGINAL_STAGE_PATH VARCHAR,
                    PREPARED_STAGE_PATH VARCHAR,
                    ERROR VARCHAR
                )"""
            )
            for asset in self.catalog.assets.values():
                cursor.execute(
                    f"""CREATE TABLE IF NOT EXISTS {self.qname(asset.raw_table)} (
                        RAW_RECORD VARIANT NOT NULL,
                        SOURCE_URL VARCHAR NOT NULL,
                        SOURCE_FILE VARCHAR NOT NULL,
                        SOURCE_FILE_SHA256 VARCHAR NOT NULL,
                        SOURCE_ROW_NUMBER NUMBER NOT NULL,
                        INGESTION_RUN_ID VARCHAR NOT NULL,
                        LOADED_AT TIMESTAMP_TZ NOT NULL
                    )"""
                )
                safe_asset = asset.name.replace("'", "''")
                cursor.execute(
                    f"""CREATE OR REPLACE VIEW {self.qname(asset.latest_view)} AS
                    SELECT r.*
                    FROM {self.qname(asset.raw_table)} r
                    JOIN (
                        SELECT SOURCE_FILE_SHA256, RUN_ID
                        FROM {self.qname(FILES_TABLE)}
                        WHERE ASSET = '{safe_asset}' AND STATUS = 'SUCCESS'
                        QUALIFY ROW_NUMBER() OVER (
                            ORDER BY FINISHED_AT DESC, RUN_ID DESC
                        ) = 1
                    ) successful
                      ON r.SOURCE_FILE_SHA256 = successful.SOURCE_FILE_SHA256
                     AND r.INGESTION_RUN_ID = successful.RUN_ID"""
                )
        self.connection.commit()

    def begin_attempt(
        self, run_id: str, batch_id: str | None, asset: Asset, mode: str
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {self.qname(FILES_TABLE)}
                (RUN_ID, BATCH_ID, ASSET, MODE, SOURCE_URL, STATUS, STARTED_AT)
                SELECT %s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP()""",
                (run_id, batch_id, asset.name, mode, asset.url),
            )
        self.connection.commit()

    def attach_file_metadata(
        self,
        run_id: str,
        download: DownloadResult,
        prepared: PreparedFile,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET SOURCE_FILE = %s, SOURCE_FILE_SHA256 = %s, ROWS_READ = %s,
                    OBSERVED_HEADERS = PARSE_JSON(%s), SCHEMA_CHANGED = %s
                WHERE RUN_ID = %s""",
                (
                    download.source_file,
                    download.sha256,
                    prepared.rows,
                    json.dumps(prepared.headers),
                    prepared.schema_changed,
                    run_id,
                ),
            )
        self.connection.commit()

    def find_success(self, asset: Asset, checksum: str) -> tuple[str, int] | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT RUN_ID, ROWS_LOADED
                FROM {self.qname(FILES_TABLE)}
                WHERE ASSET = %s AND SOURCE_FILE_SHA256 = %s AND STATUS = 'SUCCESS'
                QUALIFY ROW_NUMBER() OVER (ORDER BY FINISHED_AT DESC, RUN_ID DESC) = 1""",
                (asset.name, checksum),
            )
            row = cursor.fetchone()
        return (str(row[0]), int(row[1])) if row else None

    def mark_skipped(self, run_id: str, rows: int, successful_run_id: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET STATUS = 'SKIPPED', FINISHED_AT = CURRENT_TIMESTAMP(),
                    ROWS_LOADED = %s, ERROR = %s
                WHERE RUN_ID = %s""",
                (rows, f"Already loaded by run {successful_run_id}", run_id),
            )
        self.connection.commit()

    @staticmethod
    def _file_uri(path: Path) -> str:
        return path.resolve().as_uri().replace("'", "''")

    def publish(
        self,
        run_id: str,
        asset: Asset,
        download: DownloadResult,
        prepared: PreparedFile,
    ) -> int:
        stage = self.qname(STAGE)
        checksum = download.sha256
        original_prefix = f"original/{asset.name}/{checksum}"
        prepared_prefix = f"prepared/{asset.name}/{checksum}/{run_id}"
        temp_table = identifier(f"LOAD_{run_id.replace('-', '_')}")
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"PUT '{self._file_uri(download.path)}' @{stage}/{original_prefix} "
                "AUTO_COMPRESS = FALSE OVERWRITE = FALSE PARALLEL = 4"
            )
            for part in prepared.files:
                if part.stat().st_size:
                    cursor.execute(
                        f"PUT '{self._file_uri(part)}' @{stage}/{prepared_prefix} "
                        "AUTO_COMPRESS = TRUE OVERWRITE = TRUE PARALLEL = 4"
                    )
            cursor.execute(
                f"CREATE TEMPORARY TABLE {temp_table} "
                "(RAW_RECORD VARIANT NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL)"
            )
            if prepared.rows:
                cursor.execute(
                    f"""COPY INTO {temp_table} (RAW_RECORD, SOURCE_ROW_NUMBER)
                    FROM (
                        SELECT $1:raw_record, $1:source_row_number::NUMBER
                        FROM @{stage}/{prepared_prefix}
                    )
                    FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO)
                    ON_ERROR = ABORT_STATEMENT FORCE = TRUE"""
                )
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
            loaded = int(cursor.fetchone()[0])
            if loaded != prepared.rows:
                raise RuntimeError(
                    f"Prepared/load count mismatch for {asset.name}: "
                    f"read={prepared.rows}, staged={loaded}"
                )
            # COPY populates only a session-scoped temporary table. Commit that load
            # before opening the publication transaction for RAW + control metadata.
            self.connection.commit()
            try:
                cursor.execute("BEGIN")
                # Removes rows from a prior interrupted publication of this exact version.
                cursor.execute(
                    f"DELETE FROM {self.qname(asset.raw_table)} "
                    "WHERE SOURCE_FILE_SHA256 = %s",
                    (checksum,),
                )
                cursor.execute(
                    f"""INSERT INTO {self.qname(asset.raw_table)}
                    (RAW_RECORD, SOURCE_URL, SOURCE_FILE, SOURCE_FILE_SHA256,
                     SOURCE_ROW_NUMBER, INGESTION_RUN_ID, LOADED_AT)
                    SELECT RAW_RECORD, %s, %s, %s, SOURCE_ROW_NUMBER, %s,
                           CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
                    FROM {temp_table}""",
                    (asset.url, download.source_file, checksum, run_id),
                )
                cursor.execute(
                    f"""UPDATE {self.qname(FILES_TABLE)}
                    SET STATUS = 'SUCCESS', FINISHED_AT = CURRENT_TIMESTAMP(),
                        ROWS_LOADED = %s, ORIGINAL_STAGE_PATH = %s,
                        PREPARED_STAGE_PATH = %s
                    WHERE RUN_ID = %s""",
                    (
                        loaded,
                        f"@{STAGE}/{original_prefix}/{download.source_file}",
                        f"@{STAGE}/{prepared_prefix}/",
                        run_id,
                    ),
                )
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
        return loaded

    def mark_failed(self, run_id: str, error: str) -> None:
        safe_error = error[:8000]
        try:
            self.connection.rollback()
            with self.connection.cursor() as cursor:
                cursor.execute(
                    f"""UPDATE {self.qname(FILES_TABLE)}
                    SET STATUS = 'FAILED', FINISHED_AT = CURRENT_TIMESTAMP(), ERROR = %s
                    WHERE RUN_ID = %s""",
                    (safe_error, run_id),
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def start_batch(self, batch_id: str, mode: str, assets: Iterable[str]) -> None:
        assets_json = json.dumps(list(assets))
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"""INSERT INTO {self.qname(BATCHES_TABLE)}
                (BATCH_ID, MODE, REQUESTED_ASSETS, STATUS, STARTED_AT)
                SELECT %s, %s, PARSE_JSON(%s), 'RUNNING', CURRENT_TIMESTAMP()""",
                (batch_id, mode, assets_json),
            )
        self.connection.commit()

    def finish_batch(self, batch_id: str, requested_assets: Iterable[str]) -> dict:
        results: list[dict] = []
        with self.connection.cursor() as cursor:
            for asset in requested_assets:
                cursor.execute(
                    f"""SELECT STATUS, COALESCE(ROWS_READ, 0),
                               COALESCE(ROWS_LOADED, 0), ERROR, RUN_ID
                    FROM {self.qname(FILES_TABLE)}
                    WHERE BATCH_ID = %s AND ASSET = %s
                    QUALIFY ROW_NUMBER() OVER (ORDER BY STARTED_AT DESC, RUN_ID DESC) = 1""",
                    (batch_id, asset),
                )
                row = cursor.fetchone()
                if row:
                    results.append(
                        {
                            "asset": asset,
                            "status": str(row[0]),
                            "rows_read": int(row[1]),
                            "rows_loaded": int(row[2]),
                            "error": row[3],
                            "run_id": str(row[4]),
                        }
                    )
                else:
                    results.append(
                        {
                            "asset": asset,
                            "status": "FAILED",
                            "rows_read": 0,
                            "rows_loaded": 0,
                            "error": "No ingestion attempt was recorded",
                            "run_id": None,
                        }
                    )
            failed = [result for result in results if result["status"] == "FAILED"]
            status = "FAILED" if failed else "SUCCESS"
            summary = {
                "batch_id": batch_id,
                "status": status,
                "loaded": [r["asset"] for r in results if r["status"] == "SUCCESS"],
                "skipped": [r["asset"] for r in results if r["status"] == "SKIPPED"],
                "failed": [r["asset"] for r in failed],
                "rows_read": sum(r["rows_read"] for r in results),
                "rows_loaded": sum(r["rows_loaded"] for r in results),
                "assets": results,
            }
            cursor.execute(
                f"""UPDATE {self.qname(BATCHES_TABLE)}
                SET STATUS = %s, FINISHED_AT = CURRENT_TIMESTAMP(), SUMMARY = PARSE_JSON(%s),
                    ERROR = %s
                WHERE BATCH_ID = %s""",
                (
                    status,
                    json.dumps(summary),
                    "One or more required assets failed" if failed else None,
                    batch_id,
                ),
            )
        self.connection.commit()
        return summary

