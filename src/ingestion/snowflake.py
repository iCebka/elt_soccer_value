from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable

import snowflake.connector
from snowflake.connector.errors import OperationalError, ProgrammingError

from .config import Asset, AssetCatalog, SnowflakeSettings, identifier
from .models import DownloadResult, PreparedFile, RemoteMetadata, SourceVersion


DATASET = "Dataset1"
STAGE = "TRANSFERMARKT_BRONZE_STAGE"
FILES_TABLE = "TRANSFERMARKT_INGESTION_FILES"
BATCHES_TABLE = "TRANSFERMARKT_INGESTION_BATCHES"
JSON_FORMAT = "TRANSFERMARKT_NDJSON_FORMAT"


def _is_transient_snowflake(exc: OperationalError) -> bool:
    """Exclude configuration/authentication/policy failures from retry handling."""
    message = str(exc).lower()
    permanent_markers = (
        "incorrect username or password",
        "authentication",
        "multi-factor",
        "mfa",
        "account identifier",
        "does not exist or not authorized",
        "insufficient privileges",
        "not authorized",
    )
    return not any(marker in message for marker in permanent_markers)


def snowflake_connection_kwargs(
    settings: SnowflakeSettings,
    *,
    application: str,
) -> dict:
    """Build the one password-authenticated connector contract used by Bronze/Silver."""
    return {
        "account": settings.account,
        "user": settings.user,
        "password": settings.password,
        "authenticator": "snowflake",
        "role": settings.role,
        "warehouse": settings.warehouse,
        "database": settings.database,
        "login_timeout": 30,
        "network_timeout": 120,
        "application": application,
        "autocommit": False,
        "session_parameters": {"TIMEZONE": "UTC"},
    }


def connect_snowflake(
    settings: SnowflakeSettings,
    *,
    application: str,
):
    """Connect with at most three attempts, retrying transient failures only."""
    kwargs = snowflake_connection_kwargs(settings, application=application)
    for attempt in range(1, 4):
        try:
            return snowflake.connector.connect(**kwargs)
        except OperationalError as exc:
            if attempt == 3 or not _is_transient_snowflake(exc):
                raise
            time.sleep(min(10 * (2 ** (attempt - 1)), 120))
    raise AssertionError("unreachable")


class SnowflakeWarehouse:
    """Snowflake persistence boundary for an atomic asset publication."""

    def __init__(self, settings: SnowflakeSettings, catalog: AssetCatalog):
        self.settings = settings
        self.catalog = catalog
        self.connection = self._connect_with_retry()

    def _connection_kwargs(self) -> dict:
        return snowflake_connection_kwargs(
            self.settings,
            application="transfermarkt_bronze_ingestion",
        )

    def _connect_with_retry(self):
        return connect_snowflake(
            self.settings,
            application="transfermarkt_bronze_ingestion",
        )

    def _execute(self, cursor, sql: str, params=None, *, context: str):
        """Add safe statement/object context to Snowflake 002043 failures."""
        try:
            return cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        except ProgrammingError as exc:
            errno = getattr(exc, "errno", None)
            if errno == 2043 or "002043" in str(exc):
                statement = " ".join(sql.split())[:500]
                raise RuntimeError(
                    f"Snowflake 002043 while {context}; statement={statement}"
                ) from exc
            raise

    @property
    def schema(self) -> str:
        return self.settings.qualified_schema

    def qname(self, name: str) -> str:
        return f"{self.schema}.{identifier(name)}"

    def close(self) -> None:
        self.connection.close()

    def check_connection(self) -> None:
        with self.connection.cursor() as cursor:
            self._execute(cursor, "SELECT 1", context="checking the authenticated session")
            row = cursor.fetchone()
        if not row or int(row[0]) != 1:
            raise RuntimeError("Snowflake connection check returned an unexpected result")

    def recent_audit(self, limit: int = 25) -> dict[str, list[dict]]:
        """Return recent batch/file audit rows without exposing connection settings."""
        if not 1 <= limit <= 1000:
            raise ValueError("audit limit must be between 1 and 1000")

        def fetch(cursor, sql: str, *, context: str) -> list[dict]:
            self._execute(cursor, sql, context=context)
            columns = [str(column[0]).lower() for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

        with self.connection.cursor() as cursor:
            batches = fetch(
                cursor,
                f"""SELECT BATCH_ID, MODE, LOGICAL_DATE, TRIGGER_SOURCE, STATUS,
                           STARTED_AT, FINISHED_AT, SUMMARY, ERROR
                    FROM {self.qname(BATCHES_TABLE)}
                    ORDER BY STARTED_AT DESC
                    LIMIT {limit}""",
                context="reading recent Transfermarkt batch audit",
            )
            files = fetch(
                cursor,
                f"""SELECT DATASET, RUN_ID, BATCH_ID, ASSET, SOURCE_URL, STATUS,
                           LOGICAL_DATE, SOURCE_CHECKED_AT, CAPTURED_AT,
                           HEAD_STATUS, GET_STATUS, REMOTE_ETAG, DOWNLOAD_ETAG,
                           SOURCE_VERSION, SOURCE_FILE_SHA256, HTTP_ATTEMPTS,
                           REFERENCE_RUN_ID, SKIP_REASON, ROWS_READ, ROWS_LOADED,
                           ERROR
                    FROM {self.qname(FILES_TABLE)}
                    ORDER BY STARTED_AT DESC
                    LIMIT {limit}""",
                context="reading recent Transfermarkt file audit",
            )
        return {"batches": batches, "files": files}

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
            statements = (
                (f"USE ROLE {role}", f"using role {role}"),
                (f"USE WAREHOUSE {warehouse}", f"using warehouse {warehouse}"),
                (f"USE DATABASE {database}", f"using database {database}"),
                (f"CREATE SCHEMA IF NOT EXISTS {schema_name}", f"creating schema {schema_name}"),
                (f"USE SCHEMA {schema_name}", f"using schema {schema_name}"),
                (f"CREATE STAGE IF NOT EXISTS {self.qname(STAGE)}", f"creating stage {self.qname(STAGE)}"),
                (
                    f"CREATE FILE FORMAT IF NOT EXISTS {self.qname(JSON_FORMAT)} "
                    "TYPE = JSON COMPRESSION = AUTO STRIP_OUTER_ARRAY = FALSE",
                    f"creating file format {self.qname(JSON_FORMAT)}",
                ),
            )
            for sql, context in statements:
                self._execute(cursor, sql, context=context)

            self._execute(
                cursor,
                f"""CREATE TABLE IF NOT EXISTS {self.qname(BATCHES_TABLE)} (
                    BATCH_ID VARCHAR NOT NULL,
                    MODE VARCHAR NOT NULL,
                    REQUESTED_ASSETS VARIANT NOT NULL,
                    STATUS VARCHAR NOT NULL,
                    STARTED_AT TIMESTAMP_TZ NOT NULL,
                    FINISHED_AT TIMESTAMP_TZ,
                    SUMMARY VARIANT,
                    ERROR VARCHAR,
                    LOGICAL_DATE TIMESTAMP_TZ,
                    TRIGGER_SOURCE VARCHAR
                )""",
                context=f"creating table {self.qname(BATCHES_TABLE)}",
            )
            self._execute(
                cursor,
                f"""CREATE TABLE IF NOT EXISTS {self.qname(FILES_TABLE)} (
                    RUN_ID VARCHAR NOT NULL,
                    BATCH_ID VARCHAR,
                    DATASET VARCHAR,
                    ASSET VARCHAR NOT NULL,
                    MODE VARCHAR NOT NULL,
                    SOURCE_URL VARCHAR NOT NULL,
                    SOURCE_FILE VARCHAR,
                    SOURCE_FILE_SHA256 VARCHAR,
                    STATUS VARCHAR NOT NULL,
                    STARTED_AT TIMESTAMP_TZ NOT NULL,
                    FINISHED_AT TIMESTAMP_TZ,
                    LOGICAL_DATE TIMESTAMP_TZ,
                    TRIGGER_SOURCE VARCHAR,
                    SOURCE_CHECKED_AT TIMESTAMP_TZ,
                    CAPTURED_AT TIMESTAMP_TZ,
                    HEAD_STATUS NUMBER,
                    GET_STATUS NUMBER,
                    REMOTE_ETAG VARCHAR,
                    REMOTE_LAST_MODIFIED VARCHAR,
                    REMOTE_CONTENT_LENGTH NUMBER,
                    DOWNLOAD_ETAG VARCHAR,
                    DOWNLOAD_LAST_MODIFIED VARCHAR,
                    DOWNLOAD_CONTENT_LENGTH NUMBER,
                    SOURCE_VERSION VARCHAR,
                    HTTP_ATTEMPTS NUMBER,
                    REFERENCE_RUN_ID VARCHAR,
                    SKIP_REASON VARCHAR,
                    ROWS_READ NUMBER,
                    ROWS_LOADED NUMBER,
                    OBSERVED_HEADERS VARIANT,
                    SCHEMA_CHANGED BOOLEAN,
                    ORIGINAL_STAGE_PATH VARCHAR,
                    PREPARED_STAGE_PATH VARCHAR,
                    ERROR VARCHAR
                )""",
                context=f"creating table {self.qname(FILES_TABLE)}",
            )

            # Preserve existing data: CREATE TABLE IF NOT EXISTS does not add columns.
            batch_columns = {
                "LOGICAL_DATE": "TIMESTAMP_TZ",
                "TRIGGER_SOURCE": "VARCHAR",
            }
            file_columns = {
                "DATASET": "VARCHAR",
                "LOGICAL_DATE": "TIMESTAMP_TZ",
                "TRIGGER_SOURCE": "VARCHAR",
                "SOURCE_CHECKED_AT": "TIMESTAMP_TZ",
                "CAPTURED_AT": "TIMESTAMP_TZ",
                "HEAD_STATUS": "NUMBER",
                "GET_STATUS": "NUMBER",
                "REMOTE_ETAG": "VARCHAR",
                "REMOTE_LAST_MODIFIED": "VARCHAR",
                "REMOTE_CONTENT_LENGTH": "NUMBER",
                "DOWNLOAD_ETAG": "VARCHAR",
                "DOWNLOAD_LAST_MODIFIED": "VARCHAR",
                "DOWNLOAD_CONTENT_LENGTH": "NUMBER",
                "SOURCE_VERSION": "VARCHAR",
                "HTTP_ATTEMPTS": "NUMBER",
                "REFERENCE_RUN_ID": "VARCHAR",
                "SKIP_REASON": "VARCHAR",
            }
            for column, data_type in batch_columns.items():
                self._execute(
                    cursor,
                    f"ALTER TABLE {self.qname(BATCHES_TABLE)} "
                    f"ADD COLUMN IF NOT EXISTS {identifier(column)} {data_type}",
                    context=f"migrating {self.qname(BATCHES_TABLE)}.{column}",
                )
            for column, data_type in file_columns.items():
                self._execute(
                    cursor,
                    f"ALTER TABLE {self.qname(FILES_TABLE)} "
                    f"ADD COLUMN IF NOT EXISTS {identifier(column)} {data_type}",
                    context=f"migrating {self.qname(FILES_TABLE)}.{column}",
                )

            for asset in self.catalog.assets.values():
                self._execute(
                    cursor,
                    f"""CREATE TABLE IF NOT EXISTS {self.qname(asset.raw_table)} (
                        RAW_RECORD VARIANT NOT NULL,
                        SOURCE_URL VARCHAR NOT NULL,
                        SOURCE_FILE VARCHAR NOT NULL,
                        SOURCE_FILE_SHA256 VARCHAR NOT NULL,
                        SOURCE_ROW_NUMBER NUMBER NOT NULL,
                        INGESTION_RUN_ID VARCHAR NOT NULL,
                        LOADED_AT TIMESTAMP_TZ NOT NULL
                    )""",
                    context=f"creating table {self.qname(asset.raw_table)}",
                )
                safe_asset = asset.name.replace("'", "''")
                self._execute(
                    cursor,
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
                     AND r.INGESTION_RUN_ID = successful.RUN_ID""",
                    context=f"creating view {self.qname(asset.latest_view)}",
                )
        self.connection.commit()

    def begin_attempt(
        self,
        run_id: str,
        batch_id: str | None,
        asset: Asset,
        mode: str,
        logical_date: str | None,
        trigger_source: str,
    ) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""INSERT INTO {self.qname(FILES_TABLE)}
                (RUN_ID, BATCH_ID, DATASET, ASSET, MODE, SOURCE_URL, STATUS,
                 STARTED_AT, LOGICAL_DATE, TRIGGER_SOURCE)
                SELECT %s, %s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP(),
                       TRY_TO_TIMESTAMP_TZ(%s::VARCHAR), %s""",
                (
                    run_id,
                    batch_id,
                    DATASET,
                    asset.name,
                    mode,
                    asset.url,
                    logical_date,
                    trigger_source,
                ),
                context=f"recording ingestion attempt for {asset.name}",
            )
        self.connection.commit()

    @staticmethod
    def _source_version(row) -> SourceVersion | None:
        if not row:
            return None
        return SourceVersion(
            run_id=str(row[0]),
            checksum=str(row[1]),
            rows=int(row[2] or 0),
            source_file=str(row[3]) if row[3] is not None else None,
            etag=str(row[4]) if row[4] is not None else None,
            last_modified=str(row[5]) if row[5] is not None else None,
            content_length=int(row[6]) if row[6] is not None else None,
            captured_at=str(row[7]) if row[7] is not None else None,
        )

    def latest_source_version(self, asset: Asset) -> SourceVersion | None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""SELECT RUN_ID, SOURCE_FILE_SHA256, ROWS_LOADED, SOURCE_FILE,
                           COALESCE(DOWNLOAD_ETAG, REMOTE_ETAG),
                           COALESCE(DOWNLOAD_LAST_MODIFIED, REMOTE_LAST_MODIFIED),
                           COALESCE(DOWNLOAD_CONTENT_LENGTH, REMOTE_CONTENT_LENGTH),
                           CAPTURED_AT
                FROM {self.qname(FILES_TABLE)}
                WHERE ASSET = %s AND SOURCE_URL = %s
                  AND COALESCE(DATASET, %s) = %s
                  AND STATUS IN ('SUCCESS', 'SKIPPED_UNCHANGED')
                  AND SOURCE_FILE_SHA256 IS NOT NULL
                QUALIFY ROW_NUMBER() OVER (
                    ORDER BY FINISHED_AT DESC, RUN_ID DESC
                ) = 1""",
                (asset.name, asset.url, DATASET, DATASET),
                context=f"reading the latest successful source version for {asset.name}",
            )
            return self._source_version(cursor.fetchone())

    def record_probe(self, run_id: str, metadata: RemoteMetadata) -> None:
        status_column = "HEAD_STATUS" if metadata.method == "HEAD" else "GET_STATUS"
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET SOURCE_CHECKED_AT = TRY_TO_TIMESTAMP_TZ(%s::VARCHAR),
                    {status_column} = %s,
                    REMOTE_ETAG = COALESCE(%s, REMOTE_ETAG),
                    REMOTE_LAST_MODIFIED = COALESCE(%s, REMOTE_LAST_MODIFIED),
                    REMOTE_CONTENT_LENGTH = COALESCE(%s, REMOTE_CONTENT_LENGTH),
                    HTTP_ATTEMPTS = %s
                WHERE RUN_ID = %s""",
                (
                    metadata.checked_at,
                    metadata.status_code,
                    metadata.etag,
                    metadata.last_modified,
                    metadata.content_length,
                    metadata.attempts,
                    run_id,
                ),
                context=f"recording {metadata.method} metadata for run {run_id}",
            )
        self.connection.commit()

    def record_download(self, run_id: str, download: DownloadResult) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET SOURCE_FILE = %s, SOURCE_FILE_SHA256 = %s,
                    CAPTURED_AT = TRY_TO_TIMESTAMP_TZ(%s::VARCHAR), GET_STATUS = %s,
                    DOWNLOAD_ETAG = %s, DOWNLOAD_LAST_MODIFIED = %s,
                    DOWNLOAD_CONTENT_LENGTH = %s,
                    SOURCE_VERSION = COALESCE(%s, %s), HTTP_ATTEMPTS = %s
                WHERE RUN_ID = %s""",
                (
                    download.source_file,
                    download.sha256,
                    download.captured_at,
                    download.status_code,
                    download.etag,
                    download.last_modified,
                    download.content_length,
                    download.etag,
                    download.sha256,
                    download.attempts,
                    run_id,
                ),
                context=f"recording downloaded version for run {run_id}",
            )
        self.connection.commit()

    def attach_file_metadata(self, run_id: str, prepared: PreparedFile) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET ROWS_READ = %s, OBSERVED_HEADERS = PARSE_JSON(%s),
                    SCHEMA_CHANGED = %s
                WHERE RUN_ID = %s""",
                (
                    prepared.rows,
                    json.dumps(prepared.headers),
                    prepared.schema_changed,
                    run_id,
                ),
                context=f"recording prepared file metadata for run {run_id}",
            )
        self.connection.commit()

    def find_success(self, asset: Asset, checksum: str) -> SourceVersion | None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""SELECT RUN_ID, SOURCE_FILE_SHA256, ROWS_LOADED, SOURCE_FILE,
                           DOWNLOAD_ETAG, DOWNLOAD_LAST_MODIFIED,
                           DOWNLOAD_CONTENT_LENGTH, CAPTURED_AT
                FROM {self.qname(FILES_TABLE)}
                WHERE ASSET = %s AND SOURCE_URL = %s
                  AND COALESCE(DATASET, %s) = %s
                  AND SOURCE_FILE_SHA256 = %s AND STATUS = 'SUCCESS'
                QUALIFY ROW_NUMBER() OVER (
                    ORDER BY FINISHED_AT DESC, RUN_ID DESC
                ) = 1""",
                (asset.name, asset.url, DATASET, DATASET, checksum),
                context=f"checking checksum idempotency for {asset.name}",
            )
            return self._source_version(cursor.fetchone())

    def mark_skipped_unchanged(
        self,
        run_id: str,
        reference: SourceVersion,
        reason: str,
        http_attempts: int,
    ) -> None:
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""UPDATE {self.qname(FILES_TABLE)}
                SET STATUS = 'SKIPPED_UNCHANGED', FINISHED_AT = CURRENT_TIMESTAMP(),
                    SOURCE_FILE = COALESCE(SOURCE_FILE, %s),
                    SOURCE_FILE_SHA256 = COALESCE(SOURCE_FILE_SHA256, %s),
                    CAPTURED_AT = COALESCE(
                        CAPTURED_AT, TRY_TO_TIMESTAMP_TZ(%s::VARCHAR)
                    ),
                    REMOTE_ETAG = COALESCE(REMOTE_ETAG, %s),
                    REMOTE_LAST_MODIFIED = COALESCE(REMOTE_LAST_MODIFIED, %s),
                    REMOTE_CONTENT_LENGTH = COALESCE(REMOTE_CONTENT_LENGTH, %s),
                    SOURCE_VERSION = COALESCE(DOWNLOAD_ETAG, REMOTE_ETAG, %s),
                    REFERENCE_RUN_ID = %s, SKIP_REASON = %s,
                    ROWS_READ = 0, ROWS_LOADED = 0, HTTP_ATTEMPTS = %s,
                    ERROR = NULL
                WHERE RUN_ID = %s""",
                (
                    reference.source_file,
                    reference.checksum,
                    reference.captured_at,
                    reference.etag,
                    reference.last_modified,
                    reference.content_length,
                    reference.checksum,
                    reference.run_id,
                    reason,
                    http_attempts,
                    run_id,
                ),
                context=f"committing unchanged checkpoint for run {run_id}",
            )
        self.connection.commit()

    @staticmethod
    def _file_uri(path: Path) -> str:
        return path.resolve().as_uri().replace("'", "''")

    def _publish_once(
        self,
        run_id: str,
        asset: Asset,
        download: DownloadResult,
        prepared: PreparedFile,
        http_attempts: int,
    ) -> int:
        stage = self.qname(STAGE)
        checksum = download.sha256
        original_prefix = f"original/{asset.name}/{checksum}"
        prepared_prefix = f"prepared/{asset.name}/{checksum}/{run_id}"
        temp_table = identifier(f"LOAD_{run_id.replace('-', '_')}")
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"PUT '{self._file_uri(download.path)}' @{stage}/{original_prefix} "
                "AUTO_COMPRESS = FALSE OVERWRITE = FALSE PARALLEL = 4",
                context=f"archiving {asset.name} original in {stage}",
            )
            for part in prepared.files:
                if part.stat().st_size:
                    self._execute(
                        cursor,
                        f"PUT '{self._file_uri(part)}' @{stage}/{prepared_prefix} "
                        "AUTO_COMPRESS = TRUE OVERWRITE = TRUE PARALLEL = 4",
                        context=f"staging prepared {asset.name} data in {stage}",
                    )
            self._execute(
                cursor,
                f"CREATE OR REPLACE TEMPORARY TABLE {temp_table} "
                "(RAW_RECORD VARIANT NOT NULL, SOURCE_ROW_NUMBER NUMBER NOT NULL)",
                context=f"creating temporary load table {temp_table}",
            )
            if prepared.rows:
                self._execute(
                    cursor,
                    f"""COPY INTO {temp_table} (RAW_RECORD, SOURCE_ROW_NUMBER)
                    FROM (
                        SELECT $1:raw_record, $1:source_row_number::NUMBER
                        FROM @{stage}/{prepared_prefix}
                    )
                    FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO)
                    ON_ERROR = ABORT_STATEMENT FORCE = TRUE""",
                    context=f"copying staged data into {temp_table}",
                )
            self._execute(
                cursor,
                f"SELECT COUNT(*) FROM {temp_table}",
                context=f"counting staged rows in {temp_table}",
            )
            loaded = int(cursor.fetchone()[0])
            if loaded != prepared.rows:
                raise RuntimeError(
                    f"Prepared/load count mismatch for {asset.name}: "
                    f"read={prepared.rows}, staged={loaded}"
                )
            self.connection.commit()
            try:
                self._execute(cursor, "BEGIN", context=f"beginning publication for {asset.name}")
                # Recovery is idempotent for this dataset/table/URL/checksum.
                self._execute(
                    cursor,
                    f"DELETE FROM {self.qname(asset.raw_table)} "
                    "WHERE SOURCE_URL = %s AND SOURCE_FILE_SHA256 = %s",
                    (asset.url, checksum),
                    context=f"removing an interrupted version from {self.qname(asset.raw_table)}",
                )
                self._execute(
                    cursor,
                    f"""INSERT INTO {self.qname(asset.raw_table)}
                    (RAW_RECORD, SOURCE_URL, SOURCE_FILE, SOURCE_FILE_SHA256,
                     SOURCE_ROW_NUMBER, INGESTION_RUN_ID, LOADED_AT)
                    SELECT RAW_RECORD, %s, %s, %s, SOURCE_ROW_NUMBER, %s,
                           CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())
                    FROM {temp_table}""",
                    (asset.url, download.source_file, checksum, run_id),
                    context=f"publishing version into {self.qname(asset.raw_table)}",
                )
                self._execute(
                    cursor,
                    f"""UPDATE {self.qname(FILES_TABLE)}
                    SET STATUS = 'SUCCESS', FINISHED_AT = CURRENT_TIMESTAMP(),
                        ROWS_LOADED = %s, ORIGINAL_STAGE_PATH = %s,
                        PREPARED_STAGE_PATH = %s,
                        SOURCE_VERSION = COALESCE(DOWNLOAD_ETAG, %s),
                        HTTP_ATTEMPTS = %s, ERROR = NULL
                    WHERE RUN_ID = %s""",
                    (
                        loaded,
                        f"@{STAGE}/{original_prefix}/{download.source_file}",
                        f"@{STAGE}/{prepared_prefix}/",
                        checksum,
                        http_attempts,
                        run_id,
                    ),
                    context=f"committing successful checkpoint for run {run_id}",
                )
                self._execute(cursor, "COMMIT", context=f"committing publication for {asset.name}")
            except Exception:
                try:
                    cursor.execute("ROLLBACK")
                except Exception:
                    pass
                raise
        return loaded

    def publish(
        self,
        run_id: str,
        asset: Asset,
        download: DownloadResult,
        prepared: PreparedFile,
        http_attempts: int,
    ) -> int:
        """Retry only transient connector failures, reusing the captured file."""
        for attempt in range(1, 4):
            try:
                return self._publish_once(
                    run_id, asset, download, prepared, http_attempts
                )
            except OperationalError as exc:
                if attempt == 3 or not _is_transient_snowflake(exc):
                    raise
                try:
                    self.connection.close()
                except Exception:
                    pass
                time.sleep(min(10 * (2 ** (attempt - 1)), 120))
                # One connection attempt belongs to this publication retry; do not
                # nest the separate initial-connection retry loop.
                self.connection = snowflake.connector.connect(**self._connection_kwargs())
        raise AssertionError("unreachable")

    def mark_failed(self, run_id: str, error: str, http_attempts: int) -> None:
        safe_error = error[:8000]
        try:
            self.connection.rollback()
            with self.connection.cursor() as cursor:
                self._execute(
                    cursor,
                    f"""UPDATE {self.qname(FILES_TABLE)}
                    SET STATUS = 'FAILED', FINISHED_AT = CURRENT_TIMESTAMP(),
                        ERROR = %s, HTTP_ATTEMPTS = %s
                    WHERE RUN_ID = %s
                      AND STATUS NOT IN ('SUCCESS', 'SKIPPED_UNCHANGED')""",
                    (safe_error, http_attempts, run_id),
                    context=f"recording failure for run {run_id}",
                )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def start_batch(
        self,
        batch_id: str,
        mode: str,
        assets: Iterable[str],
        logical_date: str | None,
        trigger_source: str,
    ) -> None:
        assets_json = json.dumps(list(assets))
        with self.connection.cursor() as cursor:
            self._execute(
                cursor,
                f"""INSERT INTO {self.qname(BATCHES_TABLE)}
                (BATCH_ID, MODE, REQUESTED_ASSETS, STATUS, STARTED_AT,
                 LOGICAL_DATE, TRIGGER_SOURCE)
                SELECT %s, %s, PARSE_JSON(%s), 'RUNNING', CURRENT_TIMESTAMP(),
                       TRY_TO_TIMESTAMP_TZ(%s::VARCHAR), %s""",
                (batch_id, mode, assets_json, logical_date, trigger_source),
                context=f"starting batch {batch_id}",
            )
        self.connection.commit()

    def finish_batch(self, batch_id: str, requested_assets: Iterable[str]) -> dict:
        results: list[dict] = []
        with self.connection.cursor() as cursor:
            for asset in requested_assets:
                self._execute(
                    cursor,
                    f"""SELECT STATUS, COALESCE(ROWS_READ, 0),
                               COALESCE(ROWS_LOADED, 0), ERROR, RUN_ID
                    FROM {self.qname(FILES_TABLE)}
                    WHERE BATCH_ID = %s AND ASSET = %s
                    QUALIFY ROW_NUMBER() OVER (
                        ORDER BY STARTED_AT DESC, RUN_ID DESC
                    ) = 1""",
                    (batch_id, asset),
                    context=f"reading batch result for {asset}",
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
            successful_statuses = {"SUCCESS", "SKIPPED_UNCHANGED", "SKIPPED"}
            failed = [r for r in results if r["status"] not in successful_statuses]
            status = "FAILED" if failed else "SUCCESS"
            summary = {
                "batch_id": batch_id,
                "status": status,
                "loaded": [r["asset"] for r in results if r["status"] == "SUCCESS"],
                "skipped": [
                    r["asset"]
                    for r in results
                    if r["status"] in {"SKIPPED_UNCHANGED", "SKIPPED"}
                ],
                "failed": [r["asset"] for r in failed],
                "rows_read": sum(r["rows_read"] for r in results),
                "rows_loaded": sum(r["rows_loaded"] for r in results),
                "assets": results,
            }
            self._execute(
                cursor,
                f"""UPDATE {self.qname(BATCHES_TABLE)}
                SET STATUS = %s, FINISHED_AT = CURRENT_TIMESTAMP(),
                    SUMMARY = PARSE_JSON(%s), ERROR = %s
                WHERE BATCH_ID = %s""",
                (
                    status,
                    json.dumps(summary),
                    "One or more required assets failed" if failed else None,
                    batch_id,
                ),
                context=f"finalizing batch {batch_id}",
            )
        self.connection.commit()
        return summary
