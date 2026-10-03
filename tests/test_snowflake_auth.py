from __future__ import annotations

import logging

import pytest

from ingestion.cli import _format_error
from ingestion.config import AssetCatalog, SnowflakeSettings
from ingestion.snowflake import SnowflakeWarehouse


BASE_ENV = {
    "SNOWFLAKE_ACCOUNT": "test-account",
    "SNOWFLAKE_USER": "test-user",
    "SNOWFLAKE_ROLE": "test-role",
    "SNOWFLAKE_WAREHOUSE": "test-warehouse",
    "SNOWFLAKE_DATABASE": "test-database",
    "SNOWFLAKE_BRONZE_SCHEMA": "test-schema",
}


def _set_base_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in BASE_ENV.items():
        monkeypatch.setenv(name, value)
    for obsolete in (
        "SNOWFLAKE_PRIVATE_KEY_FILE",
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE",
    ):
        monkeypatch.delenv(obsolete, raising=False)


def test_password_without_private_key_reaches_connection_attempt(monkeypatch):
    _set_base_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_AUTH_METHOD", raising=False)
    password = "synthetic-test-password"
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", password)
    calls = []
    connection = object()

    def fake_connect(**kwargs):
        calls.append(kwargs)
        return connection

    monkeypatch.setattr("ingestion.snowflake.snowflake.connector.connect", fake_connect)

    settings = SnowflakeSettings.from_env()
    warehouse = SnowflakeWarehouse(settings, AssetCatalog({}, chunk_rows=1))

    assert warehouse.connection is connection
    assert settings.auth_method == "password"
    assert calls[0]["password"] == password
    assert calls[0]["authenticator"] == "snowflake"
    assert "private_key" not in calls[0]
    assert "private_key_file" not in calls[0]
    assert "private_key_file_pwd" not in calls[0]


def test_missing_password_has_clear_error(monkeypatch):
    _set_base_env(monkeypatch)
    monkeypatch.setenv("SNOWFLAKE_AUTH_METHOD", "password")
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

    with pytest.raises(
        ValueError,
        match="SNOWFLAKE_PASSWORD is required for password authentication",
    ):
        SnowflakeSettings.from_env()


def test_private_key_mode_is_not_supported(monkeypatch):
    _set_base_env(monkeypatch)
    monkeypatch.setenv("SNOWFLAKE_AUTH_METHOD", "private_key")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "synthetic-test-password")

    with pytest.raises(ValueError, match="must be password"):
        SnowflakeSettings.from_env()


def test_password_is_redacted_from_repr_and_error_output(monkeypatch, caplog):
    _set_base_env(monkeypatch)
    password = "do-not-log-this-synthetic-secret"
    monkeypatch.setenv("SNOWFLAKE_AUTH_METHOD", "password")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", password)
    settings = SnowflakeSettings.from_env()

    with caplog.at_level(logging.DEBUG):
        rendered = _format_error(RuntimeError(f"authentication failed: {password}"))

    assert password not in repr(settings)
    assert password not in rendered
    assert password not in caplog.text
    assert "[REDACTED]" in rendered
