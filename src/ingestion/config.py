from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def identifier(value: str) -> str:
    """Return a quoted Snowflake identifier after strict validation."""
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid Snowflake identifier: {value!r}")
    return f'"{value.upper()}"'


@dataclass(frozen=True)
class Asset:
    name: str
    url: str
    headers: tuple[str, ...]

    @property
    def raw_table(self) -> str:
        return f"{self.name.upper()}_RAW"

    @property
    def latest_view(self) -> str:
        return f"{self.name.upper()}_LATEST"


@dataclass(frozen=True)
class AssetCatalog:
    assets: dict[str, Asset]
    chunk_rows: int

    @classmethod
    def load(cls, path: str | Path) -> "AssetCatalog":
        with Path(path).open(encoding="utf-8") as handle:
            document: dict[str, Any] = yaml.safe_load(handle)
        base_url = str(document["base_url"]).rstrip("/")
        assets = {
            name: Asset(
                name=name,
                url=f"{base_url}/{name}.csv.gz",
                headers=tuple(spec["headers"]),
            )
            for name, spec in document["assets"].items()
        }
        return cls(assets=assets, chunk_rows=int(document.get("chunk_rows", 100_000)))

    def require(self, name: str) -> Asset:
        try:
            return self.assets[name]
        except KeyError as exc:
            valid = ", ".join(sorted(self.assets))
            raise ValueError(f"Unknown asset {name!r}. Valid assets: {valid}") from exc


@dataclass(frozen=True)
class SnowflakeSettings:
    account: str
    user: str
    role: str
    warehouse: str
    database: str
    schema: str
    auth_method: str
    password: str = field(repr=False)

    @classmethod
    def from_env(cls) -> "SnowflakeSettings":
        required = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_DATABASE",
            "SNOWFLAKE_BRONZE_SCHEMA",
        ]
        missing = [name for name in required if not os.getenv(name)]
        if missing:
            raise ValueError("Missing Snowflake settings: " + ", ".join(missing))
        auth_method = os.getenv("SNOWFLAKE_AUTH_METHOD", "password").strip().lower()
        if auth_method != "password":
            raise ValueError(
                "SNOWFLAKE_AUTH_METHOD must be password; "
                "private-key authentication is not supported by this pipeline"
            )
        password = os.getenv("SNOWFLAKE_PASSWORD")
        if not password:
            raise ValueError("SNOWFLAKE_PASSWORD is required for password authentication")
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_BRONZE_SCHEMA"],
            auth_method=auth_method,
            password=password,
        )

    @property
    def qualified_schema(self) -> str:
        return f"{identifier(self.database)}.{identifier(self.schema)}"

