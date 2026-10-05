#!/usr/bin/env python3

"""
Build GOLD.OBT_FOOTBALL_EVENTS from the Gold dimensional model.

Contract:
    - Input: four Gold facts + Gold dimensions.
    - One Gold fact row -> exactly one OBT row.
    - No additional cleaning or filtering.
    - Optional dimensional joins never remove observations.
    - Joins must never multiply observations.
    - dim_date coverage is mandatory.
    - event_id is globally unique and deterministic.
    - Output is rebuilt with overwrite semantics.

The Snowflake Spark connector must already be available to spark-submit.
"""

from __future__ import annotations

import os
import sys
from functools import reduce
from typing import Dict, Iterable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SNOWFLAKE_FORMAT = "net.snowflake.spark.snowflake"

OUTPUT_TABLE = "OBT_FOOTBALL_EVENTS"

EVENT_TYPES = {
    "PLAYER_MATCH",
    "MATCH_ACTION",
    "TRANSFER",
    "VALUATION",
}


# Columns present before dimensional enrichment.
#
# Every projection is forced into this common schema before unionByName.
PRE_ENRICH_COLUMNS = [
    # Identity and lineage
    "event_id",
    "event_type",
    "event_subtype",
    "source_fact",
    "source_record_id",
    "source_key",

    # Time
    "event_date_key",
    "event_date",

    # Player context
    "player_id",
    "player_assist_id",
    "player_in_id",

    # Club context
    "club_id",
    "opponent_club_id",
    "from_club_id",
    "to_club_id",

    # Competition
    "competition_id",

    # Match context
    "game_id",
    "minute",

    # Player-match measures
    "home_or_away",
    "is_win",
    "is_starter",
    "is_captain",
    "minutes_played",
    "goals",
    "assists",
    "yellow_cards",
    "red_cards",

    # Match-action context
    "action_description",

    # Transfer context
    "transfer_season",
    "transfer_fee",

    # Economic measure
    "market_value_in_eur",
]


# Final physical column order in Snowflake.
FINAL_COLUMNS = [
    # Identity and lineage
    "event_id",
    "event_type",
    "event_subtype",
    "source_fact",
    "source_record_id",
    "source_key",

    # Time
    "event_date_key",
    "event_date",
    "event_year",
    "event_quarter",
    "event_month",
    "event_month_name",
    "event_day",
    "event_day_of_week",
    "event_day_name",
    "event_week_of_year",

    # Main player
    "player_id",
    "player_code",
    "player_name",
    "player_first_name",
    "player_last_name",
    "player_date_of_birth",
    "player_country_of_birth",
    "player_city_of_birth",
    "player_country_of_citizenship",
    "player_position",
    "player_sub_position",
    "player_foot",
    "player_height_in_cm",
    "contract_expiration_date",

    # Related players
    "player_assist_id",
    "player_assist_name",
    "player_in_id",
    "player_in_name",

    # Club context
    "club_id",
    "club_name",
    "opponent_club_id",
    "opponent_club_name",
    "from_club_id",
    "from_club_name",
    "to_club_id",
    "to_club_name",

    # Competition context
    "competition_id",
    "competition_name",
    "competition_type",
    "competition_sub_type",
    "competition_country_id",
    "competition_country_name",
    "competition_confederation",

    # Match
    "game_id",
    "minute",
    "home_or_away",
    "is_win",
    "is_starter",
    "is_captain",
    "minutes_played",
    "goals",
    "assists",
    "yellow_cards",
    "red_cards",
    "action_description",

    # Transfer
    "transfer_season",
    "transfer_fee",

    # Economic measure
    "market_value_in_eur",
]


# ---------------------------------------------------------------------------
# Environment and Snowflake
# ---------------------------------------------------------------------------

def require_env(name: str) -> str:
    """
    Resolve either the regular environment name or Kestra's ENV_ alias.

    Example:
        SNOWFLAKE_ACCOUNT
        ENV_SNOWFLAKE_ACCOUNT
    """

    value = os.getenv(name)

    if value is None or not value.strip():
        value = os.getenv(f"ENV_{name}")

    if value is None or not value.strip():
        raise RuntimeError(
            f"Required environment variable is missing: "
            f"{name} / ENV_{name}"
        )

    return value.strip()

def snowflake_url(account: str) -> str:
    """
    Accept either:
        xy12345.eu-central-1
        xy12345.eu-central-1.snowflakecomputing.com
        https://xy12345.eu-central-1.snowflakecomputing.com
    """

    value = account.strip()

    if value.startswith("https://"):
        value = value[len("https://"):]

    if value.startswith("http://"):
        value = value[len("http://"):]

    value = value.rstrip("/")

    if not value.endswith(".snowflakecomputing.com"):
        value = f"{value}.snowflakecomputing.com"

    return value


def get_snowflake_options() -> Dict[str, str]:
    account = require_env("SNOWFLAKE_ACCOUNT")

    return {
        "sfURL": snowflake_url(account),
        "sfUser": require_env("SNOWFLAKE_USER"),
        "sfPassword": require_env("SNOWFLAKE_PASSWORD"),
        "sfDatabase": require_env("SNOWFLAKE_DATABASE"),
        "sfSchema": (os.getenv("SNOWFLAKE_GOLD_SCHEMA") or os.getenv("ENV_SNOWFLAKE_GOLD_SCHEMA") or "GOLD"),
        "sfWarehouse": require_env("SNOWFLAKE_WAREHOUSE"),
        "sfRole": require_env("SNOWFLAKE_ROLE"),
        "disablePlatformDetection": "true",
    }


def lowercase_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[column.lower() for column in df.columns])


def load_gold_table(
    spark: SparkSession,
    sf_options: Dict[str, str],
    table: str,
) -> DataFrame:

    df = (
        spark.read
        .format(SNOWFLAKE_FORMAT)
        .options(**sf_options)
        .option("dbtable", table)
        .load()
    )

    return lowercase_columns(df)


# ---------------------------------------------------------------------------
# Generic validation helpers
# ---------------------------------------------------------------------------

def require_columns(
    df: DataFrame,
    table_name: str,
    required: Iterable[str],
) -> None:

    missing = sorted(set(required) - set(df.columns))

    if missing:
        raise RuntimeError(
            f"{table_name} is missing required columns: {missing}"
        )


def fail_if_rows(df: DataFrame, message: str) -> None:
    if df.limit(1).count() > 0:
        raise RuntimeError(message)


def print_count(label: str, count: int) -> None:
    print(f"[COUNT] {label}: {count:,}")


# ---------------------------------------------------------------------------
# Canonical source_key / event_id
# ---------------------------------------------------------------------------

def string_value(column) -> F.Column:
    """
    Canonical representation for identifiers.

    NULL is represented explicitly so the serialization is deterministic.
    """

    return F.coalesce(
        column.cast("string"),
        F.lit("<NULL>"),
    )


def date_value(column) -> F.Column:
    """
    Canonical ISO representation YYYY-MM-DD.
    """

    return F.coalesce(
        F.date_format(column, "yyyy-MM-dd"),
        F.lit("<NULL>"),
    )


def key_part(name: str, value: F.Column) -> F.Column:
    return F.concat(F.lit(f"{name}="), value)


def source_key(*parts: F.Column) -> F.Column:
    return F.concat_ws("|", *parts)


def add_event_id(df: DataFrame) -> DataFrame:
    payload = F.concat(
        F.col("event_type"),
        F.lit("|"),
        F.col("source_key"),
    )

    return df.withColumn(
        "event_id",
        F.sha2(payload, 256),
    )


# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------

def canonicalize(df: DataFrame) -> DataFrame:
    """
    Add structural NULLs for fields that do not apply to an event family.

    No row is filtered here.
    """

    result = df

    for column_name in PRE_ENRICH_COLUMNS:
        if column_name not in result.columns:
            result = result.withColumn(column_name, F.lit(None))

    return result.select(*PRE_ENRICH_COLUMNS)


# ---------------------------------------------------------------------------
# Projection: PLAYER_MATCH
# ---------------------------------------------------------------------------

def build_player_match(df: DataFrame) -> DataFrame:
    require_columns(
        df,
        "FACT_PLAYER_MATCH",
        {
            "appearance_id",
            "game_id",
            "player_id",
            "game_date",
            "game_date_key",
            "club_id",
            "opponent_club_id",
            "competition_id",
            "home_or_away",
            "is_win",
            "is_starter",
            "is_captain",
            "minutes_played",
            "goals",
            "assists",
            "yellow_cards",
            "red_cards",
        },
    )

    result = df.select(
        F.lit("PLAYER_MATCH").alias("event_type"),
        F.lit(None).cast("string").alias("event_subtype"),
        F.lit("fact_player_match").alias("source_fact"),
        F.col("appearance_id").cast("string").alias("source_record_id"),

        source_key(
            key_part("game_id", string_value(F.col("game_id"))),
            key_part("player_id", string_value(F.col("player_id"))),
        ).alias("source_key"),

        F.col("game_date_key").alias("event_date_key"),
        F.col("game_date").alias("event_date"),

        F.col("player_id"),
        F.col("club_id"),
        F.col("opponent_club_id"),
        F.col("competition_id"),
        F.col("game_id"),

        F.col("home_or_away"),
        F.col("is_win"),
        F.col("is_starter"),
        F.col("is_captain"),
        F.col("minutes_played"),
        F.col("goals"),
        F.col("assists"),
        F.col("yellow_cards"),
        F.col("red_cards"),
    )

    return canonicalize(add_event_id(result))


# ---------------------------------------------------------------------------
# Projection: MATCH_ACTION
# ---------------------------------------------------------------------------

def build_match_action(df: DataFrame) -> DataFrame:
    require_columns(
        df,
        "FACT_MATCH_EVENT",
        {
            "game_event_id",
            "game_id",
            "event_date",
            "event_date_key",
            "competition_id",
            "minute",
            "event_type",
            "player_id",
            "club_id",
            "player_assist_id",
            "player_in_id",
            "description",
        },
    )

    result = df.select(
        F.lit("MATCH_ACTION").alias("event_type"),
        F.col("event_type").alias("event_subtype"),
        F.lit("fact_match_event").alias("source_fact"),
        F.col("game_event_id").cast("string").alias("source_record_id"),

        source_key(
            key_part(
                "game_event_id",
                string_value(F.col("game_event_id")),
            ),
        ).alias("source_key"),

        F.col("event_date_key"),
        F.col("event_date"),

        F.col("player_id"),
        F.col("player_assist_id"),
        F.col("player_in_id"),
        F.col("club_id"),
        F.col("competition_id"),
        F.col("game_id"),
        F.col("minute"),
        F.col("description").alias("action_description"),
    )

    return canonicalize(add_event_id(result))


# ---------------------------------------------------------------------------
# Projection: TRANSFER
# ---------------------------------------------------------------------------

def build_transfer(df: DataFrame) -> DataFrame:
    require_columns(
        df,
        "FACT_TRANSFER",
        {
            "player_id",
            "transfer_date",
            "transfer_date_key",
            "transfer_season",
            "from_club_id",
            "to_club_id",
            "transfer_fee",
            "market_value_in_eur",
        },
    )

    result = df.select(
        F.lit("TRANSFER").alias("event_type"),
        F.lit(None).cast("string").alias("event_subtype"),
        F.lit("fact_transfer").alias("source_fact"),
        F.lit(None).cast("string").alias("source_record_id"),

        source_key(
            key_part(
                "player_id",
                string_value(F.col("player_id")),
            ),
            key_part(
                "transfer_date",
                date_value(F.col("transfer_date")),
            ),
            key_part(
                "from_club_id",
                string_value(F.col("from_club_id")),
            ),
            key_part(
                "to_club_id",
                string_value(F.col("to_club_id")),
            ),
        ).alias("source_key"),

        F.col("transfer_date_key").alias("event_date_key"),
        F.col("transfer_date").alias("event_date"),

        F.col("player_id"),
        F.col("from_club_id"),
        F.col("to_club_id"),
        F.col("transfer_season"),
        F.col("transfer_fee"),
        F.col("market_value_in_eur"),
    )

    return canonicalize(add_event_id(result))


# ---------------------------------------------------------------------------
# Projection: VALUATION
# ---------------------------------------------------------------------------

def build_valuation(df: DataFrame) -> DataFrame:
    require_columns(
        df,
        "FACT_PLAYER_VALUATION",
        {
            "player_id",
            "valuation_date",
            "valuation_date_key",
            "club_id",
            "competition_id",
            "market_value_in_eur",
        },
    )

    result = df.select(
        F.lit("VALUATION").alias("event_type"),
        F.lit(None).cast("string").alias("event_subtype"),
        F.lit("fact_player_valuation").alias("source_fact"),
        F.lit(None).cast("string").alias("source_record_id"),

        source_key(
            key_part(
                "player_id",
                string_value(F.col("player_id")),
            ),
            key_part(
                "valuation_date",
                date_value(F.col("valuation_date")),
            ),
        ).alias("source_key"),

        F.col("valuation_date_key").alias("event_date_key"),
        F.col("valuation_date").alias("event_date"),

        F.col("player_id"),
        F.col("club_id"),
        F.col("competition_id"),
        F.col("market_value_in_eur"),
    )

    return canonicalize(add_event_id(result))


# ---------------------------------------------------------------------------
# OBT validation before enrichment
# ---------------------------------------------------------------------------

def event_counts(df: DataFrame) -> Dict[str, int]:
    return {
        row["event_type"]: row["count"]
        for row in df.groupBy("event_type").count().collect()
    }


def validate_projection_counts(
    df: DataFrame,
    expected: Dict[str, int],
) -> int:

    actual = event_counts(df)

    unknown = set(actual) - EVENT_TYPES

    if unknown:
        raise RuntimeError(
            f"Unexpected event_type values after projection: {sorted(unknown)}"
        )

    for event_type in sorted(EVENT_TYPES):
        expected_count = expected[event_type]
        actual_count = actual.get(event_type, 0)

        if actual_count != expected_count:
            raise RuntimeError(
                f"Cardinality mismatch for {event_type}: "
                f"expected {expected_count}, got {actual_count}"
            )

        print_count(f"OBT {event_type}", actual_count)

    expected_total = sum(expected.values())
    actual_total = sum(actual.values())

    if actual_total != expected_total:
        raise RuntimeError(
            f"Total OBT cardinality mismatch before enrichment: "
            f"expected {expected_total}, got {actual_total}"
        )

    print_count("OBT total before enrichment", actual_total)

    return actual_total


def validate_common_contract(df: DataFrame) -> None:
    mandatory = [
        "event_id",
        "event_type",
        "event_date",
        "event_date_key",
        "source_fact",
        "source_key",
    ]

    invalid_common = reduce(
        lambda left, right: left | right,
        [F.col(column).isNull() for column in mandatory],
    )

    fail_if_rows(
        df.filter(invalid_common),
        "OBT contains a row with a NULL common mandatory field.",
    )

    fail_if_rows(
        df.filter(~F.col("event_type").isin(*sorted(EVENT_TYPES))),
        "OBT contains an event_type outside the allowed domain.",
    )


def validate_type_contracts(df: DataFrame) -> None:
    fail_if_rows(
        df.filter(
            (F.col("event_type") == "PLAYER_MATCH")
            & (
                F.col("game_id").isNull()
                | F.col("player_id").isNull()
            )
        ),
        "PLAYER_MATCH violates its Gold contract.",
    )

    fail_if_rows(
        df.filter(
            (F.col("event_type") == "MATCH_ACTION")
            & (
                F.col("game_id").isNull()
                | F.col("event_subtype").isNull()
                | F.col("minute").isNull()
            )
        ),
        "MATCH_ACTION violates its Gold contract.",
    )

    fail_if_rows(
        df.filter(
            (F.col("event_type") == "TRANSFER")
            & (
                F.col("player_id").isNull()
                | F.col("transfer_season").isNull()
                | (
                    F.col("from_club_id").isNull()
                    & F.col("to_club_id").isNull()
                )
            )
        ),
        "TRANSFER violates its Gold contract.",
    )

    fail_if_rows(
        df.filter(
            (F.col("event_type") == "VALUATION")
            & (
                F.col("player_id").isNull()
                | F.col("market_value_in_eur").isNull()
            )
        ),
        "VALUATION violates its Gold contract.",
    )


def validate_event_id_uniqueness(df: DataFrame) -> None:
    duplicates = (
        df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
    )

    fail_if_rows(
        duplicates,
        "Duplicate event_id detected in OBT.",
    )


# ---------------------------------------------------------------------------
# Cardinality-safe dimensional joins
# ---------------------------------------------------------------------------

def join_preserving_count(
    df: DataFrame,
    dimension: DataFrame,
    on,
    how: str,
    expected_count: int,
    label: str,
) -> DataFrame:

    result = (
        df.join(
            dimension,
            on=on,
            how=how,
        )
        .persist(StorageLevel.DISK_ONLY)
    )

    actual_count = result.count()

    if actual_count != expected_count:
        result.unpersist()

        raise RuntimeError(
            f"Join '{label}' changed cardinality: "
            f"expected {expected_count}, got {actual_count}"
        )

    print(
        f"[JOIN OK] {label}: "
        f"{expected_count:,} -> {actual_count:,}"
    )

    df.unpersist(blocking=False)

    return result


# ---------------------------------------------------------------------------
# Dimensional enrichment
# ---------------------------------------------------------------------------

def enrich_dimensions(
    events: DataFrame,
    dim_date: DataFrame,
    dim_player: DataFrame,
    dim_club: DataFrame,
    dim_competition: DataFrame,
    expected_count: int,
) -> DataFrame:

    # -----------------------------------------------------------------------
    # Mandatory date coverage
    # -----------------------------------------------------------------------

    date_keys = dim_date.select("date_key")

    missing_dates = (
        events.select("event_date_key")
        .join(
            date_keys,
            events["event_date_key"] == date_keys["date_key"],
            "left_anti",
        )
    )

    fail_if_rows(
        missing_dates,
        "At least one OBT event_date_key has no corresponding DIM_DATE row.",
    )

    date_context = dim_date.select(
        F.col("date_key").alias("event_date_key"),
        F.col("year").alias("event_year"),
        F.col("quarter").alias("event_quarter"),
        F.col("month").alias("event_month"),
        F.col("month_name").alias("event_month_name"),
        F.col("day").alias("event_day"),
        F.col("day_of_week").alias("event_day_of_week"),
        F.col("day_name").alias("event_day_name"),
        F.col("week_of_year").alias("event_week_of_year"),
    )

    result = join_preserving_count(
        events,
        date_context,
        on="event_date_key",
        how="inner",
        expected_count=expected_count,
        label="dim_date",
    )

    # -----------------------------------------------------------------------
    # Main player
    # -----------------------------------------------------------------------

    player_context = dim_player.select(
        "player_id",
        "player_code",
        F.col("name").alias("player_name"),
        F.col("first_name").alias("player_first_name"),
        F.col("last_name").alias("player_last_name"),
        F.col("date_of_birth").alias("player_date_of_birth"),
        F.col("country_of_birth").alias("player_country_of_birth"),
        F.col("city_of_birth").alias("player_city_of_birth"),
        F.col("country_of_citizenship").alias(
            "player_country_of_citizenship"
        ),
        F.col("position").alias("player_position"),
        F.col("sub_position").alias("player_sub_position"),
        F.col("foot").alias("player_foot"),
        F.col("height_in_cm").alias("player_height_in_cm"),
        "contract_expiration_date",
    )

    result = join_preserving_count(
        result,
        player_context,
        on="player_id",
        how="left",
        expected_count=expected_count,
        label="dim_player",
    )

    # -----------------------------------------------------------------------
    # Assist player
    # -----------------------------------------------------------------------

    assist_player = dim_player.select(
        F.col("player_id").alias("player_assist_id"),
        F.col("name").alias("player_assist_name"),
    )

    result = join_preserving_count(
        result,
        assist_player,
        on="player_assist_id",
        how="left",
        expected_count=expected_count,
        label="dim_player as assist_player",
    )

    # -----------------------------------------------------------------------
    # Incoming player
    # -----------------------------------------------------------------------

    incoming_player = dim_player.select(
        F.col("player_id").alias("player_in_id"),
        F.col("name").alias("player_in_name"),
    )

    result = join_preserving_count(
        result,
        incoming_player,
        on="player_in_id",
        how="left",
        expected_count=expected_count,
        label="dim_player as player_in",
    )

    # -----------------------------------------------------------------------
    # Main club
    # -----------------------------------------------------------------------

    club = dim_club.select(
        "club_id",
        F.col("name").alias("club_name"),
    )

    result = join_preserving_count(
        result,
        club,
        on="club_id",
        how="left",
        expected_count=expected_count,
        label="dim_club as club",
    )

    # -----------------------------------------------------------------------
    # Opponent club
    # -----------------------------------------------------------------------

    opponent = dim_club.select(
        F.col("club_id").alias("opponent_club_id"),
        F.col("name").alias("opponent_club_name"),
    )

    result = join_preserving_count(
        result,
        opponent,
        on="opponent_club_id",
        how="left",
        expected_count=expected_count,
        label="dim_club as opponent_club",
    )

    # -----------------------------------------------------------------------
    # From club
    # -----------------------------------------------------------------------

    from_club = dim_club.select(
        F.col("club_id").alias("from_club_id"),
        F.col("name").alias("from_club_name"),
    )

    result = join_preserving_count(
        result,
        from_club,
        on="from_club_id",
        how="left",
        expected_count=expected_count,
        label="dim_club as from_club",
    )

    # -----------------------------------------------------------------------
    # To club
    # -----------------------------------------------------------------------

    to_club = dim_club.select(
        F.col("club_id").alias("to_club_id"),
        F.col("name").alias("to_club_name"),
    )

    result = join_preserving_count(
        result,
        to_club,
        on="to_club_id",
        how="left",
        expected_count=expected_count,
        label="dim_club as to_club",
    )

    # -----------------------------------------------------------------------
    # Competition
    # -----------------------------------------------------------------------

    competition = dim_competition.select(
        "competition_id",
        F.col("name").alias("competition_name"),
        F.col("type").alias("competition_type"),
        F.col("sub_type").alias("competition_sub_type"),
        F.col("country_id").alias("competition_country_id"),
        F.col("country_name").alias("competition_country_name"),
        F.col("confederation").alias("competition_confederation"),
    )

    result = join_preserving_count(
        result,
        competition,
        on="competition_id",
        how="left",
        expected_count=expected_count,
        label="dim_competition",
    )

    return result


# ---------------------------------------------------------------------------
# Final validation
# ---------------------------------------------------------------------------

def validate_final_obt(
    df: DataFrame,
    expected_counts: Dict[str, int],
) -> None:

    validate_common_contract(df)
    validate_type_contracts(df)
    validate_event_id_uniqueness(df)

    actual_counts = event_counts(df)

    for event_type in sorted(EVENT_TYPES):
        expected = expected_counts[event_type]
        actual = actual_counts.get(event_type, 0)

        if actual != expected:
            raise RuntimeError(
                f"Final count mismatch for {event_type}: "
                f"expected {expected}, got {actual}"
            )

    expected_total = sum(expected_counts.values())
    actual_total = df.count()

    if actual_total != expected_total:
        raise RuntimeError(
            f"Final OBT count mismatch: "
            f"expected {expected_total}, got {actual_total}"
        )

    print("[VALIDATION OK] event_type domain")
    print("[VALIDATION OK] mandatory common fields")
    print("[VALIDATION OK] per-type contracts")
    print("[VALIDATION OK] event_id uniqueness")
    print("[VALIDATION OK] per-type cardinalities")
    print_count("Final OBT rows", actual_total)


# ---------------------------------------------------------------------------
# Write to Snowflake
# ---------------------------------------------------------------------------

def write_obt(
    df: DataFrame,
    sf_options: Dict[str, str],
) -> None:

    print(
        "[WRITE] Overwriting "
        f"{sf_options['sfDatabase']}."
        f"{sf_options['sfSchema']}."
        f"{OUTPUT_TABLE}"
    )

    (
        df.write
        .format(SNOWFLAKE_FORMAT)
        .options(**sf_options)
        .option("dbtable", OUTPUT_TABLE)
        .mode("overwrite")
        .save()
    )

    print("[WRITE OK] OBT written successfully.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("build-obt-football-events")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[SPARK] version: {spark.version}")
    print(f"[SPARK] master: {spark.sparkContext.master}")
    print(
        f"[SPARK] default parallelism: "
        f"{spark.sparkContext.defaultParallelism}"
    )

    sf_options = get_snowflake_options()

    try:
        # -------------------------------------------------------------------
        # Read Gold facts
        # -------------------------------------------------------------------

        fact_player_match = load_gold_table(
            spark,
            sf_options,
            "FACT_PLAYER_MATCH",
        )

        fact_match_event = load_gold_table(
            spark,
            sf_options,
            "FACT_MATCH_EVENT",
        )

        fact_transfer = load_gold_table(
            spark,
            sf_options,
            "FACT_TRANSFER",
        )

        fact_player_valuation = load_gold_table(
            spark,
            sf_options,
            "FACT_PLAYER_VALUATION",
        )

        print(
            "[SPARK] FACT_PLAYER_MATCH partitions:",
            fact_player_match.rdd.getNumPartitions(),
        )
        print(
            "[SPARK] FACT_MATCH_EVENT partitions:",
            fact_match_event.rdd.getNumPartitions(),
        )
        print(
            "[SPARK] FACT_TRANSFER partitions:",
            fact_transfer.rdd.getNumPartitions(),
        )
        print(
            "[SPARK] FACT_PLAYER_VALUATION partitions:",
            fact_player_valuation.rdd.getNumPartitions(),
        )
        # -------------------------------------------------------------------
        # Read Gold dimensions
        # -------------------------------------------------------------------

        dim_player = load_gold_table(
            spark,
            sf_options,
            "DIM_PLAYER",
        )

        dim_club = load_gold_table(
            spark,
            sf_options,
            "DIM_CLUB",
        )

        dim_competition = load_gold_table(
            spark,
            sf_options,
            "DIM_COMPETITION",
        )

        dim_date = load_gold_table(
            spark,
            sf_options,
            "DIM_DATE",
        )

        # -------------------------------------------------------------------
        # Source cardinalities
        # -------------------------------------------------------------------

        expected_counts = {
            "PLAYER_MATCH": fact_player_match.count(),
            "MATCH_ACTION": fact_match_event.count(),
            "TRANSFER": fact_transfer.count(),
            "VALUATION": fact_player_valuation.count(),
        }

        for event_type, count in expected_counts.items():
            print_count(f"Gold {event_type}", count)

        # -------------------------------------------------------------------
        # Project facts into canonical event schema
        # -------------------------------------------------------------------

        player_match = build_player_match(fact_player_match)
        match_action = build_match_action(fact_match_event)
        transfer = build_transfer(fact_transfer)
        valuation = build_valuation(fact_player_valuation)

        # No join between facts.
        #
        # We perform only a vertical union because each fact represents
        # a different grain.
        events = (
            player_match
            .unionByName(match_action)
            .unionByName(transfer)
            .unionByName(valuation)
            .persist(StorageLevel.DISK_ONLY)
        )

        expected_total = validate_projection_counts(
            events,
            expected_counts,
        )

        validate_common_contract(events)
        validate_type_contracts(events)
        validate_event_id_uniqueness(events)

        # -------------------------------------------------------------------
        # Dimensional enrichment
        # -------------------------------------------------------------------

        enriched = enrich_dimensions(
            events=events,
            dim_date=dim_date,
            dim_player=dim_player,
            dim_club=dim_club,
            dim_competition=dim_competition,
            expected_count=expected_total,
        )

        # -------------------------------------------------------------------
        # Final canonical output
        # -------------------------------------------------------------------

        final_obt = (
            enriched
            .select(*FINAL_COLUMNS)
            .persist(StorageLevel.DISK_ONLY)
        )

        final_obt.count()

        enriched.unpersist(blocking=False)

        # -------------------------------------------------------------------
        # Final validation
        # -------------------------------------------------------------------

        validate_final_obt(
            final_obt,
            expected_counts,
        )

        # -------------------------------------------------------------------
        # Persist only after all validations pass
        # -------------------------------------------------------------------

        write_obt(
            final_obt,
            sf_options,
        )

        print("[SUCCESS] GOLD.OBT_FOOTBALL_EVENTS completed.")

        final_obt.unpersist(blocking=False)

    except Exception as exc:
        print(
            f"[FAIL] OBT construction failed: {exc}",
            file=sys.stderr,
        )
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()