from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
ASSETS = {
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
}


def test_every_asset_has_staging_classification_and_base_models():
    staging = {
        path.stem.removeprefix("stg_tm__")
        for path in (ROOT / "dbt/models/staging/transfermarkt").glob("stg_tm__*.sql")
    }
    classified = {
        path.stem.removeprefix("int_tm__").removesuffix("_classified")
        for path in (ROOT / "dbt/models/silver/intermediate").glob(
            "int_tm__*_classified.sql"
        )
    }
    base = {
        path.stem.removeprefix("base_tm__")
        for path in (ROOT / "dbt/models/silver/base/transfermarkt").glob(
            "base_tm__*.sql"
        )
    }
    assert staging == classified == base == ASSETS


def test_staging_reads_raw_sources_and_later_models_use_refs():
    for path in (ROOT / "dbt/models/staging/transfermarkt").glob("stg_tm__*.sql"):
        sql = path.read_text(encoding="utf-8")
        assert "tm_selected_bronze_rows" in sql
        assert "_latest" not in sql.lower()
    for path in (ROOT / "dbt/models/silver").rglob("*.sql"):
        sql = path.read_text(encoding="utf-8")
        if path.name == "base_tm__source_manifest.sql":
            assert "source('transfermarkt_bronze'" in sql
        else:
            assert "source('transfermarkt_bronze'" not in sql


def test_game_event_unknown_minute_sentinel_becomes_null():
    staging_sql = (
        ROOT / "dbt/models/staging/transfermarkt/stg_tm__game_events.sql"
    ).read_text(encoding="utf-8")
    classified_sql = (
        ROOT / "dbt/models/silver/intermediate/int_tm__game_events_classified.sql"
    ).read_text(encoding="utf-8")

    assert "nullif({{ tm_integer('raw_record:minute') }}, -1)" in staging_sql
    assert "trim(raw_record:minute::varchar) <> '-1'" in classified_sql
    assert "'negative_event_minute'" in classified_sql


def test_manifest_requires_all_assets_when_fixed():
    sql = (ROOT / "dbt/models/silver/control/base_tm__source_manifest.sql").read_text(
        encoding="utf-8"
    )
    for asset in ASSETS:
        assert f"'{asset}'" in sql
    assert "missing asset" in sql
    assert "Unknown asset" in sql
    assert "status = 'SUCCESS'" in sql


def test_quarantine_and_reconciliation_cover_every_asset_once():
    for relative in (
        "dbt/models/silver/quality/base_tm__quarantine.sql",
        "dbt/models/silver/quality/base_tm__reconciliation.sql",
    ):
        sql = (ROOT / relative).read_text(encoding="utf-8")
        for asset in ASSETS:
            assert sql.count(f"'{asset}':") == 1


def test_observed_alias_seeds_are_documented_and_unique():
    for filename in (
        "tm_position_aliases.csv",
        "tm_event_type_aliases.csv",
        "tm_lineup_type_aliases.csv",
    ):
        rows = (ROOT / "dbt/seeds/transfermarkt" / filename).read_text(
            encoding="utf-8"
        ).splitlines()
        source_values = [row.split(",", 1)[0] for row in rows[1:]]
        assert source_values
        assert len(source_values) == len(set(source_values))


def test_stage_two_tag_covers_models_seeds_and_tests():
    project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text(encoding="utf-8"))
    assert "silver_stage_2" in project["models"]["elt_soccer_value"]["staging"]["+tags"]
    assert "silver_stage_2" in project["models"]["elt_soccer_value"]["silver"]["+tags"]
    assert "silver_stage_2" in project["seeds"]["elt_soccer_value"]["transfermarkt"]["+tags"]
    assert "silver_stage_2" in project["data_tests"]["elt_soccer_value"]["+tags"]
