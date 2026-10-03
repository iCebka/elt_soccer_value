from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]


def test_dbt_profile_uses_password_auth_without_private_key_fields():
    profile = yaml.safe_load((ROOT / "dbt/profiles.yml").read_text(encoding="utf-8"))
    outputs = profile["elt_soccer_value"]["outputs"]

    assert set(outputs) == {"dev", "prod", "test"}
    for output in outputs.values():
        assert output["type"] == "snowflake"
        assert output["authenticator"] == "snowflake"
        assert "SNOWFLAKE_PASSWORD" in output["password"]
        assert not any("private" in key.lower() for key in output)


def test_dbt_materializations_and_source_inventory_are_explicit():
    project = yaml.safe_load((ROOT / "dbt/dbt_project.yml").read_text(encoding="utf-8"))
    model_config = project["models"]["elt_soccer_value"]
    assert model_config["staging"]["+materialized"] == "view"
    assert model_config["silver"]["+materialized"] == "table"

    sources = yaml.safe_load(
        (ROOT / "dbt/models/staging/transfermarkt/_sources.yml").read_text(encoding="utf-8")
    )["sources"][0]
    source_names = {table["name"] for table in sources["tables"]}
    assert source_names == {
        "competitions_raw",
        "clubs_raw",
        "players_raw",
        "games_raw",
        "appearances_raw",
        "player_valuations_raw",
        "transfers_raw",
        "club_games_raw",
        "game_events_raw",
        "game_lineups_raw",
        "countries_raw",
        "national_teams_raw",
        "transfermarkt_ingestion_batches",
        "transfermarkt_ingestion_files",
    }


def test_compose_passes_only_password_auth_and_silver_settings_to_dbt():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    dbt = compose["services"]["dbt"]
    environment = dbt["environment"]

    assert dbt["profiles"] == ["silver"]
    assert environment["SNOWFLAKE_AUTH_METHOD"].endswith(":-password}")
    assert {
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_BRONZE_SCHEMA",
        "SNOWFLAKE_STAGING_SCHEMA",
        "SNOWFLAKE_SILVER_SCHEMA",
        "DBT_TARGET",
        "DBT_DEV_SCHEMA",
        "DBT_TEST_SCHEMA",
    }.issubset(environment)
    assert not any("private" in key.lower() or "passphrase" in key.lower() for key in environment)


def test_dbt_versions_are_directly_pinned_and_transitively_locked():
    direct = (ROOT / "docker/dbt/requirements.in").read_text(encoding="utf-8")
    lock = (ROOT / "docker/dbt/requirements.lock").read_text(encoding="utf-8")

    assert "dbt-core==1.12.5" in direct
    assert "dbt-snowflake==1.12.1" in direct
    assert "dbt-core==1.12.5" in lock
    assert "dbt-snowflake==1.12.1" in lock
    assert "--hash=sha256:" in lock


def test_stage_two_models_replace_infrastructure_sentinels():
    assert not (ROOT / "dbt/models/staging/_staging_contract.sql").exists()
    assert not (ROOT / "dbt/models/silver/_silver_contract.sql").exists()
    assert len(list((ROOT / "dbt/models/staging/transfermarkt").glob("stg_tm__*.sql"))) == 12
    assert len(list((ROOT / "dbt/models/silver/base/transfermarkt").glob("base_tm__*.sql"))) == 12
