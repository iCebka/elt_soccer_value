from pathlib import Path

import yaml


ROOT = Path(__file__).parents[1]
MODEL = ROOT / "dbt/models/silver/final/silver_players_current.sql"


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_players_current_uses_clean_refs_and_left_joins():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for model in (
        "base_tm__players",
        "base_tm__national_teams",
        "base_tm__countries",
        "base_tm__clubs",
        "int_tm__country_name_resolution",
    ):
        assert f"ref('{model}')" in sql
    assert "source(" not in sql
    assert sql.count("left join") >= 9
    assert (
        "players.current_national_team_id = national_teams.national_team_id" in sql
    )
    assert "national_teams.country_id = national_team_country.country_id" in sql
    assert "players.current_club_id = clubs.club_id" in sql


def test_country_roles_are_separate_and_originals_are_retained():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "country_of_birth_source",
        "birth_country_id",
        "birth_country_name",
        "birth_country_resolution_status",
        "country_of_citizenship_source",
        "citizenship_country_id",
        "citizenship_country_name",
        "citizenship_country_resolution_status",
        "national_team_country_id",
        "national_team_country_name",
        "national_team_country_resolution_status",
    ):
        assert column in sql


def test_country_resolution_has_no_fuzzy_matching():
    sql = read(
        "dbt/models/silver/intermediate/int_tm__country_name_resolution.sql"
    ).lower()
    assert "tm_country_name_key" in sql
    assert "matched_name" in sql
    assert "matched_alias" in sql
    assert "ambiguous" in sql
    for forbidden in ("soundex", "jaro", "levenshtein", "editdistance"):
        assert forbidden not in sql


def test_country_alias_seed_contains_only_profiled_variants():
    rows = read("dbt/seeds/transfermarkt/tm_country_name_aliases.csv").splitlines()
    assert rows[0] == "alias_name,canonical_country_name,rationale"
    aliases = {row.split(",", 1)[0] for row in rows[1:]}
    assert aliases == {"Turkey", "Macedonia"}


def test_current_context_and_source_lineage_are_explicit():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "current_market_value_eur",
        "current_contract_expiration_date",
        "cumulative_international_caps",
        "cumulative_international_goals",
        "current_national_team_fifa_ranking",
        "players_source_captured_at",
        "national_teams_source_captured_at",
        "countries_source_captured_at",
        "clubs_source_captured_at",
        "silver_processed_at",
    ):
        assert column in sql


def test_stage_three_tags_and_contract_tests_exist():
    project = yaml.safe_load(read("dbt/dbt_project.yml"))
    assert "silver_stage_3" in project["models"]["elt_soccer_value"]["silver"][
        "final"
    ]["+tags"]
    expected_tests = {
        "assert_country_normalized_names_unique.sql",
        "assert_country_aliases_resolve_once.sql",
        "assert_silver_players_preserves_players.sql",
        "assert_silver_players_resolution_consistent.sql",
        "assert_player_citizenship_is_scalar.sql",
        "fixture_silver_players_current.sql",
    }
    assert expected_tests <= {
        path.name for path in (ROOT / "dbt/tests").glob("*.sql")
    }
