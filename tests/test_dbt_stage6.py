from pathlib import Path


ROOT = Path(__file__).parents[1]
MODEL = ROOT / "dbt/models/silver/final/silver_player_valuations_enriched.sql"


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_valuations_use_only_clean_history_and_current_profile_refs():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for model in (
        "base_tm__player_valuations",
        "silver_players_current",
        "base_tm__source_manifest",
    ):
        assert f"ref('{model}')" in sql
    assert "source(" not in sql
    assert "valuations.player_id = players_current.player_id" in sql
    assert "ref('base_tm__national_teams')" not in sql
    assert "ref('base_tm__countries')" not in sql
    assert "silver_player_match" not in sql


def test_full_history_is_not_reduced_and_values_are_distinct():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for forbidden in ("row_number(", "qualify ", "max(valuation_date", "latest"):
        assert forbidden not in sql
    assert "historical_market_value_eur" in sql
    assert "player_current_market_value_eur_snapshot" in sql
    assert "valuation_source_current_club_id" in sql
    assert "current_club_id_comparison_status" in sql


def test_age_is_birthday_adjusted_and_nullable_when_invalid():
    sql = MODEL.read_text(encoding="utf-8").lower()
    assert "age_at_valuation_years" in sql
    assert "age_at_valuation_status" in sql
    assert "to_char(valuations.valuation_date, 'mmdd')" in sql
    assert "to_char(players_current.date_of_birth, 'mmdd')" in sql
    assert "valuations.valuation_date < players_current.date_of_birth" in sql


def test_snapshot_fields_and_lineage_are_explicit():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "player_current_club_id_snapshot",
        "player_current_national_team_id_snapshot",
        "citizenship_country_id_current_snapshot",
        "current_national_team_fifa_ranking_snapshot",
        "player_current_contract_expiration_date_snapshot",
        "player_highest_market_value_eur_snapshot",
        "player_cumulative_international_caps_snapshot",
        "valuations_source_version_checksum",
        "players_source_version_checksum",
        "national_teams_source_version_checksum",
        "countries_source_version_checksum",
        "clubs_source_version_checksum",
    ):
        assert column in sql


def test_stage_six_contract_tests_exist():
    expected = {
        "assert_silver_player_valuations_inputs_unique.sql",
        "assert_silver_player_valuations_preserves_history.sql",
        "assert_silver_player_valuations_age_consistent.sql",
        "assert_silver_player_valuations_context_consistent.sql",
        "assert_silver_player_valuations_valid_amounts.sql",
        "fixture_silver_player_valuations_enriched.sql",
    }
    assert expected <= {path.name for path in (ROOT / "dbt/tests").glob("*.sql")}
    for filename in expected:
        assert "silver_stage_6" in read(f"dbt/tests/{filename}")


def test_four_table_contract_and_gold_boundary_are_documented():
    docs = read("docs/silver_contract.md").lower()
    for model in (
        "silver_players_current",
        "silver_games_enriched",
        "silver_player_match",
        "silver_player_valuations_enriched",
    ):
        assert model in docs
    assert "(`player_id`, `valuation_date`)" in docs
    assert "joins as-of" in docs
    assert "gold" in docs
