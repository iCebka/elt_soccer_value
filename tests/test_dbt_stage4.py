from pathlib import Path


ROOT = Path(__file__).parents[1]
MODEL = ROOT / "dbt/models/silver/final/silver_games_enriched.sql"


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_games_enriched_uses_clean_refs_and_role_specific_left_joins():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for model in (
        "base_tm__games",
        "base_tm__competitions",
        "base_tm__clubs",
        "base_tm__source_manifest",
    ):
        assert f"ref('{model}')" in sql
    assert "source(" not in sql
    assert sql.count("left join") == 6
    assert "games.competition_id = competitions.competition_id" in sql
    assert "games.home_club_id = home_club.club_id" in sql
    assert "games.away_club_id = away_club.club_id" in sql


def test_home_away_and_temporal_semantics_are_explicit():
    sql = MODEL.read_text(encoding="utf-8").lower()
    required = (
        "home_club_name_at_game",
        "away_club_name_at_game",
        "home_manager_name_at_game",
        "away_manager_name_at_game",
        "home_formation_at_game",
        "away_formation_at_game",
        "home_club_name_current_snapshot",
        "away_club_name_current_snapshot",
        "competition_type_at_game",
        "competition_type_snapshot",
    )
    for column in required:
        assert column in sql
    for forbidden in (
        "home_club.coach_name",
        "away_club.coach_name",
        "home_club.squad_size",
        "away_club.squad_size",
        "home_club.total_market_value_eur",
        "away_club.total_market_value_eur",
    ):
        assert forbidden not in sql


def test_resolution_and_source_lineage_are_separate():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "competition_resolution_status",
        "home_club_resolution_status",
        "away_club_resolution_status",
        "games_source_version_checksum",
        "competitions_source_version_checksum",
        "clubs_source_version_checksum",
        "games_source_captured_at",
        "competitions_source_captured_at",
        "clubs_source_captured_at",
        "silver_processed_at",
    ):
        assert column in sql


def test_stage_four_contract_tests_exist():
    expected_tests = {
        "assert_silver_games_inputs_unique.sql",
        "assert_silver_games_preserves_games.sql",
        "assert_silver_games_resolution_consistent.sql",
        "assert_silver_games_valid_game_fields.sql",
        "fixture_silver_games_enriched.sql",
    }
    assert expected_tests <= {
        path.name for path in (ROOT / "dbt/tests").glob("*.sql")
    }
    for filename in expected_tests:
        assert "silver_stage_4" in read(f"dbt/tests/{filename}")


def test_player_match_consumer_contract_is_documented():
    docs = read("docs/silver_games_enriched.md").lower()
    normalized_docs = " ".join(docs.split())
    assert "appearances.game_id" in docs
    assert "silver_games_enriched.game_id" in docs
    assert "exactamente una fila por `game_id`" in normalized_docs
