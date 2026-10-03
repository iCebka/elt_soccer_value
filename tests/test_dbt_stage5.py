from pathlib import Path


ROOT = Path(__file__).parents[1]
MODEL = ROOT / "dbt/models/silver/final/silver_player_match.sql"


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_player_match_uses_clean_refs_and_left_joins():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for model in (
        "base_tm__appearances",
        "silver_games_enriched",
        "base_tm__club_games",
        "base_tm__source_manifest",
    ):
        assert f"ref('{model}')" in sql
    assert "source(" not in sql
    assert sql.count("left join") == 7
    assert "appearances.game_id = games.game_id" in sql
    assert "appearances.game_id = club_games.game_id" in sql
    assert "appearances.player_club_id = club_games.club_id" in sql


def test_historical_and_current_club_roles_are_separate():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "player_club_id",
        "player_current_club_id",
        "matched_player_club_id",
        "opponent_club_id",
        "player_team_hosting",
        "player_club_name_at_game",
        "opponent_club_name_at_game",
        "player_team_result",
    ):
        assert column in sql
    assert "coalesce(appearances.player_club_id" not in sql


def test_date_and_competition_precedence_is_auditable():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "appearance_date_source",
        "game_date_source",
        "match_date_source",
        "date_comparison_status",
        "appearance_competition_id",
        "game_competition_id",
        "competition_id_source",
        "competition_comparison_status",
    ):
        assert column in sql
    assert "coalesce(games.game_date, appearances.appearance_date)" in sql
    assert "coalesce(games.competition_id, appearances.competition_id)" in sql


def test_stats_are_not_capped_or_filled_and_valuations_are_excluded():
    sql = MODEL.read_text(encoding="utf-8").lower()
    for column in (
        "minutes_played",
        "goals",
        "assists",
        "yellow_cards",
        "red_cards",
    ):
        assert f"appearances.{column}" in sql
    assert "least(" not in sql
    assert "base_tm__player_valuations" not in sql
    assert "silver_player_valuations" not in sql


def test_stage_five_contract_tests_exist():
    expected = {
        "assert_silver_player_match_inputs_unique.sql",
        "assert_silver_player_match_preserves_appearances.sql",
        "assert_silver_player_match_context_consistent.sql",
        "assert_silver_player_match_comparisons_consistent.sql",
        "assert_silver_player_match_valid_stats.sql",
        "fixture_silver_player_match.sql",
    }
    assert expected <= {path.name for path in (ROOT / "dbt/tests").glob("*.sql")}
    for filename in expected:
        assert "silver_stage_5" in read(f"dbt/tests/{filename}")


def test_gold_boundary_and_incomplete_context_are_documented():
    docs = read("docs/silver_player_match.md").lower()
    normalized_docs = " ".join(docs.split())
    assert "player_current_club_id" in docs
    assert "sin games/club_games" in docs
    assert "por `player_id` solamente" in normalized_docs
    assert "gold" in docs
