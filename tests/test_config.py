from __future__ import annotations

import pytest

from ingestion.config import AssetCatalog, identifier


def test_catalog_contains_all_required_assets():
    catalog = AssetCatalog.load("config/assets.yml")
    assert set(catalog.assets) == {
        "competitions", "clubs", "players", "games", "appearances",
        "player_valuations", "transfers", "club_games", "game_events",
        "game_lineups", "countries", "national_teams",
    }


def test_identifier_rejects_sql_injection():
    assert identifier("bronze") == '"BRONZE"'
    with pytest.raises(ValueError):
        identifier("bronze; drop schema public")

