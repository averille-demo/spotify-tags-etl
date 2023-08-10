"""Tests for parsing spotify credentials as pydantic settings."""
import pytest

from spotify_tags_etl.util import settings


def test_load_spotify_settings():
    """Check if parsing TOML to pydantic settings was successful."""
    config = settings.load_spotify_config()
    assert isinstance(config, settings.SpotifyApiConfig)
    for attr in [
        "client_id",
        "client_secret",
        "redirect_uri",
        "port",
        "scopes",
        "market",
        "api_timeout",
        "api_limit",
        "thold",
    ]:
        assert hasattr(config, attr)


def test_load_wrong_env_toml():
    """Check if parsing invalid TOML to pydantic settings raises exception."""
    with pytest.raises(KeyError) as ex:
        settings.load_db_config(environment="test")
        print(ex.message)
        assert ex
