"""Tests for parsing database credentials as pydantic settings."""
import pytest

from src.util import settings


def test_load_postgres_settings():
    """Check if parsing TOML to pydantic settings was successful."""
    config = settings.load_db_config()
    assert isinstance(config, settings.DatabaseConfig)
    for attr in ["name", "timezone", "environment", "endpoint", "username", "password", "database", "port", "timeout"]:
        assert hasattr(config, attr)


def test_load_wrong_env_toml():
    """Check if parsing invalid TOML to pydantic settings raises exception."""
    with pytest.raises(KeyError) as ex:
        settings.load_db_config(environment="test")
        print(ex.message)
        assert ex
