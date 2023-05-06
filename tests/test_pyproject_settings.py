"""Tests for parsing pyproject settings."""

from pathlib import Path
from typing import Dict, List

import pytest

from media_etl.util import settings

VALID_TOML = settings.TOML_PATH
INVALID_TOML = Path(settings.TOML_PATH.parent, "does_not_exist.toml")
EMPTY_TOML = Path(settings.TOML_PATH.parent, "settings_example.toml")


def test_open_toml_valid_path():
    """Check if able to open valid TOML file."""
    result = settings.open_toml(VALID_TOML)
    assert isinstance(result, Dict)
    assert isinstance(result["project"]["name"], str)


def test_open_toml_invalid_path():
    """Check if opening invalid TOML file should raise exception."""
    with pytest.raises(FileNotFoundError) as ex:
        settings.open_toml(INVALID_TOML)
        print(ex.message)
        assert ex


def test_parse_pyproject_toml():
    """Check parsing pyproject TOML for required attributes."""
    result = settings.parse_pyproject()
    assert isinstance(result, settings.PyProjectToolPoetry)
    assert isinstance(result.name, str)
    assert isinstance(result.version, str)
    assert isinstance(result.description, str)
    assert isinstance(result.authors, List)
