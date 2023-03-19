"""Parse pydantic settings for /config/*.toml and pyproject.toml.

https://docs.pydantic.dev/usage/settings/
"""
import locale
import platform
import tomllib
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from pydantic import AnyUrl, BaseSettings, conint, validator

PROJECT_PATH: Final[Path] = Path(__file__).resolve().parent.parent.parent
DEBUG: Final[bool] = False
VALID_DB_ENV = frozenset(("dev", "prod"))

DATA_PATH: Final[Path] = Path(PROJECT_PATH, "data", "input")
if not DATA_PATH.is_dir():
    raise FileNotFoundError(f"DATA folder does not exist: {DATA_PATH}")

SQL_PATH: Final[Path] = Path(PROJECT_PATH, "src", "sql")
if not SQL_PATH.is_dir():
    raise FileNotFoundError(f"SQL folder does not exist: {SQL_PATH}")

TOML_PATH: Final[Path] = Path(PROJECT_PATH, "config", "settings_secret.toml")
if not TOML_PATH.is_file():
    raise FileNotFoundError(f"TOML file does not exist: {TOML_PATH}")

PYPROJECT_PATH: Final[Path] = Path(PROJECT_PATH, "pyproject.toml")
if not PYPROJECT_PATH.is_file():
    raise FileNotFoundError(f"PYPROJECT file does not exist: {PYPROJECT_PATH}")


def open_toml(path: Path = TOML_PATH) -> Dict[str, Any]:
    """Open TOML file, return all key/value pairs (tomllib new to python 3.11)."""
    if path.is_file():
        with open(file=path, mode="rb") as fp:
            return tomllib.load(fp)
    else:
        raise FileNotFoundError(f"TOML file not found: {path}")


# pylint: disable=[missing-function-docstring,too-few-public-methods,missing-class-docstring]
class DatabaseConfig(BaseSettings):
    """Model for API credentials.

    https://docs.pydantic.dev/usage/settings/
    """

    name: str
    timezone: str
    environment: str
    endpoint: str
    username: str
    password: str
    database: str
    port: conint(gt=1024, lt=49151)  # type: ignore
    timeout: conint(gt=1, lt=10)  # type: ignore

    class Config:
        """Pydantic configuration subclass."""

        secrets_dir = TOML_PATH.parent.as_posix()
        case_sensitive = True

    @validator("environment")
    def check_environment(cls, env: str):
        """Validate toml setting."""
        if env not in VALID_DB_ENV:
            raise ValueError(f"environment '{env}' not in {VALID_DB_ENV}")
        return env


def load_db_config(
    backend: str = "postgres",
    environment: str = "dev",
) -> DatabaseConfig:
    """Convert TOML key/value pairs to pydantic BaseSettings object.

    Args:
        backend (str): type of database
        environment (str): environment string (from TOML)

    Returns:
        PostgresConfig: pydantic settings object
    """
    config = open_toml()
    return DatabaseConfig(
        name=config["project"]["name"],
        timezone=config["project"]["timezone"],
        environment=environment,
        endpoint=config[backend][environment]["endpoint"],
        username=config[backend][environment]["username"],
        password=config[backend][environment]["password"],
        database=config[backend][environment]["database"],
        port=config[backend][environment]["port"],
        timeout=config[backend][environment]["timeout"],
    )


# pylint: disable=[missing-function-docstring,too-few-public-methods,missing-class-docstring]
class SpotifyApiConfig(BaseSettings):
    """Model for API credentials.

    https://docs.pydantic.dev/usage/settings/
    """

    client_id: str
    client_secret: str
    redirect_uri: AnyUrl
    port: conint(gt=1024, lt=49151)  # type: ignore
    scope: str
    timeout: conint(gt=1, lt=10)  # type: ignore

    class Config:
        """Pydantic configuration subclass."""

        secrets_dir = TOML_PATH.parent.as_posix()
        case_sensitive = True


def load_spotify_config(
    environment: str = "dev",
) -> SpotifyApiConfig:
    """Convert TOML key/value pairs to pydantic BaseSettings object.

    Returns:
        SpotifyApiConfig: pydantic settings object
    """
    config = open_toml()
    return SpotifyApiConfig(
        client_id=config["spotify"][environment]["client_id"],
        client_secret=config["spotify"][environment]["client_secret"],
        redirect_uri=config["spotify"][environment]["redirect_uri"],
        port=config["spotify"][environment]["port"],
        scope=config["spotify"][environment]["scope"],
        timeout=config["spotify"][environment]["timeout"],
    )


class PyProjectToolPoetry(BaseSettings):
    """Pydantic settings for poetry information.

    Required: name, version, description, authors
    https://python-poetry.org/docs/pyproject/
    """

    host: str
    name: str
    version: str
    description: str
    license: Optional[str]
    authors: List[str]
    readme: Optional[str]
    repository: Optional[str]
    documentation: Optional[str]
    keywords: Optional[List[str]]

    class Config:
        """Pydantic configuration subclass."""

        case_sensitive = True


def parse_pyproject() -> PyProjectToolPoetry:
    """Extract tool.poetry section from pyproject.toml."""
    tool_poetry_sections: List[str] = [
        "name",
        "version",
        "description",
        "license",
        "authors",
        "repository",
        "documentation",
        "keywords",
    ]
    # init mapping to include all available sections (regardless if actually present in TOML)
    parsed_toml = dict.fromkeys(tool_poetry_sections, None)

    # read pyproject.toml
    pyproject = open_toml(path=PYPROJECT_PATH)

    # only include [tool.poetry] section
    pyproject = pyproject["tool"]["poetry"]
    # overwrite None with values present in TOML
    for key in parsed_toml.keys():
        # skip nested .dependencies or .group.dev
        if isinstance(key, str):
            parsed_toml[key] = pyproject.get(key, None)

    host_enc = locale.getpreferredencoding()
    host_arch = f"{platform.system()} {platform.architecture()[0]} {platform.machine()}"
    parsed_toml["host"] = f"{platform.node():6} ({host_enc} {host_arch:16})"

    # build pydantic BaseSettings object with **kwargs
    return PyProjectToolPoetry.parse_obj(parsed_toml)


if __name__ == "__main__":
    print(parse_pyproject())
    print(load_db_config())
    print(load_spotify_config())
