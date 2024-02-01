"""Parse pydantic settings for /config/*.toml and pyproject.toml.

updated: 2024-01-31
https://docs.pydantic.dev/usage/settings/
"""

import platform
import tomllib
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from pydantic import confloat, conint, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENABLE_API: Final[bool] = False
DEBUG: Final[bool] = False

VALID_DB_ENV = frozenset(("dev", "prod"))

SRC_PATH: Final[Path] = Path(__file__).resolve().parent.parent
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent.parent.parent
REPO_NAME: Final[str] = PROJECT_ROOT.stem.replace(" ", "_").replace("-", "_")

DATA_PATH: Final[Path] = Path(PROJECT_ROOT, "data")
if not DATA_PATH.is_dir():
    raise NotADirectoryError(f"{DATA_PATH=}")

API_PATH: Final[Path] = Path(PROJECT_ROOT, "api")
if not API_PATH.is_dir():
    API_PATH.mkdir(parents=True, exist_ok=True)

SQL_PATH: Final[Path] = Path(SRC_PATH, "sql")
if not SQL_PATH.is_dir():
    raise NotADirectoryError(f"{SQL_PATH=}")

TOML_PATH: Final[Path] = Path(PROJECT_ROOT, "config", "settings_secret.toml")
if not TOML_PATH.is_file():
    print(f"ERROR: update credentials in {TOML_PATH.name}")
    TOML_PATH.write_text(Path(PROJECT_ROOT, "config", "settings_example.toml").read_text())

PYPROJECT_PATH: Final[Path] = Path(PROJECT_ROOT, "pyproject.toml")
if not PYPROJECT_PATH.is_file():
    raise FileNotFoundError(f"{PYPROJECT_PATH=}")


def open_toml(path: Path = TOML_PATH) -> Dict[str, Any]:
    """Open TOML file, return all key/value pairs (tomllib new to python 3.11)."""
    if path.is_file():
        with open(file=path, mode="rb") as fp:
            return tomllib.load(fp)
    else:
        raise FileNotFoundError(f"{path=}")


# pylint: disable=[missing-function-docstring,too-few-public-methods,missing-class-docstring]
class DatabaseConfig(BaseSettings):
    """Model for API credentials.

    https://docs.pydantic.dev/usage/settings/
    """

    model_config = SettingsConfigDict(case_sensitive=True, secrets_dir=TOML_PATH.parent.as_posix())

    name: str
    timezone: str
    environment: str
    endpoint: str
    username: str
    password: str
    database: str
    port: conint(gt=1024, lt=49151)  # type: ignore [valid-type]
    timeout: conint(gt=1, lt=10)  # type: ignore [valid-type]

    @field_validator("environment", mode="before")
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

    model_config = SettingsConfigDict(case_sensitive=True, secrets_dir=TOML_PATH.parent.as_posix())

    client_id: str
    client_secret: str
    redirect_uri: str
    port: conint(gt=1024, lt=49151)  # type: ignore [valid-type]
    scopes: str
    market: str = "US"
    api_timeout: confloat(gt=0.0, lt=5.0)  # type: ignore [valid-type]
    api_limit: conint(ge=1, le=50)  # type: ignore [valid-type]
    thold: confloat(gt=0.0, lt=100.0)  # type: ignore [valid-type]

    @field_validator("scopes", mode="before")
    def convert_list_to_comma_delimited_string(cls, scopes: List) -> str:
        """Create comma delimited string from list of values."""
        if isinstance(scopes, str):
            scopes = [scopes]
        if isinstance(scopes, List):
            return ",".join(scopes)
        else:
            raise ValueError(f"invalid input: {scopes=}")


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
        scopes=config["spotify"][environment]["scopes"],
        market=config["spotify"][environment]["market"],
        api_timeout=config["spotify"][environment]["api_timeout"],
        api_limit=config["spotify"][environment]["api_limit"],
        thold=config["spotify"][environment]["thold"],
    )


class PyProjectToolPoetry(BaseSettings):
    """Pydantic settings for poetry information.

    Required: name, version, description, authors
    https://python-poetry.org/docs/pyproject/
    """

    model_config = SettingsConfigDict(case_sensitive=True)

    host: str
    name: str
    version: str
    description: str
    authors: List[str]
    license: Optional[str]
    readme: Optional[str]
    repository: Optional[str]
    documentation: Optional[str]
    keywords: Optional[List[str]]


def parse_pyproject() -> PyProjectToolPoetry:
    """Extract tool.poetry section from pyproject.toml."""
    tool_poetry_sections: List[str] = [
        "name",
        "version",
        "description",
        "license",
        "authors",
        "readme",
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

    # add host platform information
    host_arch = f"{platform.system()} {platform.architecture()[0]} {platform.machine()}"
    parsed_toml["host"] = f"{platform.node()} ({host_arch})"

    # build pydantic BaseSettings object with **kwargs
    return PyProjectToolPoetry.parse_obj(parsed_toml)


if __name__ == "__main__":
    print(f"{parse_pyproject()=}")
    print(f"{load_db_config()=}")
    print(f"{load_spotify_config()=}")
