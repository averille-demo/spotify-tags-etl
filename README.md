# *Music Library* ETL - Relational

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-312/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-blue?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Linting: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
## Project Rationale
Most of us have **local** music files (mp3, m4a, flac, wma, etc.) accumulated over the years.

Another 'media-parser' project was written to extract tags and metadata from local files and save results as standardized dataframes exported to JSON [here](./data/local_media_extract.json).

The purpose of **_this_** project is to use several data management tools to leverage APIs into usable *persistent* formats:
1) **Query tag data** from each track against **[Spotify Web API](https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks)** to determine which [artist, album, track] ID belongs to each song.
   * given library managers (MediaMonkey, Rhythmbox, MusicBrainz, Mp3tag, iTunes, foobar2000, etc.) have different interpretations of tag data
   * process involves **[fuzzy pattern matching](https://github.com/maxbachmann/rapidfuzz)**

2) **Load *newline delimited* JSON** (source data generated by 'media-parser') to Postgres backend running in Docker orchestrated by this [ETL pipeline](./src/spotify_tags_etl/run_pipeline.py).

3) **Query Spotify 'Liked Songs' playlist** from this [code](./src/spotify_tags_etl/run_playlist_etl.py)
   * save API responses as custom formatted SQLModels
   * export [SQLModels](./src/spotify_tags_etl/sql/models.py) as newline delimited JSON files (custom formatted rows)
   * load results to two new 'liked_song' and 'audio_feature' tables in Postgres

**updated: 2024-01-31** to include pydantic v2 models

## Data source (offline)
![image](./img/local_media_extract.png)

## Data sink
![image](./img/datagrip_tables.png)


### Step 1: Setup credentials
[Spotify OAuth2](https://developer.spotify.com/documentation/general/guides/authorization/)
```
# update *_secret.toml file to include your Spotify API credentials
cp './config/settings_example.toml' './config/settings_secret.toml'

within './config/*_secret.toml':
[spotify.dev]
    client_id = "123456789abcdefg"        # <-- enter your CLIENT_ID
    client_secret = "abcdefg123456789"    # <-- enter your CLIENT_SECRET

# update: './docker/config/*_secret.env':
cp './docker/config/postgres_example_dev.env' './docker/config/postgres_private_dev.env'

POSTGRES_USER      # <-- enter your username
POSTGRES_PASSWORD  # <-- enter your password (no special chars)
PGADMIN_DEFAULT_EMAIL
PGADMIN_DEFAULT_PASSWORD
```

### Step 2: Setup Python Virtual Environment
[Poetry Commands](https://python-poetry.org/docs/cli/)
```
# check poetry
poetry --version

# use latest python version for venv
pyenv install --list | grep " 3.12"
pyenv install 3.12.0
pyenv local 3.12.0

# optional: update poetry settings
poetry config virtualenvs.in-project true
poetry config virtualenvs.prefer-active-python true
poetry config --list

# check pyproject.toml
poetry check
poetry lock

# upgrade pip within venv
poetry run python -m pip install --upgrade pip

# create virtual environment
poetry install -vvv | tee ./logs/poetry_install.log

# optional: update git settings
git config --global --add safe.directory "$(pwd)"
git config --list

# optional: setup pre-commit
poetry run pre-commit autoupdate
poetry run pre-commit install --install-hooks
```

### Step 3: Setup Docker container
[Install Docker Desktop](https://www.docker.com/products/docker-desktop)

[Docker Compose Commands](https://docs.docker.com/engine/reference/commandline/compose/)
```
# start backend with convenience script:
./docker/rebuild_container.sh
```
![image](./img/docker_desktop.png)

### Step 4: Trigger Media Tag ETL
```
# load local JSON file to postgres
poetry run python ./src/spotify_tags_etl/run_pipeline.py

# optional: query spotify for matching album, artist, track IDs:
poetry run python ./src/spotify_tags_etl/run_pipeline.py --query-spotify
```

### Step 5: Run playlist ETL script
```
# to only load JSON files (prior query to postgres using SQLModel ORM library:
poetry run python ./src/spotify_tags_etl/run_playlist_etl.py

# optional: query Spotify 'Liked Songs' and audio feature extraction, and load to postgres:
poetry run python ./src/spotify_tags_etl/run_playlist_etl.py --query-spotify
```

![image](./img/tqdm_status.png)


## Cleanup and maintenance
```
cd ./docker
# shut down container
docker-compose -f ./docker-compose-dev.yaml --env-file ./config/postgres_secret_dev.env down --remove-orphans

# optional: update poetry
poetry self lock
poetry self install --sync
poetry self update

# update dependencies in venv
poetry update -vvv | tee ./logs/poetry_update.log
```

## Observations
While leveraging the Spotify APIs, I noticed a few quirks:
* Search queries with [&,.] symbols returned incorrect 'id' matches:
  * example:
    * 'Sallie Ford & The Sound Outside' -> '6S7GGa2a501L3vN8h7yGhv' = INVALID
    * 'Sallie Ford The Sound Outside' -> '0Z8RhQLJrLxKMWoUW2qo95' = **CORRECT**
* Classical music queries are poorly supported in [Search for Item API](https://developer.spotify.com/documentation/web-api/reference/search):
  * missing relevant search parameters: 'orchestra', 'date performed', 'conductor', etc.
* Search queries with Unicode characters also returned incorrect 'id' matches:
  * example:
    * 'Björk' -> '0L2E40bnomT7iQiZvKYC0B' = INVALID
    * 'Bjork' -> '6xy8s41CbAZbN6skLwoPYn' = **CORRECT**
* While performing pagination, the "total" number of items available to return was also invalid:
  * I ended up checking if items list had elements vs. continuing to query endpoint for empty items (last n-pages)
* It would be better for API endpoints to support subqueries (GraphQL: only return specific fields in each response)
  * Goal: ignore unnecessary portion of API response payload (images, available_markets, etc.)


## Resources
* [Spotipy](https://spotipy.readthedocs.io)
* [RapidFuzz](https://github.com/maxbachmann/rapidfuzz)
* [pgadmin4](https://www.pgadmin.org/docs/pgadmin4/latest/container_deployment.html)
* [Musicstax](https://musicstax.com/search)
* [SQLModel](https://sqlmodel.tiangolo.com/)
