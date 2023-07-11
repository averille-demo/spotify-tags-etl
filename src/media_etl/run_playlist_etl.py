"""SQLModel driver to create backend tables from ORMs.

https://sqlmodel.tiangolo.com/db-to-code/
https://github.com/tiangolo/fastapi/tree/master/docs_src/sql_databases/sql_app_py310
"""
import json
import time
from pathlib import Path

import pendulum
from spotify_client import SpotifyClient
from sql.models import SpotifyAudioFeatureModel, SpotifyFavoriteModel, init_database
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import NoReferencedColumnError, OperationalError, ProgrammingError
from sqlmodel import Session, SQLModel
from util.logger import init_logger

from media_etl.util.settings import DATA_PATH

MODULE = Path(__file__).resolve().name


def load_json_to_postgres(
    path: Path,
    base: SQLModel,
):
    """Load data from newline delimited JSON text file to Postgres.

    The benefit of this approach is database loads can be performed 'offline' (without hitting Spotify APIs).
    https://sqlmodel.tiangolo.com/tutorial/insert/

    Args:
        path (Path): source data file containing newline delimited JSON (each row represents one model instance)
        base (SQLModel): model loaded to backend
    """
    if path.is_file():
        print(f"loading: {path.name=}")
        # read newline delimited JSON text data
        newline_delimited = path.read_text(encoding="utf-8")
        raw_json_data = newline_delimited.split("\n")
        # convert string to dict (ignore empty lines)
        records = [json.loads(r) for r in raw_json_data if r]
        # create new session for each group of operations with database that belong together
        with Session(engine) as session:
            for i, record in enumerate(records, start=1):
                try:
                    # convert all JSON kwargs (key/value pairs) to SQLModel object
                    model: SQLModel = base(**record)
                    # update timestamp for data loaded
                    model.load_date = pendulum.now(tz="UTC")
                    # write single data row to database
                    session.add(model)
                    session.commit()
                    print(f"  {record['type']}_{i:02d} ▶ {model.to_string()}")
                except (ProgrammingError, NoReferencedColumnError):
                    log.exception(f"{record=}")
            session.close()


def load_object_relational_models():
    """Driver to generate database record(s) from source JSON."""
    print(f"{MODULE} started: {pendulum.now(tz='America/Los_Angeles').to_atom_string()}")
    start = time.perf_counter()
    try:
        conn = engine.connect()
        print(f"{conn=} {conn.get_isolation_level()}")
        if isinstance(conn, Connection):
            load_json_to_postgres(
                path=Path(DATA_PATH, "liked_song_records.json"),
                base=SpotifyFavoriteModel,
            )
            load_json_to_postgres(
                path=Path(DATA_PATH, "audio_feature_records.json"),
                base=SpotifyAudioFeatureModel,
            )
            conn.close()
    except OperationalError:
        log.exception("postgres")
    print(f"{MODULE} finished ({time.perf_counter() - start:0.2f} seconds)")


def trigger_etl():
    """Driver to generate database record(s) from source JSON."""
    print(f"{MODULE} started: {pendulum.now(tz='America/Los_Angeles').to_datetime_string()}")
    start = time.perf_counter()

    client = SpotifyClient()
    # extract latest values from 'Liked Songs' playlist, save as JSON
    track_ids = client.extract_favorite_tracks()
    # for each track, query spotify metrics, save as JSON
    client.query_audio_features(track_ids)

    load_object_relational_models()
    engine.dispose()
    print(f"{MODULE} finished ({time.perf_counter() - start:0.2f} seconds)")


if __name__ == "__main__":
    log = init_logger(__file__)
    engine: Engine = init_database()
    trigger_etl()
