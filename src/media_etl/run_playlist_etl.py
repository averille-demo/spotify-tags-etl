"""SQLModel driver to create backend tables from ORMs.

https://sqlmodel.tiangolo.com/db-to-code/
https://github.com/tiangolo/fastapi/tree/master/docs_src/sql_databases/sql_app_py310
"""
import json
import time
from pathlib import Path

import pendulum
from sql.models import SpotifyAudioFeatureModel, SpotifyFavoriteModel, init_database
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import NoReferencedColumnError, OperationalError, ProgrammingError
from sqlmodel import Session, SQLModel
from util.logger import init_logger

from media_etl.util.settings import DATA_PATH, DatabaseConfig, load_db_config

MODULE = Path(__file__).resolve().name
config: DatabaseConfig = load_db_config()
log = init_logger(__file__)


def load_json_to_postgres(
    engine: Engine,
    path: Path,
    base: SQLModel,
):
    """Load data from JSON to Postgres.

    https://sqlmodel.tiangolo.com/tutorial/insert/
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
        engine = init_database()
        conn = engine.connect()
        print(f"{conn=} {conn.get_isolation_level()}")
        if isinstance(conn, Connection):
            load_json_to_postgres(
                engine=engine,
                path=Path(DATA_PATH, "liked_song_records.json"),
                base=SpotifyFavoriteModel,
            )
            load_json_to_postgres(
                engine=engine,
                path=Path(DATA_PATH, "audio_feature_records.json"),
                base=SpotifyAudioFeatureModel,
            )
            conn.close()
        engine.dispose()
    except OperationalError:
        log.exception("postgres")
    print(f"{MODULE} finished ({time.perf_counter() - start:0.2f} seconds)")


if __name__ == "__main__":
    load_object_relational_models()