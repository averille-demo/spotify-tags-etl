"""Driver to insert JSON music tags into PostgreSQL backend."""

import time
from pathlib import Path

import pendulum
from postgres_media import PostgresMedia
from sql import params_queries

MODULE = Path(__file__).resolve().name


def trigger_etl():
    """Driver to generate database record(s) from source JSON."""
    print(f"{MODULE} started: {pendulum.now(tz='America/Los_Angeles').to_atom_string()}")
    start = time.perf_counter()
    with PostgresMedia() as pgm:
        if pgm.connect():
            pgm.create_database()
            pgm.recreate_tables()
            pgm.load_data()
            # after processing data, perform various canned queries
            pgm.query(query=params_queries.ARTIST_SELECT, params=["Mazzy Star"])
            pgm.query(query=params_queries.ALBUM_SELECT, params=["Debut"])
            pgm.query(query=params_queries.TRACK_SELECT, params=["Future Proof"])
            pgm.query(query=params_queries.GENRE_SELECT, params=["Trip-Hop", "Alternative"])
            pgm.query(query=params_queries.FILE_SELECT, params=[".flac"])
            pgm.query(query=params_queries.GAIN_SELECT, params=["-4.0"])
            pgm.query(query=params_queries.JOIN_SELECT, params=["Classical"])
            pgm.query(query=params_queries.AVG_SIZE_SELECT, params=[])
    print(f"{MODULE} finished ({time.perf_counter() - start:0.2f} seconds)")


if __name__ == "__main__":
    trigger_etl()
