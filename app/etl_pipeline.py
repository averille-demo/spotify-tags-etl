"""Driver module to insert JSON media tag data into PostgreSQL"""
import time
from pathlib import Path
from typing import Dict
import argparse
from pathvalidate.argparse import validate_filepath_arg
from SpotifyClient import SpotifyClient
from PostgresMedia import PostgresMedia
import etl_queries as pgq
from dev_tools_lib import init_logger

MODULE = Path(__file__).resolve().name
CWD_PATH = Path(__file__).resolve().parent


def parse_cmd_args() -> Dict:
    """command line arguments to scan recursively for files"""
    parser = argparse.ArgumentParser()
    valid_db_env = ["admin", "development", "staging", "production"]
    parser.add_argument(
        "-f",
        "--file_path",
        type=validate_filepath_arg,
        default=CWD_PATH.parent.joinpath(*["data", "input"]),
        required=False,
        help="enter directory path to source data (*.json)",
    )
    parser.add_argument(
        "-db",
        "--db_env",
        type=str,
        default="development",
        required=False,
        help=f"enter valid database environment: {valid_db_env}",
    )
    # convert parser to dict with vars()
    args = vars(parser.parse_args())

    file_path = str(args["file_path"])
    args["file_path"] = Path(args["file_path"])

    if not args["file_path"].is_dir():
        parser.error(f"directory does not exist: '{file_path}'")
        args["file_path"] = None

    if args["db_env"] not in valid_db_env:
        parser.error(f"'{args['db_env']}' not in: {valid_db_env}")
    return args


def trigger_etl():
    """driver to generate database record(s) from source JSON"""
    start = time.perf_counter()
    logger.info(f"processing cmd_args: {cmd_args}")
    spotify = SpotifyClient()
    if spotify.connect():
        with PostgresMedia(
                file_path=cmd_args['file_path'],
                db_env=cmd_args['db_env'],
                spotify=spotify,
        ) as pgm:
            if pgm.connect():
                pgm.create_database()
                pgm.drop_tables()
                pgm.create_tables()
                # after processing data, perform various canned queries
                pgm.process_data()
                pgm.query(query=pgq.ARTIST_SELECT, params=["Mazzy Star"])
                pgm.query(query=pgq.ALBUM_SELECT, params=["Debut"])
                pgm.query(query=pgq.TRACK_SELECT, params=["Future Proof"])
                pgm.query(query=pgq.GENRE_SELECT, params=["Trip-Hop", "Alternative"])
                pgm.query(query=pgq.FILE_SELECT, params=[".flac"])
                pgm.query(query=pgq.GAIN_SELECT, params=["-4.0"])
                pgm.query(query=pgq.JOIN_SELECT, params=["Rockabilly"])
                pgm.query(query=pgq.AVG_SIZE_SELECT, params=[])
    logger.info(f"finished {time.perf_counter() - start:0.2f} seconds")


if __name__ == "__main__":
    logger = init_logger(log_name=MODULE)
    cmd_args = parse_cmd_args()
    trigger_etl()
