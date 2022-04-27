"""
Media driver module to insert JSON media tags into PostgreSQL.
https://www.psycopg.org/docs/errors.html
"""
import os
from pathlib import Path
from typing import List, Dict
import uuid
import pendulum
import psycopg2
import pandas as pd
import toml
from psycopg2 import DatabaseError, InterfaceError, OperationalError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import etl_queries as pgq
from dev_tools_lib import init_logger, limit_path, show_data

MODULE = Path(__file__).resolve().name
CWD_PATH = Path(__file__).resolve().parent

pd.set_option("display.max_rows", 128)
pd.set_option("expand_frame_repr", False)
pd.set_option("display.max_columns", 36)
pd.set_option("display.max_colwidth", 36)


class PostgresMedia:
    """Class to add/remove media tags to Postgres backend."""

    def __enter__(self):
        # super().__init__(**kwargs)
        return self

    def __init__(self, file_path: Path, db_env: str, spotify=None):
        self.src_path = file_path
        self.database = None
        self.db_env = db_env
        self.spotify = spotify
        self.logger = init_logger(log_name=MODULE)
        self.is_config_valid = self.__parse_config()
        self.is_loaded = False
        self.db_conn = None
        self.db_cursor = None

    def __parse_config(self, path=Path(CWD_PATH, "config", "spotify_cfg_private.toml")) -> Dict:
        """Extract credentials from configuration file"""
        try:
            if not path.is_file():
                self.logger.error(msg=f"missing: {limit_path(path=path)}")
            else:
                config = toml.load(path)
                os.environ["PG_HOST"] = config["postgres"][self.db_env]["endpoint"]
                os.environ["PG_PORT"] = config["postgres"][self.db_env]["port"]
                os.environ["PG_DATABASE"] = config["postgres"][self.db_env]["database"]
                os.environ["PG_USERNAME"] = config["postgres"][self.db_env]["username"]
                os.environ["PG_PASSWORD"] = config["postgres"][self.db_env]["password"]
                self.database = os.environ.get("PG_DATABASE")
                return True
        except (KeyError, TypeError):
            self.logger.exception(msg=f"{limit_path(path=path)}")
        return False

    def connect(self) -> bool:
        """Open connection (either postgres or media_db)."""
        timeout = 5
        try:
            if self.is_config_valid:
                self.database = os.environ.get("PG_DATABASE")
                self.db_conn = psycopg2.connect(
                    host=os.environ.get("PG_HOST"),
                    port=os.environ.get("PG_PORT"),
                    dbname=os.environ.get("PG_DATABASE"),
                    user=os.environ.get("PG_USERNAME"),
                    password=os.environ.get("PG_PASSWORD"),
                    connect_timeout=timeout,
                    async_=False,
                )
                if self.db_conn:
                    self.db_conn.set_session(autocommit=True)
                    self.db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    self.db_cursor = self.db_conn.cursor()
                    self.logger.info(f"database: {self.database}")
                    return True
        except OperationalError:
            self.logger.exception(msg=f"connection timeout: ({timeout}s)")
        except (OSError, DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg=f"{self.database}")
        return False

    def show_db_tables(self) -> None:
        """Display current list of media_lib tables."""
        query = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';"
        self.db_cursor.execute(query=query)
        result_set = self.db_cursor.fetchall()
        row_count = self.db_cursor.rowcount
        if result_set:
            print(f"{row_count} result(s): {[''.join(r) for r in result_set]}")

    def show_db_status(self) -> List:
        """Displays current list of Postgres databases on host."""
        try:
            pg_ver = str(self.db_conn.server_version)
            dot_ver = f"{pg_ver[0:1]}.{pg_ver[1:3]}.{pg_ver[-2:]}"
            print(f"\n\tpg_ver: {dot_ver}\n\tdb: {self.database}")
            self.show_db_tables()
        except (AttributeError, DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg="")

    def query(self, query: str, params: List) -> List:
        """
        Query media database for result set based on params.
        https://www.psycopg.org/docs/usage.html#query-parameters
        """
        result_set = []
        try:
            if isinstance(query, str):
                # mogrify returns bytes object
                if not params:
                    params = []
                fmt_query = self.db_cursor.mogrify(query=query, vars=params)
                self.db_cursor.execute(query=fmt_query)
                result_set = self.db_cursor.fetchall()
                # convert mogrify() bytes to string with decode()
                show_data(title=fmt_query.decode(), data=result_set)
        except (SyntaxError, DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg="")
        return result_set

    def user_exists(self, username="postgres") -> bool:
        """Check if user role is configured."""
        try:
            query = f"SELECT rolname FROM pg_roles WHERE rolname IN ('{username}');"
            self.db_cursor.execute(query=query)
            result_set = self.db_cursor.fetchone()
            if not result_set:
                self.logger.error(f"username: {username} not found")
            else:
                self.logger.info(f"username: {''.join(result_set)}")
                return True
        except (DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg=f"username: {username}")
        return False

    def add_user(self, username: str, password: str) -> bool:
        """Create new admin user role to access media database."""
        try:
            if isinstance(username, str) and isinstance(password, str):
                if not self.user_exists(username=username):
                    query = (
                        f"CREATE ROLE {username} WITH "
                        f"LOGIN PASSWORD '{password}' "
                        "CREATEDB CREATEROLE NOINHERIT CONNECTION LIMIT -1 "
                        "VALID UNTIL '2022-12-31';"
                    )
                    self.db_cursor.execute(query=query)
                    self.logger.info(f"added: {username}")
        except (DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg=f"failed to add '{username}'")
        return self.user_exists(username=username)

    def create_database(self, database="media_db", owner="run_admin_run") -> bool:
        """Create media Postgres database."""
        try:
            query = "SELECT datname FROM pg_database;"
            self.db_cursor.execute(query=query)
            result_set = self.db_cursor.fetchall()
            curr_databases = [''.join(r) for r in result_set]
            if database not in curr_databases:
                query = f"CREATE DATABASE {database} WITH ENCODING = 'UTF8' OWNER = {owner} CONNECTION LIMIT = -1;"
                self.logger.info(f"{query}")
                self.db_cursor.execute(query=query)
                self.logger.info(msg=f"database: '{database}' created")
            return True
        except (DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg=f"database: '{database}'")
        return False

    def drop_tables(self) -> bool:
        """Remove tables from Postgres media database."""
        try:
            for table in pgq.TABLES:
                query = f"DROP TABLE IF EXISTS {table};"
                self.db_cursor.execute(query=query)
            self.logger.info(f"dropped tables: {pgq.TABLES}")
            return True
        except (DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg="")
        return False

    def create_tables(self) -> bool:
        """Create tables into Postgres database."""
        tables_init = {}
        try:
            for table, query in pgq.CREATE_TABLES_QUERIES.items():
                self.db_cursor.execute(query=query)
                # returns -1 if query was successful
                if self.db_cursor.rowcount == -1:
                    tables_init[table] = self.db_cursor.statusmessage
            # check return value of all table creations
            if next((status for status in list(tables_init.values())), "CREATE TABLE"):
                self.logger.info(f"created tables: {pgq.TABLES}")
                return True
        except (DatabaseError, InterfaceError, OperationalError):
            self.logger.exception(msg="")
        return False

    def load(self, df: pd.DataFrame) -> bool:
        """Driver to parse JSON file and commit to Postgres database."""
        loaded_ok = {}
        try:
            if isinstance(df, pd.DataFrame):
                for i, series in df.iterrows():
                    artist_name = series["artist_name"]
                    album_title = series["album_title"]
                    track_number = series["track_number"]
                    track_title = series["track_title"]

                    # update values from spotify queries prior to load to postgres
                    series["artist_id"] = self.spotify.get_artist_id(artist_name)
                    series["album_id"] = self.spotify.get_album_id(series["artist_id"], album_title)

                    # create unique key (no duplicates)
                    uuid_tag = str(uuid.uuid4()).split("-", maxsplit=1)[0]
                    track_uid = f"{artist_name} | {album_title} | {track_number:02}-{track_title} | {uuid_tag}"

                    # split out each data row to dedicated sql tables
                    for table, columns in pgq.HEADER_MAP.items():
                        try:
                            # select subset of columns from dataframe
                            self.db_cursor.execute(query=pgq.INSERT_QUERY_MAP[table], vars=series[columns])
                            # returns 1 if query was successful
                            insert_ok = self.db_cursor.rowcount == 1
                            if not insert_ok:
                                self.logger.error(f"insert {track_uid} to {table}")
                            loaded_ok[track_uid] = insert_ok
                        except (KeyError, IndexError, DatabaseError):
                            self.logger.exception(msg=f"table: {table} track: '{track_uid}'")

                    # update log after tables for one series/song were loaded: [artist, album, track, genre, metadata]
                    self.logger.info(f"track_uid_{i:02d}: {track_uid}")
        except (pd.errors.DtypeWarning, pd.errors.ParserWarning):
            self.logger.exception(msg="")
        # validate db inserts were ok for all files
        return next((status for status in list(loaded_ok.values())), True)

    def get_paths_by_extension(self, file_ext: str = ".json") -> list:
        """non-recursive search for files by extension in directory"""
        paths = []
        if self.src_path.is_dir():
            paths = [
                p.absolute()
                for p in sorted(self.src_path.glob(f"*{file_ext}"))
                if p.is_file() and p.stat().st_size > 0
            ]
        return paths

    def process_data(self) -> bool:
        """Locates source JSON files non-recursively from file path."""
        processed_ok = {}
        try:
            self.logger.info(f"processing: {limit_path(path=self.src_path)}")
            for json_path in self.get_paths_by_extension(file_ext=".json"):
                df = pd.read_json(json_path, orient="records", lines=True, encoding="utf-8")
                df["etl_ts"] = pendulum.now(tz='UTC').to_iso8601_string()
                processed_ok[str(json_path)] = self.load(df=df)
            if processed_ok:
                # if all executed successfully, return True
                if next((s for s in list(processed_ok.values())), True):
                    self.is_loaded = True
                else:
                    self.logger.error(f"{self.src_path} {processed_ok}")
        except (OSError, PermissionError, KeyError):
            self.logger.exception(msg=f"{limit_path(path=self.src_path)}")
        return self.is_loaded

    def close(self):
        """Cleanup database connection"""
        if self.db_cursor:
            self.db_cursor.close()
        if self.db_conn:
            self.db_conn.close()

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


if __name__ == "__main__":
    with PostgresMedia(file_path=CWD_PATH.parent.joinpath(*["input"])) as pgm:
        if pgm.connect():
            pgm.create_database()
            pgm.user_exists(username="run_admin_run")
            pgm.show_db_tables()
            pgm.query("SELECT artist_name FROM genre WHERE music_genre IN ('Rockabilly');")
