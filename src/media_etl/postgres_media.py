"""PostgreSQL client wrapper to setup and query backend.

https://www.psycopg.org/docs/index.html
"""
import pprint as pp
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd
import pendulum
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, connection
from psycopg2.extras import NamedTupleCursor

from media_etl.spotify_client import SpotifyClient
from media_etl.util.logger import get_relative_path, init_logger
from media_etl.util.settings import DATA_PATH, SQL_PATH, DatabaseConfig, load_db_config, parse_pyproject

pd.set_option("display.max_rows", 128)
pd.set_option("expand_frame_repr", False)
pd.set_option("display.max_columns", 16)
pd.set_option("display.max_colwidth", 36)


class PostgresMedia:
    """Class to add/remove media tags to Postgres backend."""

    pyproject = parse_pyproject()
    log = init_logger(__file__)

    def __enter__(self):
        """Dunder methods enter/exit needed for with() context."""
        return self

    def __init__(self):
        """Initialize class."""
        self.spotify_client: SpotifyClient = SpotifyClient()
        self._config: DatabaseConfig = load_db_config()
        self.is_loaded: bool = False
        self.db_conn: connection = None

    def connect(self) -> bool:
        """Open connection (either postgres or media_db)."""
        try:
            if isinstance(self._config, DatabaseConfig):
                self.db_conn = psycopg2.connect(
                    host=self._config.endpoint,
                    port=self._config.port,
                    dbname=self._config.database,
                    user=self._config.username,
                    password=self._config.password,
                    connect_timeout=self._config.timeout,
                    async_=False,
                )
                if isinstance(self.db_conn, connection):
                    self.db_conn.set_session(autocommit=True)
                    self.db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    self.log.info(f"connected: {self._config.database}")
                    return True
        except psycopg2.OperationalError:
            self.log.exception(f"connection timeout: ({self._config.timeout}s)")
        except (psycopg2.DatabaseError, psycopg2.InterfaceError):
            self.log.exception(f"{self._config.database}")
        return False

    def show_tables(self) -> None:
        """Display current list of database tables."""
        query = "SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';"
        with self.db_conn.cursor() as cursor:
            cursor.execute(query)
            result_set = cursor.fetchall()
            if result_set:
                print(f"{cursor.rowcount} table(s): {[''.join(r) for r in result_set]}")

    def show_status(self) -> None:
        """Displays current list of Postgres databases on host."""
        try:
            pg_ver = str(self.db_conn.server_version)
            print(f"database: {self._config.database}\t version: {pg_ver}")
            self.show_tables()
        except (AttributeError, psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"{self._config.database}")

    def query(self, query: str, params: List) -> List[Tuple]:
        """Query database with parameters.

        https://www.psycopg.org/docs/usage.html#query-parameters
        """
        result_set = []
        try:
            if isinstance(query, str):
                if not params:
                    params = []
                with self.db_conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
                    # optional: use mogrify method to format query string
                    params_query = cursor.mogrify(query=query, vars=params)
                    cursor.execute(params_query)
                    result_set = cursor.fetchall()
                    # convert mogrify() bytes to string with decode()
                    print(params_query.decode())
                    pp.pprint(object=result_set, indent=2, width=120, compact=True)
        except (SyntaxError, psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"{query}")
        return result_set

    def verify_role_exists(self, role_name: str) -> bool:
        """Check if role is configured."""
        try:
            query = "SELECT rolname FROM pg_roles WHERE rolname=%s;"
            with self.db_conn.cursor() as cursor:
                cursor.execute(query, [role_name])
                result_set = cursor.fetchone()
                if cursor.rowcount == 0:
                    self.log.error(f"rolname: '{role_name}' not found")
                else:
                    self.log.info(f"rolname: '{result_set[0]}' found")
                    return True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"rolname: {role_name}")
        return False

    def add_role(self, role_name: str, password: str) -> bool:
        """Create new admin role to access media database."""
        try:
            if isinstance(role_name, str) and isinstance(password, str):
                if not self.verify_role_exists(role_name=role_name):
                    expiration_date = pendulum.now().add(months=2).to_date_string()
                    query = (
                        "CREATE ROLE %s WITH LOGIN PASSWORD '%s' "
                        "CREATEDB CREATEROLE NOINHERIT CONNECTION LIMIT -1 "
                        f"VALID UNTIL '{expiration_date}';"
                    )
                    with self.db_conn.cursor() as cursor:
                        cursor.execute(query, [role_name, password])
                        self.log.info(f"added role: '{role_name}'")
                        return True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"failed to add role: '{role_name}'")
        return False

    def create_database(self) -> bool:
        """Create media database."""
        try:
            # check if database already exists
            query = "SELECT datname FROM pg_database;"
            with self.db_conn.cursor() as cursor:
                cursor.execute(query)
                result_set = cursor.fetchall()
                curr_databases = ["".join(r) for r in result_set]
                # if does not exist, create database
                if self._config.database not in curr_databases:
                    query = (
                        f"CREATE DATABASE {self._config.database} "
                        f"WITH ENCODING='UTF8' OWNER={self._config.username} CONNECTION LIMIT=-1;"
                    )
                    self.log.info(f"{query}")
                    cursor.execute(query)
                    self.log.info(f"database: '{self._config.database}' created")
                    return True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"database: '{self._config.database}'")
        return False

    def recreate_tables(self) -> bool:
        """Performs DROP and CREATE tables statements from SQL file."""
        try:
            with open(file=Path(SQL_PATH, "create_tables.sql"), mode="r", encoding="utf-8") as sql:
                with self.db_conn.cursor() as cursor:
                    cursor.execute(query=sql.read())
                    if cursor.rowcount == -1:
                        # query was successful
                        return True
        except (psycopg2.DatabaseError, psycopg2.InterfaceError, psycopg2.OperationalError):
            self.log.exception(f"{self._config.database}")
        return False

    def query_table_columns(self, table_schema: str = "public") -> Dict[str, List[str]]:
        """Query all columns found in each table of a given database."""
        table_map: Dict[str, List[str]] = {}
        tbl_query = "SELECT table_name FROM information_schema.tables WHERE table_catalog=%s AND table_schema=%s;"
        col_query = "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s;"
        with self.db_conn.cursor() as cursor:
            cursor.execute(tbl_query, [self._config.database, table_schema])
            tables = cursor.fetchall()
            for table_name in ["".join(t) for t in tables]:
                table_map[table_name] = []
                cursor.execute(col_query, [table_schema, table_name])
                columns = cursor.fetchall()
                table_map[table_name].extend(["".join(c) for c in columns])
                table_map[table_name].remove("id")
        return table_map

    def load_df(self, df: pd.DataFrame, truncate: bool = False) -> bool:
        """Driver to parse JSON file and commit to Postgres database."""
        loaded_ok = {}
        if isinstance(df, pd.DataFrame):
            if truncate:
                df = df.head(1)
            for i, series in df.iterrows():
                # update values from spotify queries prior to load to postgres
                if self.spotify_client:
                    series["artist_id"] = self.spotify_client.get_artist_id(
                        artist_name=series["artist_name"],
                    )
                    series["album_id"] = self.spotify_client.get_album_id(
                        artist_id=series["artist_id"],
                        album_title=series["album_title"],
                    )
                    series["track_id"] = self.spotify_client.get_track_id(
                        artist_name=series["artist_name"],
                        album_title=series["album_title"],
                        track_title=series["track_title"],
                    )
                # create unique key (no duplicates even with same artist/song across different albums)
                track_tag = (
                    f"{i:03d} | {series['artist_name']} | {series['album_title']} | "
                    f"{series['track_number']:02d}-{series['track_title']}"
                )
                # split out each data row in json file to dedicated database tables
                for table, columns in self.query_table_columns().items():
                    try:
                        with self.db_conn.cursor() as cursor:
                            query = (
                                f"INSERT INTO {table} ({', '.join(columns)}) "
                                f"VALUES ({', '.join(['%s'] * len(columns))})"
                            )
                            # select ordered subset of columns from series
                            cursor.execute(query=query, vars=series[columns])
                            if cursor.rowcount == 1:
                                loaded_ok[track_tag] = True
                            else:
                                self.log.error(f"{table} insert row: {track_tag}")
                                loaded_ok[track_tag] = False
                    except (pd.errors.DtypeWarning, pd.errors.ParserWarning, psycopg2.DatabaseError):
                        self.log.exception(f"table: {table} track: '{track_tag}'")
        # only return true if all inserts were successful
        return next((status for status in list(loaded_ok.values())), True)

    @staticmethod
    def get_source_data() -> List[Path]:
        """Performs non-recursive search for JSON files by extension in directory."""
        paths = []
        if DATA_PATH.is_dir():
            paths = [p.absolute() for p in sorted(DATA_PATH.glob("*.json")) if p.is_file() and p.stat().st_size > 0]
        return paths

    def load_data(self) -> bool:
        """Loads JSON file(s) from source data folder."""
        processed_ok = {}
        try:
            self.log.info(f"processing: {get_relative_path(DATA_PATH)}")
            for path in self.get_source_data():
                df = pd.read_json(path, orient="records", lines=True, encoding="utf-8")
                df["extract_date"] = pendulum.now(tz="UTC").to_iso8601_string()
                processed_ok[path.as_posix()] = self.load_df(df=df)
            if processed_ok:
                # if all executed successfully, return True
                if next((p for p in list(processed_ok.values())), True):
                    self.is_loaded = True
                else:
                    self.log.error(f"{get_relative_path(DATA_PATH)} {processed_ok}")
        except (OSError, PermissionError, KeyError):
            self.log.exception(f"{get_relative_path(DATA_PATH)}")
        return self.is_loaded

    def close(self):
        """Cleanup database connection."""
        if self.db_conn:
            self.db_conn.close()

    def __exit__(self, exc_type, exc_value, traceback):
        """Close with context."""
        self.close()
