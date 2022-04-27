"""Tables and insert queries for PostgreSQL.
https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
"""
ARTIST = "artist"
ALBUM = "album"
TRACK = "track"
GENRE = "genre"
METADATA = "metadata"

TABLES = [ARTIST, ALBUM, TRACK, GENRE, METADATA]


def build_placeholders(query: str, params: list) -> str:
    """Create placeholder string '(%s,...)' based on size of params"""
    placeholders = ""
    query_str = f"{query}"
    if isinstance(params, list) and len(params) > 0:
        if any(sql in query for sql in ["IN", "VALUES"]):
            placeholders += "("
            placeholders += ", ".join(["%s"] * len(params))
            placeholders += ")"
            if "IN" in query:
                query_str = query.replace("IN", f"IN {placeholders}")
            elif "VALUES" in query:
                query_str = query.replace("VALUES", f"VALUES {placeholders}")
    if query_str[-1] != ";":
        query_str += ";"
    print(f"{placeholders:24} {params}\n{query_str}")
    return query_str


ARTIST_HEADERS = ["artist_id", "artist_name", "composer", "conductor", "etl_ts"]
CREATE_ARTIST_QUERY = (
    f"CREATE TABLE IF NOT EXISTS {ARTIST} "
    f"(id SERIAL PRIMARY KEY, "
    f"artist_id VARCHAR(150) NULL, "
    f"artist_name VARCHAR(150) NULL, "
    f"composer VARCHAR(150) NULL, "
    f"conductor VARCHAR(150) NULL, "
    f"etl_ts TIMESTAMP WITH TIME ZONE);"
)

ALBUM_HEADERS = ["album_id", "artist_id", "album_title", "year", "album_gain", "album_art", "etl_ts"]
CREATE_ALBUM_QUERY = (
    f"CREATE TABLE IF NOT EXISTS {ALBUM} "
    f"(id SERIAL PRIMARY KEY, "
    f"album_id VARCHAR(150) NULL, "
    f"artist_id VARCHAR(150) NULL, "
    f"album_title VARCHAR(200) NULL, "
    f"year SMALLINT, "
    f"album_gain NUMERIC(5,2), "
    f"album_art VARCHAR(48) NULL, "
    f"etl_ts TIMESTAMP WITH TIME ZONE);"
)

TRACK_HEADERS = [
    "track_id",
    "album_title",
    "track_title",
    "track_number",
    "track_length",
    "artist_id",
    "rating",
    "comment",
    "track_gain",
    "bitrate",
    "sampling_rate",
    "etl_ts",
]
CREATE_TRACK_QUERY = (
    f"CREATE TABLE IF NOT EXISTS {TRACK} "
    f"(id SERIAL PRIMARY KEY, "
    f"track_id VARCHAR(150) NULL, "
    f"album_title VARCHAR(200) NULL, "
    f"track_title VARCHAR(200) NULL, "
    f"artist_id VARCHAR(100) NULL, "
    f"track_number SMALLINT, "
    f"track_length VARCHAR(16) NULL, "
    f"rating VARCHAR(16) NULL, "
    f"comment VARCHAR(128) NULL, "
    f"track_gain NUMERIC(5,2), "
    f"bitrate INT, "
    f"sampling_rate INT, "
    f"etl_ts TIMESTAMP WITH TIME ZONE);"
)

GENRE_HEADERS = ["artist_id", "artist_name", "music_genre", "genre_in_dict", "etl_ts"]
CREATE_GENRE_QUERY = (
    f"CREATE TABLE IF NOT EXISTS {GENRE} "
    f"(id SERIAL PRIMARY KEY, "
    f"artist_id VARCHAR(100) NULL, "
    f"artist_name VARCHAR(100) NULL, "
    f"music_genre VARCHAR(150) NULL, "
    f"genre_in_dict VARCHAR(48) NULL, "
    f"etl_ts TIMESTAMP WITH TIME ZONE);"
)

METADATA_HEADERS = [
    "track_id",
    "file_size",
    "readable_size",
    "file_ext",
    "encoder",
    "file_name",
    "path_len",
    "last_modified",
    "encoding",
    "hash",
    "etl_ts",
]
CREATE_METADATA_QUERY = (
    f"CREATE TABLE IF NOT EXISTS {METADATA} "
    f"(id SERIAL PRIMARY KEY, "
    f"track_id VARCHAR(100) NULL, "
    f"file_size INTEGER, "
    f"readable_size VARCHAR(64) NULL, "
    f"file_ext VARCHAR(16) NULL, "
    f"encoder VARCHAR(64) NULL, "
    f"file_name VARCHAR(256) NULL, "
    f"path_len SMALLINT, "
    f"last_modified TIMESTAMP, "
    f"encoding VARCHAR(24) NULL, "
    f"hash VARCHAR(150) NULL, "
    f"etl_ts TIMESTAMP WITH TIME ZONE);"
)

HEADER_MAP = {
    ARTIST: ARTIST_HEADERS,
    ALBUM: ALBUM_HEADERS,
    TRACK: TRACK_HEADERS,
    GENRE: GENRE_HEADERS,
    METADATA: METADATA_HEADERS,
}

CREATE_TABLES_QUERIES = {
    ARTIST: CREATE_ARTIST_QUERY,
    ALBUM: CREATE_ALBUM_QUERY,
    TRACK: CREATE_TRACK_QUERY,
    GENRE: CREATE_GENRE_QUERY,
    METADATA: CREATE_METADATA_QUERY,
}

ARTIST_INSERT = (
    f"INSERT INTO {ARTIST} "
    f"(artist_id, artist_name, composer, conductor, etl_ts) "
    f"VALUES (%s, %s, %s, %s, %s);"
)

ALBUM_INSERT = (
    f"INSERT INTO {ALBUM} "
    f"(album_id, artist_id, album_title, year, "
    f"album_gain, album_art, etl_ts) "
    f"VALUES (%s, %s, %s, %s, %s, %s, %s);"
)

TRACK_INSERT = (
    f"INSERT INTO {TRACK} "
    f"(track_id, album_title, track_title, track_number, "
    f"track_length, artist_id, rating, comment, track_gain, bitrate, sampling_rate, etl_ts) "
    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
)

GENRE_INSERT = (
    f"INSERT INTO {GENRE} "
    f"(artist_id, artist_name, music_genre, genre_in_dict, etl_ts) "
    f"VALUES (%s, %s, %s, %s, %s);"
)

METADATA_INSERT = (
    f"INSERT INTO {METADATA} "
    f"(track_id, file_size, readable_size, file_ext, "
    f"encoder, file_name, path_len, last_modified, "
    f"encoding, hash, etl_ts)"
    f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
)

INSERT_QUERY_MAP = {
    ARTIST: ARTIST_INSERT,
    ALBUM: ALBUM_INSERT,
    TRACK: TRACK_INSERT,
    GENRE: GENRE_INSERT,
    METADATA: METADATA_INSERT,
}

"""Select queries for PostgreSQL."""
ARTIST_SELECT = (
    f"SELECT artist_id, artist_name, composer "
    f"FROM {ARTIST} "
    f"WHERE artist_name IN (%s);"
)

ALBUM_SELECT = (
    f"SELECT album_id, album_title, year, album_gain "
    f"FROM {ALBUM} "
    f"WHERE album_title IN (%s);"
)

TRACK_SELECT = (
    f"SELECT artist_id, album_title, track_title, track_length, rating "
    f"FROM {TRACK} "
    f"WHERE track_title IN (%s);"
)

GAIN_SELECT = (
    f"SELECT m.album_gain, a.artist_name, t.album_title "
    f"FROM {TRACK} t "
    f"JOIN {ARTIST} a ON t.artist_id = a.artist_id "
    f"JOIN {ALBUM} m ON m.artist_id = a.artist_id "
    f"WHERE m.album_gain <= (%s) "
    f"ORDER BY m.album_gain DESC;"
)

JOIN_SELECT = (
    f"SELECT a.artist_name, t.album_title "
    f"FROM {ARTIST} a "
    f"JOIN {GENRE} g ON g.artist_id = a.artist_id "
    f"JOIN {TRACK} t ON t.artist_id = a.artist_id "
    f"WHERE g.music_genre IN (%s) "
    f"ORDER BY artist_name;"
)

GENRE_SELECT = (
    f"SELECT artist_name, music_genre "
    f"FROM {GENRE} "
    f"WHERE music_genre IN (%s, %s);"
)

FILE_SELECT = (
    f"SELECT file_name, encoding, file_ext "
    f"FROM {METADATA} "
    f"WHERE file_ext = (%s);"
)

AVG_SIZE_SELECT = (
    f"SELECT AVG(file_size) "
    f"FROM {METADATA};"
)

if __name__ == "__main__":
    test_query_prefix = f"SELECT artist_name FROM {GENRE} WHERE music_genre IN"
    build_placeholders(query=test_query_prefix, params=["Trip-Hop", "Indie", "Noise"])
