"""Parameterized queries to postgres backend.

https://www.psycopg.org/psycopg3/docs/basic/params.html
https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS
"""

ARTIST_SELECT = "SELECT artist_id, artist_name, composer FROM artist WHERE artist_name IN (%s);"

ALBUM_SELECT = "SELECT album_id, album_title, year, album_gain FROM album WHERE album_title IN (%s);"

TRACK_SELECT = "SELECT artist_id, album_title, track_title, track_length, rating FROM track WHERE track_title IN (%s);"

GAIN_SELECT = (
    "SELECT m.album_gain, a.artist_name, t.album_title "
    "FROM track t "
    "JOIN artist a ON t.artist_id = a.artist_id "
    "JOIN album m ON m.artist_id = a.artist_id "
    "WHERE m.album_gain < (%s) "
    "ORDER BY m.album_gain DESC;"
)

JOIN_SELECT = (
    "SELECT a.artist_name, t.album_title "
    "FROM artist a "
    "JOIN genre g ON g.artist_id = a.artist_id "
    "JOIN track t ON t.artist_id = a.artist_id "
    "WHERE g.music_genre IN (%s) "
    "ORDER BY artist_name;"
)

GENRE_SELECT = "SELECT artist_name, music_genre FROM genre WHERE music_genre IN (%s, %s);"

FILE_SELECT = "SELECT file_name, encoding, file_ext FROM metadata WHERE file_ext = (%s);"

AVG_SIZE_SELECT = "SELECT AVG(file_size) FROM metadata;"


def build_placeholders(query: str, params: list) -> str:
    """Create placeholder string '(%s,...)' based on size of params."""
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


if __name__ == "__main__":
    test_query_prefix = "SELECT artist_name FROM genre WHERE music_genre IN"
    build_placeholders(query=test_query_prefix, params=["Trip-Hop", "Indie", "Classical", "Rockabilly"])
