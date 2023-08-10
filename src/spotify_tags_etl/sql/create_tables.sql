-- generate tables: artist, album, track, genre, and metadata

DROP TABLE IF EXISTS artist;
CREATE TABLE IF NOT EXISTS artist (
    id SERIAL PRIMARY KEY,
    artist_id VARCHAR(150) NULL,
    artist_name VARCHAR(150) NULL,
    composer VARCHAR(150) NULL,
    conductor VARCHAR(150) NULL,
    extract_date TIMESTAMP WITH TIME ZONE
);

DROP TABLE IF EXISTS album;
CREATE TABLE IF NOT EXISTS album (
    id SERIAL PRIMARY KEY,
    album_id VARCHAR(150) NULL,
    artist_id VARCHAR(150) NULL,
    album_title VARCHAR(200) NULL,
    year SMALLINT,
    album_gain NUMERIC(5, 2),
    album_art VARCHAR(48) NULL,
    extract_date TIMESTAMP WITH TIME ZONE
);

DROP TABLE IF EXISTS track;
CREATE TABLE IF NOT EXISTS track (
    id SERIAL PRIMARY KEY,
    track_id VARCHAR(150) NULL,
    album_title VARCHAR(200) NULL,
    track_title VARCHAR(200) NULL,
    artist_id VARCHAR(100) NULL,
    track_number SMALLINT,
    track_length VARCHAR(16) NULL,
    rating VARCHAR(16) NULL,
    comment VARCHAR(128) NULL,
    track_gain NUMERIC(5, 2),
    bitrate INT,
    sampling_rate INT,
    extract_date TIMESTAMP WITH TIME ZONE
);

DROP TABLE IF EXISTS genre;
CREATE TABLE IF NOT EXISTS genre (
    id SERIAL PRIMARY KEY,
    artist_id VARCHAR(100) NULL,
    artist_name VARCHAR(100) NULL,
    music_genre VARCHAR(150) NULL,
    genre_in_dict VARCHAR(48) NULL,
    extract_date TIMESTAMP WITH TIME ZONE
);

DROP TABLE IF EXISTS metadata;
CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    track_id VARCHAR(100) NULL,
    file_size INTEGER,
    readable_size VARCHAR(64) NULL,
    file_ext VARCHAR(16) NULL,
    encoder VARCHAR(64) NULL,
    file_name VARCHAR(256) NULL,
    path_len SMALLINT,
    last_modified TIMESTAMP,
    encoding VARCHAR(24) NULL,
    hash VARCHAR(150) NULL,
    extract_date TIMESTAMP WITH TIME ZONE
);
