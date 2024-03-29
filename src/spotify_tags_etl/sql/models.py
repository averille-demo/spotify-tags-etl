"""SQLModel module which combines pydantic and sqlalchemy functionality.

updated: 2024-01-31
https://sqlmodel.tiangolo.com/
"""

from typing import Optional

import pendulum
from pydantic import ConfigDict, condecimal, conint, field_validator
from sqlmodel import Field, SQLModel, create_engine, inspect

from spotify_tags_etl.util.settings import DatabaseConfig, load_db_config

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

VALID_TYPES = [
    "track",
    "artist",
    "album",
    "playlist",
    "show",
    "episode",
    "audio_features",
]


# pylint: disable=[too-few-public-methods, no-self-argument]
class SpotifyFavoriteModel(SQLModel, table=True):  # type: ignore [call-arg]
    """Data model for Spotify 'Liked Songs' playlist including subset of fields."""

    __table_args__ = {"extend_existing": True}
    __tablename__ = "liked_song"

    model_config = ConfigDict(
        populate_by_name=False,
        str_strip_whitespace=True,
        from_attributes=True,
    )

    track_id: str = Field(default=None, primary_key=True)
    type: str
    artist_name: str
    album_name: str
    track_name: str
    track_number: conint(ge=0)  # type: ignore [valid-type]
    duration: str
    release_date: str
    # https://www.loudlab.org/blog/spotify-popularity-leverage-algorithm/
    popularity: conint(ge=0, le=100)  # type: ignore [valid-type]
    added_at: str
    external_url: str
    # timestamp data was pulled from spotify
    extract_date: str = pendulum.now(tz="UTC").to_datetime_string()
    # timestamp JSON was loaded to postgres
    load_date: Optional[str]

    def to_string(self) -> str:
        """Helper abbreviated string representation."""
        return (
            f"artist: '{self.artist_name}' "
            f"track: '{self.track_number:02d}-{self.track_name}' "
            f"track_id: {self.track_id}"
        )

    @field_validator("type", mode="before")
    def check_type(cls, v):
        """Validate type: https://www.iana.org/assignments/uri-schemes/prov/spotify."""
        if v not in VALID_TYPES:
            raise ValueError(f"invalid input: {v} not in {VALID_TYPES}")
        return v


# Musical keys:
# https://en.wikipedia.org/wiki/Pitch_class
PITCH_CLASSES = {
    "-1": "",
    "0": "C",
    "1": "C#,D♭",
    "2": "D",
    "3": "D#,E♭",
    "4": "E,F♭",
    "5": "F,E#",
    "6": "F#,G♭",
    "7": "G",
    "8": "G#,A♭",
    "9": "A",
    "10": "A#,B♭",
    "11": "B,C♭",
}

# https://en.wikipedia.org/wiki/Mode_(music)
MUSICAL_MODES = {
    "0": "minor",
    "1": "Major",
    # others not represented: greek, gregorian, dorian, etc.
}


# pylint: disable=[too-few-public-methods, no-self-argument]
class SpotifyAudioFeatureModel(SQLModel, table=True):  # type: ignore [call-arg]
    """Spotify audio feature data model.

    https://developer.spotify.com/documentation/web-api/reference/get-several-audio-features
    """

    __table_args__ = {"extend_existing": True}
    __tablename__ = "audio_feature"

    model_config = ConfigDict(
        populate_by_name=False,
        str_strip_whitespace=True,
        from_attributes=True,
    )

    type: str
    id: str = Field(default=None, primary_key=True)
    uri: str
    # if acoustic instruments
    acousticness: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0)  # type: ignore [valid-type]
    # gauge of tempo, rhythm, beat, etc.
    danceability: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0.0)  # type: ignore [valid-type]
    # duration of the track in milliseconds
    duration_ms: conint(ge=0) = Field(default=0)  # type: ignore [valid-type]
    # intensity, fast, loud, noisy (0=mellow to 1=obnoxious),
    energy: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0.0)  # type: ignore [valid-type]
    # if track contains vocal/lyrics
    instrumentalness: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0.0)  # type: ignore [valid-type]
    # pitch class notation where 0=C, 11=B
    key: str
    # modality (minor=0, major=1) of track
    mode: str
    # if track is recorded live
    liveness: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0.0)  # type: ignore [valid-type]
    # loudness of a track in decibels (dB)
    loudness: condecimal(le=0.0, decimal_places=3) = Field(default=0.0)  # type: ignore [valid-type]
    # presence of spoken words in a track (0=no speech, 1=speech)
    speechiness: condecimal(ge=0.0, le=1.0) = Field(default=0.0)  # type: ignore [valid-type]
    # beats per minute (BPM)
    tempo: condecimal(gt=0.0) = Field(default=0.0)  # type: ignore [valid-type]
    # number of beats per bar
    time_signature: conint(gt=0) = Field(default=0)  # type: ignore [valid-type]
    # musical positiveness - 0.0 to 1.0 (1=happy, 0=sad)
    valence: condecimal(ge=0.0, le=1.0, decimal_places=6) = Field(default=0.0)  # type: ignore [valid-type]
    track_href: str
    analysis_url: str
    # timestamp data was pulled from spotify
    extract_date: str = pendulum.now(tz="UTC").to_datetime_string()
    # timestamp JSON was loaded to postgres
    load_date: Optional[str]

    def to_string(self) -> str:
        """Helper abbreviated string representation."""
        return f"track_id: {self.id} {self.key} {self.mode} ({self.tempo} bpm)"

    @field_validator("key", mode="before")
    def replace_pitch_class(cls, v: str):
        """Replace integer value with musical key."""
        if v.isdigit():
            return PITCH_CLASSES.get(v)
        return v

    @field_validator("mode", mode="before")
    def replace_mode(cls, v: str):
        """Replace integer value with Major/minor."""
        if v.isdigit():
            return MUSICAL_MODES.get(v)
        return v

    @field_validator("type", mode="before")
    def check_type(cls, v):
        """Validate type: https://www.iana.org/assignments/uri-schemes/prov/spotify."""
        if v not in VALID_TYPES:
            raise ValueError(f"invalid input: {v} not in {VALID_TYPES}")
        return v


def show_tables(engine, include_columns: bool = False):
    """Display current database tables."""
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"({len(tables)}) tables:")
    for table in tables:
        print(f"  {table=}")
        if include_columns:
            for column in inspector.get_columns(table):
                print(f"    {column=}")


def init_database(verbose: bool = False):
    """Initialize engine and recreate database and tables."""
    config: DatabaseConfig = load_db_config()
    # avoid special characters in password
    url = f"postgresql://{config.username}:{config.password}@{config.endpoint}:{config.port}/{config.database}"
    engine = create_engine(url=url, echo=verbose)
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
    show_tables(engine)
    return engine
