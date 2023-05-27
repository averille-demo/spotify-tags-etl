"""Data models for Spotify usage history."""

from pendulum import Date, DateTime, Time
from pydantic import BaseModel, HttpUrl, PositiveInt, confloat, conint, validator


# pylint: disable=[too-few-public-methods, no-self-argument]
class SpotifyFavoriteModel(BaseModel):
    """Spotify favorite (liked) track data model."""

    id: str
    artist: str
    album: str
    track: str
    track_number: PositiveInt
    duration: Time
    release_date: Date
    # https://www.loudlab.org/blog/spotify-popularity-leverage-algorithm/
    popularity: conint(ge=0, le=100)  # type: ignore [valid-type]
    date_added: DateTime
    url: HttpUrl

    class Config:
        """Model configuration options.

        https://docs.pydantic.dev/usage/model_config/#options
        """

        allow_mutation = False
        check_fields = True
        allow_population_by_field_name = False
        anystr_strip_whitespace = True
        json_encoders = {
            Date: lambda _: _.to_date_string(),
            Time: lambda _: _.strftime("%H:%M:%S.%f"),
            DateTime: lambda _: _.to_iso8601_string(),
        }


# Musical keys: https://en.wikipedia.org/wiki/Pitch_class
PITCH_CLASSES = {
    -1: "",
    0: "C",
    1: "C#,D♭",
    2: "D",
    3: "D#,E♭",
    4: "E,F♭",
    5: "F,E#",
    6: "F#,G♭",
    7: "G",
    8: "G#,A♭",
    9: "A",
    10: "A#,B♭",
    11: "B,C♭",
}

# https://en.wikipedia.org/wiki/Mode_(music)
MUSICAL_MODES = {
    0: "Major",
    1: "minor",
    # others not represented: greek, gregorian, dorian, etc.
}


# pylint: disable=[too-few-public-methods, no-self-argument]
class SpotifyAudioFeatureModel(BaseModel):
    """Spotify audio feature data model.

    https://developer.spotify.com/documentation/web-api/reference/get-several-audio-features
    """

    # if acoustic instruments
    acousticness: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]
    # gauge of tempo, rhythm, beat, etc.
    danceability: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]
    # duration of the track in milliseconds
    duration_ms: conint(ge=0)  # type: ignore [valid-type]
    # intensity, fast, loud, noisy (0=mellow to 1=obnoxious),
    energy: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]
    # if track contains vocal/lyrics
    instrumentalness: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]
    # pitch class notation where 0=C, 11=B
    key: conint(ge=-1, le=11)  # type: ignore [valid-type]
    # modality (major or minor) of a track
    mode: int
    # if track is recorded live
    liveness: float
    # loudness of a track in decibels (dB)
    loudness: float
    # presence of spoken words in a track (0=no speech, 1=speech)
    speechiness: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]
    # beats per minute (BPM)
    tempo: confloat(gt=0.0)  # type: ignore [valid-type]
    # number of beats per bar,
    time_signature: conint(gt=0)  # type: ignore [valid-type]
    # musical positiveness - 0.0 to 1.0 (1=happy, 0=sad)
    valence: confloat(ge=0.0, le=1.0)  # type: ignore [valid-type]

    class Config:
        """Model configuration options.

        https://docs.pydantic.dev/usage/model_config/#options
        """

        allow_mutation = False
        check_fields = True
        allow_population_by_field_name = False

    @validator("key")
    def replace_pitch_class(cls, v):
        """Replace integer value with musical key."""
        return PITCH_CLASSES.get(v)

    @validator("mode")
    def replace_mode(cls, v):
        """Replace integer value with Major/minor."""
        return MUSICAL_MODES.get(v)
