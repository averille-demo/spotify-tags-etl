"""Spotify API client to lookup tag data."""
import os
from pathlib import Path
import spotipy
from rapidfuzz import fuzz
import toml
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOauthError
from spotipy.exceptions import SpotifyException
from dev_tools_lib import init_logger, limit_path

MODULE = Path(__file__).resolve().name
CWD_PATH = Path(__file__).resolve().parent
DEBUG = False

OFFLINE_ARTIST_IDS = {
    "Arcade Fire": "3kjuyTCjPG1WMFCiyc5IuB",
    "Frank Sinatra": "1Mxqyy3pSjf8kZZL4QVxS0",
    "Interpol": "3WaJSfKnzc65VDgmj2zU8B",
    "Rimsky-Korsakov": "2kXJ68O899XvWOBdpzlXgs",
    "M. Ward": "6nXSnNEdLuKTzAQozRtqiI",
    "Massive Attack": "6FXMGgJwohJLUSr5nVlf9X",
    "Mazzy Star": "37w38cCSGgKLdayTRjna4W",
    "Ravel": "17hR0sYHpx7VYTMRfFUOmY",
    "Beethoven": "2wOqMjp9TyABvtHdOSOTUS",
    "Björk": "7w29UYBi0qsHi5RTcv3lmA",
    "Patsy Cline": "7dNsHhGeGU5MV01r06O8gK",
    "Sallie Ford & The Sound Outside": "0Z8RhQLJrLxKMWoUW2qo95",
}

OFFLINE_ALBUM_IDS = {
    "The Suburbs": "3DrgM5X3yX1JP1liNLAOHI",
    "Sinatra Reprise": "4Rka7iTWRtRUFouxyzEKKV",
    "Turn On The Bright Lights": "79deKDaslwLfH3yPR2T3SB",
    "Capriccio Espagnol": "4aIDs5QPfX9T7SdPIXOwVL",
    "Hold Time": "4C8AUW89DL5LE5ikBBm4sp",
    "100th Window": "60szvcndZTCqG9E7GSAplB",
    "So Tonight That I Might See": "5K18gTgac0q6Jma5HkV1vA",
    "Rapsodie Espagnol": "2tVaOSl5WI3hfTLMmkxcWs",
    "Symphony No.8 in F-major, Op.93": "7w29UYBi0qsHi5RTcv3lmA",
    "Debut": "3icT9XGrBfhlV8BKK4WEGX",
    "Definitive Collection": "3g5uyAp8sS8LnnCxh9y2em",
    "Dirty Radio": "7I9KroNPmpw9qFYZ8Vp7pN",
}


class SpotifyClient:
    """Class to lookup/add Spotify media tags to Postgres backend."""

    def __init__(self):
        """Start Spotify API client to lookup tag data."""
        self.client = None
        self.logger = init_logger(log_name=MODULE)
        self.is_config_valid = self.__parse_config()

    def __parse_config(self, path=Path(CWD_PATH, "config", "spotify_cfg_private.toml")) -> bool:
        """Extracts configuration credentials to environmental variables"""
        try:
            if not path.is_file():
                self.logger.error(msg=f"missing {limit_path(path=path)}")
            elif isinstance(path, Path) and path.is_file():
                config = toml.load(path)
                os.environ["SPOTIPY_CLIENT_ID"] = config["spotify"]["client_id"]
                os.environ["SPOTIPY_CLIENT_SECRET"] = config["spotify"]["client_secret"]
                os.environ["SPOTIPY_REDIRECT_URI"] = config["spotify"]["redirect_uri"]
                return True
        except (KeyError, TypeError):
            self.logger.exception(msg=f"{limit_path(path=path)}")
        return False

    def connect(self) -> bool:
        """True if Spotify OAut2 client successfully connected"""
        try:
            if self.is_config_valid:
                client_id = os.environ.get("SPOTIPY_CLIENT_ID")
                client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
                scc = SpotifyClientCredentials(
                    client_id=client_id,
                    client_secret=client_secret,
                )
                self.client = spotipy.Spotify(client_credentials_manager=scc)
                # test API call, if no exception, client is connected
                self.client.artist("spotify:artist:7w29UYBi0qsHi5RTcv3lmA")
                return True
        except (SpotifyOauthError, SpotifyException):
            self.logger.exception(msg="unable to connect")
        return False

    def is_connected(self):
        """verify if client is connected"""
        return isinstance(self.client, spotipy.Spotify)

    def get_artist_id(self, artist_name: str) -> str:
        """Spotify API to lookup artist ID."""
        artist_id = ""
        if self.is_connected():
            try:
                results = self.client.search(q=f"artist:{artist_name}", type="artist")
                items = results["artists"].get("items", [])
                if len(items) > 0:
                    artist_id = items[0]["id"]
            except (SpotifyOauthError, SpotifyException):
                self.logger.exception(msg=f"artist: {artist_name}")
        else:
            artist_id = OFFLINE_ARTIST_IDS[artist_name]
            self.logger.error(msg=f"offline artist: {artist_name}")
        return artist_id

    def get_album_id(self, artist_id: str, target_album: str) -> str:
        """Spotify API to lookup album ID using rapidfuzz for closest match."""
        album_id = ""
        if self.is_connected():
            try:
                results = self.client.artist_albums(artist_id=artist_id, limit=50)
                albums = results["items"]
                ratios = []
                for album in albums:
                    fuzz_ratio = round(fuzz.ratio(target_album.lower(), album["name"].lower()), 4)
                    ratios.append(fuzz_ratio)
                # check album title by string similarity matching
                max_idx = ratios.index(max(ratios))
                album_id = albums[max_idx]["id"]
                if DEBUG:
                    print(f"album: {albums['name']}\t id: {albums['id']}\t ratio: {fuzz_ratio}")
                    print(
                        f"idx: {max_idx} max:{max(ratios)}\n"
                        f"input_album:   {target_album}\n"
                        f"closest_album: {albums[max_idx]['name']}\n"
                        f"album_id:   {album_id}\n"
                    )
            except (SpotifyOauthError, SpotifyException):
                self.logger.exception(msg=f"album: {target_album}")
        else:
            album_id = OFFLINE_ALBUM_IDS[target_album]
            self.logger.error(msg=f"offline {target_album} album_id: {album_id}")
        return album_id
