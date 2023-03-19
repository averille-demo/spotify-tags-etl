"""Spotify API client to lookup tag data."""

from typing import Dict

import spotipy
from rapidfuzz import fuzz
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOauthError

from src.sql.offline_ids import OFFLINE_ALBUM_IDS, OFFLINE_ARTIST_IDS
from src.util.logger import init_logger
from src.util.settings import SpotifyApiConfig, load_spotify_config, parse_pyproject


class SpotifyClient:
    """Class to lookup/add Spotify media tags to Postgres backend."""

    pyproject = parse_pyproject()
    log = init_logger(__file__)

    def __init__(self):
        """Start Spotify API client to query tag data."""
        self.client = None
        self._config = load_spotify_config()

    def connect(self) -> bool:
        """Setup and test if Spotipy OAut2 client successfully connected."""
        try:
            if isinstance(self._config, SpotifyApiConfig):
                client_credentials_manager = SpotifyClientCredentials(
                    client_id=self._config.client_id,
                    client_secret=self._config.client_secret,
                    requests_timeout=self._config.timeout,
                )
                self.client = spotipy.Spotify(
                    client_credentials_manager=client_credentials_manager,
                )
                # test API call, if no exception, client is connected
                if self.client.categories():
                    self.log.info("spotify client connected")
                    return True
        except (SpotifyOauthError, SpotifyException):
            self.log.exception("unable to connect, check settings")
        return False

    def is_connected(self) -> bool:
        """Verify if client is connected."""
        return isinstance(self.client, spotipy.Spotify)

    @staticmethod
    def find_closest_match(keyword: str, items: Dict) -> str:
        """Use fuzzy pattern matching (case insensitive) to find closest match."""
        matches = []
        for item in items:
            fuzz_ratio = round(fuzz.ratio(s1=keyword.lower(), s2=item["name"].lower()), 4)
            matches.append(fuzz_ratio)
        # get index in list to highest similarity match
        max_idx = matches.index(max(matches))
        # return the corresponding id string
        item_id = items[max_idx]["id"]
        return item_id

    def get_artist_id(self, artist_name: str) -> str:
        """Spotify API to lookup artist id by keyword."""
        if self.is_connected():
            results = self.client.search(q=f"artist:{artist_name}", type="artist")
            items = results["artists"].get("items", [])
            artist_id = self.find_closest_match(keyword=artist_name, items=items)
            self.log.info(f"artist: {artist_name} id: {artist_id}")
        else:
            artist_id = OFFLINE_ARTIST_IDS.get(artist_name, "not_found")
            self.log.error(f"offline: {artist_name} artist_id: {artist_id}")
        return artist_id

    def get_album_id(self, artist_id: str, album_name: str) -> str:
        """Spotify API to lookup album id by keyword."""
        if self.is_connected():
            results = self.client.artist_albums(artist_id=artist_id, limit=50)
            items = results.get("items", [])
            album_id = self.find_closest_match(keyword=album_name, items=items)
            self.log.info(f"album: {album_name} (id: {album_id})")
        else:
            album_id = OFFLINE_ALBUM_IDS.get(album_name, "not_found")
            self.log.error(f"offline {album_name} (id: {album_id})")
        return album_id
