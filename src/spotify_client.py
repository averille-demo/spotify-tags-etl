"""Spotify API client to lookup music tag data."""
import json
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Tuple

from rapidfuzz import fuzz
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOauthError

from src.sql.offline_ids import OFFLINE_ALBUM_IDS, OFFLINE_ARTIST_IDS, OFFLINE_TRACK_IDS
from src.util.logger import init_logger
from src.util.settings import API_PATH, DEBUG, SpotifyApiConfig, load_spotify_config, parse_pyproject


class SpotifyClient:
    """Class to lookup/add Spotify media tags to Postgres backend."""

    pyproject = parse_pyproject()
    log = init_logger(__file__)

    def __init__(self, use_offline: bool = True):
        """Start Spotify API client to query tag data."""
        self.client: Spotify = None
        self._config: SpotifyApiConfig = load_spotify_config()
        if not use_offline:
            self.connect()

    def connect(self) -> bool:
        """Setup and test if Spotipy OAut2 client successfully connected."""
        try:
            if isinstance(self._config, SpotifyApiConfig):
                client_credentials_manager = SpotifyClientCredentials(
                    client_id=self._config.client_id,
                    client_secret=self._config.client_secret,
                    requests_timeout=self._config.timeout,
                )
                self.client = Spotify(
                    client_credentials_manager=client_credentials_manager,
                    retries=0,
                )
                # test API call, if no exception, client is connected
                if self.client.categories():
                    self.log.info(f"{Spotify.__name__} client connected")
                    return True
        except (SpotifyOauthError, SpotifyException):
            self.log.exception("unable to connect, check settings")
        return False

    def is_connected(self) -> bool:
        """Verify if client is connected."""
        return isinstance(self.client, Spotify)

    def save_response(self, filename: str, results: Dict) -> None:
        """Save full API response for debugging and validation."""
        if DEBUG:
            try:
                path = Path(API_PATH, f"{filename}.json")
                if isinstance(results, (Dict, List)):
                    with open(file=path, mode="w", encoding="utf-8") as fp:
                        formatted_str = json.dumps(results, indent=2, sort_keys=False, ensure_ascii=False, default=str)
                        fp.write(formatted_str)
                else:
                    self.log.error(f"invalid type: {type(results)}")
            except (ValueError, json.JSONDecodeError):
                self.log.exception(f"{type(results)} '{path.name}'")

    def find_closest_match(self, keyword: str, items: Dict) -> Tuple[str, float]:
        """Use fuzzy pattern matching (case insensitive) to find closest match."""
        matches = []
        results: Dict[str, Any] = {
            "keyword": keyword,
            "hit_name": None,
            "hit_id": None,
            "confidence": 0.0,
            "candidates": [],
            "count": 0,
        }
        for item in items:
            fuzz_ratio = round(fuzz.ratio(s1=keyword, s2=item["name"]), 4)
            matches.append(fuzz_ratio)
            results["candidates"].append(
                {
                    "item_name": item["name"],
                    "item_id": item.get("id"),
                    "fuzz_ratio": fuzz_ratio,
                }
            )
            # skip scanning once identical match is found
            if fuzz_ratio == 100.0:
                break
        # get index to list element with highest similarity match
        max_idx = matches.index(max(matches))
        results["hit_name"] = items[max_idx]["name"]
        results["hit_id"] = items[max_idx]["id"]
        results["confidence"] = results["candidates"][max_idx]["fuzz_ratio"]
        results["count"] = len(results["candidates"])
        self.save_response(filename="find_closest_match", results=results)
        return results["hit_id"], results["confidence"]

    def get_artist_id(self, artist_name: str) -> str:
        """Spotify API to lookup artist id by keyword."""
        if self.is_connected():
            params = {"artist": artist_name}
            query = urllib.parse.urlencode(params)
            results = self.client.search(q=query, type="artist", market="US", limit=20)
            self.save_response(filename="get_artist_id", results=results)
            items = results["artists"].get("items", [])
            artist_id, confidence = self.find_closest_match(keyword=artist_name, items=items)
            self.log.info(f"artist: {artist_name} (id: {artist_id}) {confidence:0.2f}%")
        else:
            artist_id = OFFLINE_ARTIST_IDS.get(artist_name, "not_found")
            self.log.error(f"offline: {artist_name} artist_id: {artist_id}")
        return artist_id

    def get_album_id(self, artist_id: str, album_title: str) -> str:
        """Spotify API to lookup album id by keyword."""
        if self.is_connected():
            results = self.client.artist_albums(artist_id=artist_id, limit=50)
            self.save_response(filename="get_album_id", results=results)
            items = results.get("items", [])
            album_id, confidence = self.find_closest_match(keyword=album_title, items=items)
            self.log.info(f"album: {album_title} (id: {album_id}) {confidence:0.2f}%")
        else:
            album_id = OFFLINE_ALBUM_IDS.get(album_title, "not_found")
            self.log.error(f"offline {album_title} (id: {album_id})")
        return album_id

    def get_track_id(self, artist_name: str, album_title: str, track_title: str) -> str:
        """Spotify API to lookup track/song id by keywords.

        https://developer.spotify.com/documentation/web-api/reference/#/operations/search
        """
        if self.is_connected():
            params = {"artist": artist_name, "album": album_title, "track": track_title}
            query = urllib.parse.urlencode(params)
            results = self.client.search(q=query, type="track", market="US", limit=50)
            self.save_response(filename="get_track_id", results=results)
            items = results["tracks"].get("items", [])
            track_id, confidence = self.find_closest_match(keyword=track_title, items=items)
            self.log.info(f"track: {track_title} (id: {track_id}) {confidence:0.2f}%")
        else:
            track_id = OFFLINE_TRACK_IDS.get(track_title, "not_found")
            self.log.error(f"offline {artist_name} {track_title} (id: {track_id})")
        return track_id
