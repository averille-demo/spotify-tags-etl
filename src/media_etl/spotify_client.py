"""Spotify API client to lookup music tag data.

https://github.com/spotipy-dev/spotipy-examples/blob/main/showcases.ipynb
https://developer.spotify.com/documentation/web-api/concepts/track-relinking
"""
import json
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pendulum
from pendulum.parsing.exceptions import ParserError
from pydantic import ValidationError
from rapidfuzz import fuzz
from spotipy import Spotify
from spotipy.exceptions import SpotifyException
from spotipy.oauth2 import SpotifyOAuth, SpotifyOauthError
from sqlmodel import SQLModel
from tqdm import tqdm

from media_etl.sql.models import SpotifyAudioFeatureModel, SpotifyFavoriteModel
from media_etl.sql.offline_ids import OFFLINE_ALBUM_IDS, OFFLINE_ARTIST_IDS, OFFLINE_TRACK_IDS
from media_etl.util.logger import get_relative_path, init_logger
from media_etl.util.settings import (
    API_PATH,
    DATA_PATH,
    DEBUG,
    PROJECT_ROOT,
    SpotifyApiConfig,
    load_spotify_config,
    parse_pyproject,
)


class SpotifyClient:
    """Class to lookup/add Spotify media tags to Postgres backend."""

    pyproject = parse_pyproject()
    log = init_logger(__file__)

    def __init__(self, use_offline: bool):
        """Start Spotify API client to query tag data."""
        self.client: Spotify = None
        self._config: SpotifyApiConfig = load_spotify_config(environment="dev")
        if not use_offline:
            self.connect()

    def connect(self) -> bool:
        """Setup and test if OAuth2 client is successfully connected."""
        try:
            if isinstance(self._config, SpotifyApiConfig):
                auth_manager = SpotifyOAuth(
                    client_id=self._config.client_id,
                    client_secret=self._config.client_secret,
                    redirect_uri=f"{self._config.redirect_uri}:{self._config.port}",
                    requests_timeout=2,
                    scope=",".join(self._config.scope),
                    cache_path=Path(PROJECT_ROOT, "config", ".cache"),
                )
                self.client = Spotify(
                    auth_manager=auth_manager,
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
            path = Path(API_PATH, f"{pendulum.now().to_date_string()}", f"{filename}.json")
            try:
                if isinstance(results, (Dict, List)):
                    if not path.parent.is_dir():
                        path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(
                        data=json.dumps(results, indent=2, sort_keys=False, ensure_ascii=False, default=str),
                        encoding="utf-8",
                    )
            except (ValueError, json.JSONDecodeError):
                self.log.exception(f"{type(results)} '{path.name}'")

    def save_records(self, models: List[SQLModel]):
        """Convert list of SQL models to newline delimited JSON."""
        try:
            if isinstance(models, List) and len(models) > 0:
                model = models[0]
                path = Path(DATA_PATH, f"{model.__tablename__}_records.json")
                # create newline delimited JSON
                with open(file=path, mode="w", encoding="utf-8") as fp:
                    for model in models:
                        fp.write(f"{model.json()}\n")
                print(f"saved: {get_relative_path(path)}'\t ({path.stat().st_size} bytes)")
        except (ValueError, json.JSONDecodeError):
            self.log.exception(f"{model.to_string()} '{path.name}'")

    def pause(self):
        """Don't bombard API endpoint(s) with requests."""
        time.sleep(self._config.api_timeout)

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
            results = self.client.search(
                q=query, type="artist", market=self._config.market, limit=self._config.api_limit
            )
            self.save_response(filename="get_artist_id", results=results)
            self.pause()
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
            results = self.client.artist_albums(artist_id=artist_id, limit=self._config.api_limit)
            self.save_response(filename="get_album_id", results=results)
            self.pause()
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
            results = self.client.search(
                q=query,
                type="track",
                market=self._config.market,
                limit=self._config.api_limit,
            )
            self.save_response(filename="get_track_id", results=results)
            self.pause()
            items = results["tracks"].get("items", [])
            track_id, confidence = self.find_closest_match(keyword=track_title, items=items)
            self.log.info(f"track: {track_title} (id: {track_id}) {confidence:0.2f}%")
        else:
            track_id = OFFLINE_TRACK_IDS.get(track_title, "not_found")
            self.log.error(f"offline {artist_name} {track_title} (id: {track_id})")
        return track_id

    def convert_duration(self, value: int) -> Optional[pendulum.Time]:
        """Convert milliseconds (integer since Epoch) to '%H:%M:%S' 24-hour ISO format."""
        parsed = None
        try:
            dt = pendulum.from_format(string=str(value), fmt="x", tz="UTC")
            parsed = pendulum.time(hour=dt.hour, minute=dt.minute, second=dt.second)
        except (ValueError, ParserError):
            self.log.exception(f"{value=}")
        return parsed

    def convert_release_date(self, string: str) -> Optional[pendulum.Date]:
        """Convert text to date object in 'YYYY-MM-DD' or 'YYYY' format."""
        parsed = None
        n_chars = len(string)
        try:
            match n_chars:
                # handle 'YYYY' format
                case 4:
                    parsed = pendulum.date(year=int(string), month=1, day=1)
                # handle 'YYYY-MM' format
                case 7:
                    year, month = string.split("-")
                    parsed = pendulum.date(year=int(year), month=int(month), day=1)
                # expected format
                case 10:
                    dt = pendulum.from_format(string=string, fmt="YYYY-MM-DD", tz="UTC")
                    parsed = pendulum.date(year=dt.year, month=dt.month, day=dt.day)
        except (ValueError, ParserError):
            self.log.exception(f"{string=}")
        return parsed

    def convert_added_at(self, string: str) -> Optional[pendulum.DateTime]:
        """Convert text in 'YYYY-MM-DDTHH:MM:SSZ' format to datetime object."""
        parsed = None
        try:
            string = f"{string.replace('T', ' ').replace('Z', '')}"
            parsed = pendulum.from_format(string=string, fmt="YYYY-MM-DD HH:mm:ss", tz="UTC")
        except (ValueError, ParserError):
            self.log.exception(f"{string=}")
        return parsed

    def parse_favorite(self, item: Dict[str, Any]) -> Optional[SpotifyFavoriteModel]:
        """Extract desired user liked song track data from nested API response.

        scope = "user-library-read"
        https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
        https://developer.spotify.com/documentation/web-api/concepts/track-relinking
        """
        model = None
        try:
            # See important note: removing track from playlist, operate on original track id found in linked_from object
            # https://developer.spotify.com/documentation/web-api/concepts/track-relinking
            if item["track"].get("linked_from"):
                track_id = item["track"]["linked_from"]["id"]
            else:
                track_id = item["track"]["id"]
            model = SpotifyFavoriteModel(
                type=item["track"]["type"],
                track_id=track_id,
                artist_name=item["track"]["album"]["artists"][0]["name"],
                album_name=item["track"]["album"]["name"],
                track_name=item["track"]["name"],
                track_number=item["track"]["track_number"],
                duration=self.convert_duration(value=item["track"]["duration_ms"]),
                release_date=self.convert_release_date(string=item["track"]["album"]["release_date"]),
                popularity=item["track"]["popularity"],
                added_at=self.convert_added_at(string=item["added_at"]),
                external_url=item["track"]["external_urls"]["spotify"],
                extract_date=pendulum.now(tz="UTC"),
            )
        except (KeyError, ValidationError, ParserError):
            self.log.exception(f"{item['track']['id']}")
        return model

    def get_audio_features(
        self,
        track_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Get audio track features, limited to 50 IDs per reqeust.

        https://developer.spotify.com/documentation/web-api/reference/get-audio-features
        https://spotipy.readthedocs.io/en/2.22.1/?highlight=audio_features#spotipy.client.Spotify.audio_features
        """
        models: List[Dict[str, Any]] = []
        if isinstance(track_ids, List) and len(track_ids) > 0:
            # partition track_ids into batches based on API limits
            for offset in tqdm(iterable=range(0, len(track_ids), self._config.api_limit), ascii=True):
                batch = track_ids[offset: offset + self._config.api_limit]  # fmt: skip
                results = self.client.audio_features(tracks=batch)
                self.pause()
                for item in results:
                    try:
                        # cast integers to strings for key and mode attributes (later converted to Major/minor, etc.)
                        item["key"] = str(item["key"])
                        item["mode"] = str(item["mode"])
                        # pass all kwargs to schema, in this case, all Spotify API keys match pydantic model
                        model = SpotifyAudioFeatureModel(**item)
                        model.extract_date = pendulum.now(tz="UTC")
                        models.append(model)
                    except ValidationError:
                        self.log.exception(f"{item=}")
        self.save_records(models=models)
        return models

    def add_liked_song(self, model: SpotifyFavoriteModel):
        """Add track to current user's 'Liked Songs' playlist.

        scope: user-library-modify
        https://developer.spotify.com/documentation/web-api/reference/save-tracks-user
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_add
        """
        try:
            self.log.info(f"adding track to 'Like Songs' playlist: {model.json()}")
            self.client.current_user_saved_tracks_add(tracks=[f"spotify:{model.type}:{model.track_id}"])
        except SpotifyException:
            self.log.exception(f"delete {model.to_string()}")

    def remove_liked_song(self, model: SpotifyFavoriteModel):
        """Remove duplicates from current user's 'Liked Songs' playlist.

        Spotify allows users to save duplicate tracks to 'Liked Songs' playlist.
        If linked_from is present, use linked_from.uri as pointer for object to remove, else track.uri.
        scope: user-library-modify
        https://developer.spotify.com/documentation/web-api/reference/remove-tracks-user
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_delete
        """
        try:
            self.log.info(f"removing track from 'Like Songs' playlist: {model.json()}")
            self.client.current_user_saved_tracks_delete(tracks=[f"spotify:{model.type}:{model.track_id}"])
        except SpotifyException:
            self.log.exception(f"delete {model.to_string()}")

    def extract_favorite_tracks(self) -> List[str]:
        """Get favorite tracks (liked songs in library).

        scope = "user-library-read"
        https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_delete
        """
        models = []
        track_ids = []
        # alternative to pagination, issue single request to find total number of favorites
        results = self.client.current_user_saved_tracks(limit=1, offset=0, market=self._config.market)
        total_tracks = results["total"]
        # total_tracks = 100  # for debugging
        print(f"playlist: 'Liked Songs' contains ({total_tracks}) tracks")
        for offset in tqdm(iterable=range(0, total_tracks, self._config.api_limit)):
            try:
                results = self.client.current_user_saved_tracks(
                    limit=self._config.api_limit,
                    offset=offset,
                    market=self._config.market,
                )
                self.pause()
                items = results.get("items", [])
                for item in items:
                    model = self.parse_favorite(item=item)
                    if isinstance(model, SpotifyFavoriteModel):
                        if model.track_id not in track_ids:
                            track_ids.append(model.track_id)
                        models.append(model)
            except SpotifyException:
                self.log.exception("favorite_tracks")
        if len(track_ids) != len(models):
            self.log.error(f"counts do not match: {len(track_ids)} != {len(models)} models")
        self.save_records(models=models)
        return track_ids


if __name__ == "__main__":
    client = SpotifyClient(use_offline=False)
    client.get_audio_features(track_ids=client.extract_favorite_tracks())
