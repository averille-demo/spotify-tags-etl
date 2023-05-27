"""Spotify API client to lookup music tag data.

https://github.com/spotipy-dev/spotipy-examples/blob/main/showcases.ipynb
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

from media_etl.sql.offline_ids import OFFLINE_ALBUM_IDS, OFFLINE_ARTIST_IDS, OFFLINE_TRACK_IDS
from media_etl.util.data_models import SpotifyAudioFeatureModel, SpotifyFavoriteModel
from media_etl.util.logger import init_logger
from media_etl.util.settings import (
    API_PATH,
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
                    requests_timeout=self._config.timeout,
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
            try:
                if isinstance(results, (Dict, List)):
                    path = Path(API_PATH, f"{filename}.json")
                    path.write_text(
                        data=json.dumps(results, indent=2, sort_keys=False, ensure_ascii=False, default=str),
                        encoding="utf-8",
                    )
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
            results = self.client.search(q=query, type="artist", market=self._config.market, limit=20)
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
            results = self.client.artist_albums(artist_id=artist_id, limit=self._config.limit)
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
            results = self.client.search(q=query, type="track", market=self._config.market, limit=self._config.limit)
            self.save_response(filename="get_track_id", results=results)
            items = results["tracks"].get("items", [])
            track_id, confidence = self.find_closest_match(keyword=track_title, items=items)
            self.log.info(f"track: {track_title} (id: {track_id}) {confidence:0.2f}%")
        else:
            track_id = OFFLINE_TRACK_IDS.get(track_title, "not_found")
            self.log.error(f"offline {artist_name} {track_title} (id: {track_id})")
        return track_id

    def parse_favorite(self, item: Dict[str, Any]) -> Optional[SpotifyFavoriteModel]:
        """Extract desired user 'liked' track data from API response.

        scope = "user-library-read"
        https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
        """
        model = None
        try:
            # convert milliseconds (since Epoch) to '%H:%M:%S.%f' 24-hour iso format
            dt = pendulum.from_format(
                string=str(item["track"]["duration_ms"]),
                fmt="x",
                tz="UTC",
            )
            model = SpotifyFavoriteModel(
                id=item["track"]["id"],
                artist=item["track"]["album"]["artists"][0]["name"],
                album=item["track"]["album"]["name"],
                track=item["track"]["name"],
                track_number=item["track"]["track_number"],
                duration=pendulum.time(
                    hour=dt.hour,
                    minute=dt.minute,
                    second=dt.second,
                    microsecond=dt.microsecond,
                ),
                release_date=pendulum.parse(text=item["track"]["album"]["release_date"]),
                popularity=item["track"]["popularity"],
                date_added=pendulum.parse(text=item["added_at"]),
                url=item["track"]["external_urls"]["spotify"],
            )
        except (KeyError, ValidationError, ParserError) as ex:
            self.log.exception(ex)
        return model

    def get_audio_features(
        self,
        track_ids: List[str],
    ) -> Dict[str, SpotifyAudioFeatureModel]:
        """Get audio track features."""
        features: Dict[str, SpotifyAudioFeatureModel] = {}
        if isinstance(track_ids, List) and len(track_ids) > 0:
            results = self.client.audio_features(tracks=track_ids)
            self.save_response(filename="audio_features", results=results)
            for track in results:
                track_id = track["id"]
                # remove unwanted fields
                for key in ["id", "type", "track_href", "uri"]:
                    if key in track.keys():
                        del track[key]
                try:
                    # pass all kwargs to schema
                    features[track_id] = SpotifyAudioFeatureModel(**track)
                except ValidationError:
                    self.log.exception("audio_features")
        return features

    def extract_favorite_tracks(self) -> Dict[str, Any]:
        """Get favorite tracks (liked songs in library).

        scope = "user-library-read"
        https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
        https://github.com/spotipy-dev/spotipy-examples/blob/main/showcases.ipynb
        """
        data = {
            "extract_date": pendulum.now(tz="America/Los_Angeles").to_iso8601_string(),
            "total": 0,
            "records": [],
        }
        offset = 0
        page = 0
        more_pages = True
        try:
            while more_pages:
                page += 1
                results = self.client.current_user_saved_tracks(
                    limit=self._config.limit,
                    offset=offset,
                    market=self._config.market,
                )
                self.save_response(filename="favorite_tracks", results=results)
                time.sleep(1)
                print(f"{page=:02d}\t {offset=:03d}")
                # update loop counters
                offset += self._config.limit
                if not results["next"]:
                    more_pages = False
                items = results.get("items", [])
                track_ids = [item["track"]["id"] for item in items]
                features: Dict[str, SpotifyAudioFeatureModel] = self.get_audio_features(track_ids)
                for item in items:
                    track = self.parse_favorite(item=item)
                    if isinstance(track, SpotifyFavoriteModel):
                        row = track.dict()
                        if track.id in features:
                            row.update(features[track.id].dict())
                        data["records"].append(row)
            data["total"] = results["total"]
            if len(data["records"]) != data["total"]:
                self.log.error(f"count mismatch: {len(data['records'])} records != expected: {data['total']}")
            self.save_response(filename="favorite_tracks_records", results=data)
        except SpotifyException:
            self.log.exception("favorite_tracks")
        return data


if __name__ == "__main__":
    client = SpotifyClient(use_offline=False)
    client.extract_favorite_tracks()
