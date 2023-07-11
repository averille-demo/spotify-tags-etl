"""Spotify API client to lookup music tag data.

https://github.com/spotipy-dev/spotipy-examples/blob/main/showcases.ipynb
https://developer.spotify.com/documentation/web-api/concepts/track-relinking
"""
import json
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlencode

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
from media_etl.util.logger import init_logger, relative_size
from media_etl.util.settings import (
    API_PATH,
    DATA_PATH,
    DEBUG,
    ENABLE_API,
    PROJECT_ROOT,
    SpotifyApiConfig,
    load_spotify_config,
    parse_pyproject,
)


class SpotifyClient:
    """Class to lookup/add Spotify media tags to Postgres backend."""

    pyproject = parse_pyproject()
    log = init_logger(__file__)

    def __init__(self):
        """Start Spotify API client to query tag data."""
        self.client: Spotify = None
        self._config: SpotifyApiConfig = load_spotify_config(environment="dev")
        if ENABLE_API:
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

    def save_response(self, filename: str, results: Union[List, Dict]) -> None:
        """Save full API response for debugging and validation.

        Args:
            filename (str): output basename (without '.json' extension)
            results (Dict): raw response payload saved to file
                * readable formatted output
                * not newline delimited
        """
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

    def save_records(self, models: List[SQLModel]) -> None:
        """Convert list of SQLModels to newline delimited JSON text file.

        Args:
            models (List): export SQLModels as newline delimited JSON text file
            (leverages pydantic json_encoders to format date, time, datetime strings)
        """
        try:
            if isinstance(models, List) and len(models) > 0:
                path = Path(DATA_PATH, f"{models[0].__tablename__}_records.json")
                # create newline delimited JSON
                with open(file=path, mode="w", encoding="utf-8") as fp:
                    for model in models:
                        fp.write(f"{model.json()}\n")
                print(f"saved: {relative_size(path)}")
        except (ValueError, json.JSONDecodeError):
            self.log.exception(f"{model.to_string()} '{path.name}'")

    def pause(self):
        """Don't bombard API endpoint(s) with requests."""
        time.sleep(self._config.api_timeout)

    def find_closest_match(self, keyword: str, dtype: str, items: List[Any]) -> Tuple[str, float]:
        """Use fuzzy pattern matching (case insensitive) to find closest match.

        For example, track: 'II. Allegro scherzando' from Beethoven Symphony No.8 in F Major (Op.93)
            may have many permutations depending on performing orchestra, date of recording, is compilation, etc.

        Args:
            keyword (str): query parameter as search criteria (artist, album, or track name)
                Value contained within MP3 tags (local files) may not match Spotify's naming convention
            dtype (str): data type to extract: artist, album, etc.
            items (Dict): API response return values to search by artist/album/track 'name'

        Returns:
            Tuple: closest matching track_id (str) and confidence (float 0.0 < 1.0)
        """
        matches = []
        results: Dict[str, Any] = {
            "keyword": keyword,
            "dtype": dtype,
            "hit_name": None,
            "hit_id": None,
            "confidence": 0.0,
            "count": 0,
            "candidates": [],
        }
        for item in items:
            fuzz_ratio = round(fuzz.ratio(s1=keyword, s2=item["name"]), 4)
            matches.append(fuzz_ratio)
            results["candidates"].append(
                {
                    "name": item["name"],
                    "id": item.get("id"),
                    "fuzz_ratio": fuzz_ratio,
                }
            )
            # skip scanning once identical match is found
            if fuzz_ratio == 100.0:
                break
        # get index to list element with highest similarity match
        if matches:
            max_idx = matches.index(max(matches))
            results["hit_name"] = items[max_idx]["name"]
            results["hit_id"] = items[max_idx]["id"]
            results["confidence"] = results["candidates"][max_idx]["fuzz_ratio"]
        results["count"] = len(results["candidates"])
        if results["confidence"] < 75.0:
            self.save_response(filename=f"closest_match-{dtype}-{keyword}", results=results)
        return results["hit_id"], results["confidence"]

    @staticmethod
    def normalize(text: str) -> str:
        """Convert non-english unicode characters to ASCII english alphabet.

        The Spotify API is more precise if using non-unicode chars in search query
        Args:
            text (str): string to convert (example: 'Björk' to 'Bjork')

        Returns:
            normalized string of ASCII characters
        """
        normalized = unicodedata.normalize("NFD", text)
        return "".join([c for c in normalized if not unicodedata.combining(c)])

    def query_all(self, params: Dict, dtype: str) -> List[Any]:
        """Paginate through entire result set.

        Args:
            params (Dict): string to convert
            dtype (str): data type to extract: artist, album, etc.

        Returns:
            items (List): of all available items from all API requests as single list
        """
        items = []
        offset = 0
        page = 0
        more_pages = True
        # https://github.com/spotipy-dev/spotipy/blob/d31969108d462c544f41aba4581a0d84a1e75d6f/spotipy/client.py#L572
        if dtype not in ["artist", "album", "track", "playlist", "show", "episode"]:
            raise ValueError(dtype)

        while more_pages:
            page += 1
            try:
                # print(f"{dtype=}\t {page=:02d}\t {offset=:03d}")
                batch = self.client.search(
                    q=urlencode(query=params),
                    type=dtype,
                    market=self._config.market,
                    limit=self._config.api_limit,
                    offset=offset,
                )
                # append 's' to make dtype plural
                batch_items = batch[f"{dtype}s"].get("items", [])
                if batch_items:
                    # self.save_response(filename=f"{dtype}_{page=:02d}", results=batch)
                    items.extend(batch_items)
                    # update loop counters
                    offset += self._config.api_limit
                    self.pause()
                else:
                    more_pages = False
            except SpotifyException:
                self.log.exception(f"{params=} {dtype=}")

        # once all items are extracted, return result set
        self.save_response(filename=f"query_all_{dtype}s", results=items)
        return items

    def get_artist_id(self, artist_name: str) -> str:
        """Query Spotify API to lookup artist_id by keyword.

        https://developer.spotify.com/documentation/web-api/reference/search

        Args:
            artist_name (str): search criteria by artist name

        Returns:
            string: Spotify's unique identifier for artist (based on best fuzzy pattern match)
        """
        if self.is_connected():
            params = {"artist": self.normalize(artist_name)}
            items = self.query_all(params=params, dtype="artist")
            artist_id, confidence = self.find_closest_match(keyword=artist_name, dtype="artist", items=items)
            self.log.info(f"{artist_name=} {artist_id=} {confidence=:0.2f}%")
        else:
            artist_id = OFFLINE_ARTIST_IDS.get(artist_name, "not_found")
            self.log.info(f"{artist_name=} {artist_id=} ({ENABLE_API=})")
        return artist_id

    def get_album_id(self, artist_name: str, album_title: str) -> str:
        """Query Spotify API to lookup album_id by keywords.

        https://github.com/spotipy-dev/spotipy/blob/d31969108d462c544f41aba4581a0d84a1e75d6f/spotipy/client.py#L404

        Args:
            artist_name (str): search criteria by artist name
            album_title (str): keyword of album name as search criteria

        Returns:
            string: Spotify's unique identifier for album (based on fuzzy pattern matching with best confidence)
        """
        if self.is_connected():
            params = {"artist": self.normalize(artist_name)}
            items = self.query_all(params=params, dtype="album")
            album_id, confidence = self.find_closest_match(keyword=album_title, dtype="album", items=items)
            self.log.info(f"{album_title=} {album_id=} {confidence=:0.2f}%")
        else:
            album_id = OFFLINE_ALBUM_IDS.get(album_title, "not_found")
            self.log.info(f"{album_title=} {album_id=} ({ENABLE_API=})")
        return album_id

    def get_track_id(self, artist_name: str, album_title: str, track_title: str) -> str:
        """Spotify API to lookup track_id by keywords.

        https://developer.spotify.com/documentation/web-api/reference/#/operations/search

        Args:
            artist_name (str): keyword of artist name as search criteria
            album_title (str): keyword of album name as search criteria
            track_title (str): keyword of track/song name as search criteria

        Returns:
            string: Spotify's unique identifier for track (based on fuzzy pattern matching with best confidence)
        """
        if self.is_connected():
            params = {
                "artist": self.normalize(artist_name),
                "album": self.normalize(album_title),
                "track": self.normalize(track_title),
            }
            items = self.query_all(params=params, dtype="track")
            track_id, confidence = self.find_closest_match(keyword=track_title, dtype="track", items=items)
            self.log.info(f"{artist_name=} {track_title=} {track_id=} {confidence=:0.2f}%")
        else:
            track_id = OFFLINE_TRACK_IDS.get(track_title, "not_found")
            self.log.info(f"{artist_name=} {track_title=} {track_id=} ({ENABLE_API=})")
        return track_id

    def convert_duration(self, value: int) -> Optional[pendulum.Time]:
        """Convert milliseconds to time object.

        Args:
            value (int): integer track duration in milliseconds
            (example: 200158ms = 3 minutes, 20 seconds (or '00:03:20')

        Returns:
            pendulum.Time: UTC timezone aware time object later converted to '%H:%M:%S' 24-hour ISO format
        """
        parsed = None
        try:
            dt = pendulum.from_format(string=str(value), fmt="x", tz="UTC")
            parsed = pendulum.time(hour=dt.hour, minute=dt.minute, second=dt.second)
        except (ValueError, ParserError):
            self.log.exception(f"{value=}")
        return parsed

    def convert_release_date(self, string: str) -> Optional[pendulum.Date]:
        """Convert text to date object.

        Args:
            string (str): text with date when track was release by artist
            valid input formats: 'YYYY-MM-DD', 'YYYY-MM', or 'YYYY'

        Returns:
            pendulum.Date: UTC timezone aware date object
            if month or day is not provided in input, defaults to January 1st.
        """
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
        """Convert text in 'YYYY-MM-DDTHH:MM:SSZ' format to datetime object.

        Args:
            string (str): raw text with timestamp of when track was added to playlist

        Returns:
            pendulum.DateTime: UTC timezone aware datetime object
        """
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

        Args:
            item (Dict): nested API response for track item

        Returns:
            SpotifyFavoriteModel: custom formatted SQLModel object
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

    def query_audio_features(
        self,
        track_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Get audio track features.

        https://developer.spotify.com/documentation/web-api/reference/get-audio-features
        https://spotipy.readthedocs.io/en/2.22.1/?highlight=audio_features#spotipy.client.Spotify.audio_features

        Args:
            track_ids (List): spotify track ids, either from:
                priority: item["track"]["linked_from"]["id"])
                or if 'linked_from' does not exist: track['id']
            if track_ids > 50, API requests are batched in groups of 50

        Returns:
            SpotifyFavoriteModel: custom formatted SQLModel object
        """
        models: List = []
        if self.is_connected() and isinstance(track_ids, List) and len(track_ids) > 0:
            # partition track_ids into batches based on API limits
            for offset in tqdm(iterable=range(0, len(track_ids), self._config.api_limit), ascii=True):
                batch = track_ids[offset: offset + self._config.api_limit]  # fmt: skip
                audio_features = self.client.audio_features(tracks=batch)
                self.pause()
                for feature in audio_features:
                    try:
                        # cast integers to strings for key and mode attributes (later converted to Major/minor, etc.)
                        feature["key"] = str(feature["key"])
                        feature["mode"] = str(feature["mode"])
                        # pass all kwargs to schema, in this case, all Spotify API keys match pydantic model
                        model = SpotifyAudioFeatureModel(**feature)
                        model.extract_date = pendulum.now(tz="UTC")
                        models.append(model)
                    except ValidationError:
                        self.log.exception(f"{feature=}")
        self.save_records(models=models)
        return models

    def add_liked_song(self, model: SpotifyFavoriteModel) -> None:
        """Add track to current user's 'Liked Songs' playlist.

        scope: user-library-modify
        https://developer.spotify.com/documentation/web-api/reference/save-tracks-user
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_add

        Args:
            model (SpotifyFavoriteModel): track information to add to 'Liked Songs'
        """
        try:
            if self.is_connected():
                self.log.info(f"adding track to 'Like Songs' playlist: {model.to_string()}")
                # pass track URI to endpoint
                self.client.current_user_saved_tracks_add(tracks=[f"spotify:{model.type}:{model.track_id}"])
        except SpotifyException:
            self.log.exception(f"delete {model.to_string()}")

    def remove_liked_song(self, model: SpotifyFavoriteModel) -> None:
        """Remove track from current user's 'Liked Songs' playlist.

        scope: user-library-modify
        https://developer.spotify.com/documentation/web-api/reference/remove-tracks-user
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_delete

        Args:
            model (SpotifyFavoriteModel): track information to remove from 'Liked Songs'
            If linked_from is present, use linked_from.track.uri as pointer for object to remove, else track.uri.
        """
        try:
            if self.is_connected():
                self.log.info(f"removing track from 'Like Songs' playlist: {model.to_string()}")
                # pass track URI to endpoint
                self.client.current_user_saved_tracks_delete(tracks=[f"spotify:{model.type}:{model.track_id}"])
        except SpotifyException:
            self.log.exception(f"delete {model.to_string()}")

    def extract_favorite_tracks(self, item_limit: Optional[int] = None) -> List[str]:
        """Get track information from current user's 'Liked Songs' playlist.

        scope = "user-library-read"
        https://developer.spotify.com/documentation/web-api/reference/get-users-saved-tracks
        https://spotipy.readthedocs.io/en/2.22.1/#spotipy.client.Spotify.current_user_saved_tracks_delete

        Args:
            item_limit (int): subset of all liked songs

        Returns:
            List: of track_ids as strings
                if 'linked_from' is present:
                    track_id = item["track"]["linked_from"]["id"])
                else:
                    track_id = item["track"]["id"]
        """
        models = []
        track_ids = []
        if self.is_connected():
            # alternative to pagination, issue single request to find total number of tracks
            results = self.client.current_user_saved_tracks(limit=1, offset=0, market=self._config.market)
            total_tracks = results["total"]
            print(f"playlist: 'Liked Songs' contains ({total_tracks}) tracks")
            if isinstance(item_limit, int) and item_limit < total_tracks:
                print(f"extracting subset: {item_limit} of {total_tracks} available tracks")
                total_tracks = item_limit
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
