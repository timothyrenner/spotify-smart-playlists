import os

from dotenv import load_dotenv, find_dotenv
from spotify_smart_playlists.helpers import DuckDBEncryptedCacheHandler
from spotipy.oauth2 import SpotifyOAuth
from dataclasses import dataclass


@dataclass
class SpotifyCredentials:
    client_id: str
    client_secret: str
    redirect_uri: str


def get_spotify_credentials_from_environment() -> SpotifyCredentials:
    load_dotenv(find_dotenv())
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")

    if client_id is None:
        raise ValueError("Missing SPOTIFY_CLIENT_ID in env or .env")
    if client_secret is None:
        raise ValueError("Missing SPOTIFY_CLIENT_SECRET in env or .env")
    if redirect_uri is None:
        raise ValueError("Missing SPOTIFY_REDIRECT_URI in env or .env")
    return SpotifyCredentials(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )


def spotify_auth(
    database: str,
    credentials: SpotifyCredentials | None,
    cache_key: str | None = None,
) -> SpotifyOAuth:
    if credentials is None:
        credentials = get_spotify_credentials_from_environment()
    scope = " ".join(
        [
            "playlist-read-private",
            "user-read-recently-played",
            "playlist-modify-public",
            "playlist-modify-private",
            "user-library-read",
        ]
    )
    client_credentials_manager = SpotifyOAuth(
        client_id=credentials.client_id,
        client_secret=credentials.client_secret,
        redirect_uri=credentials.redirect_uri,
        scope=scope,
        cache_handler=DuckDBEncryptedCacheHandler(
            database, cache_fernet_key=cache_key
        ),
    )
    return client_credentials_manager
