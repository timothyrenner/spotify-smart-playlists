from spotipy import cache_handler
import typer
import os
import spotipy

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from spotipy.oauth2 import SpotifyOAuth
from github_file_cache_handler import CacheFileGithubHandler

logger.info("Loading dotenv file.")
load_dotenv(find_dotenv())

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

if not SPOTIFY_CLIENT_ID:
    msg = "Missing SPOTIFY_CLIENT_ID in env or .env."
    logger.fatal(msg)
    raise ValueError(msg)

if not SPOTIFY_CLIENT_SECRET:
    msg = "Missing SPOTIFY_CLIENT_SECRET in env or .env."
    logger.fatal(msg)
    raise ValueError(msg)

if not SPOTIFY_REDIRECT_URI:
    msg = "Missing SPOTIFY_REDIRECT_URI in env or .env."
    logger.fatal(msg)
    raise ValueError(msg)


def main():
    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(
        client_credentials_manager=SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            scope="playlist-read-private",
            cache_handler=CacheFileGithubHandler(),
        )
    )

    logger.info("Fetching results.")
    offset = 0
    results = spotify.current_user_playlists()

    items = results["items"]
    for item in items:
        print(item["name"], item["tracks"])


if __name__ == "__main__":
    typer.run(main)