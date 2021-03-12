import typer
import os
import spotipy

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from spotipy.oauth2 import SpotifyOAuth
from dateutil.relativedelta import relativedelta
from datetime import datetime

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
            scope="user-read-recently-played",
        )
    )

    one_week_ago = datetime.utcnow() - relativedelta(weeks=1)
    logger.info(f"Reading tracks after {one_week_ago.strftime('%Y-%m-%d')}.")

    logger.info("Fetching results.")
    results = spotify.current_user_recently_played(
        after=int(one_week_ago.timestamp())
    )
    print(results)


if __name__ == "__main__":
    typer.run(main)