import typer
import spotipy

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth


def main():
    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    logger.info("Fetching results.")
    results = spotify.current_user_recently_played()

    for rt in results["items"]:
        print(rt["track"]["id"], rt["track"]["name"])


if __name__ == "__main__":
    typer.run(main)