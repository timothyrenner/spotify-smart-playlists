import typer
from prefect import task, flow, get_run_logger
from spotify_smart_playlists.extract import pull_library_tracks
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
from database import save_to_database
import spotipy
import polars as pl
from typing import Tuple, Optional
import duckdb


@task(name="Pull library tracks")
def pull_library_tracks_task(
    spotify: spotipy.Spotify,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger = get_run_logger()
    logger.info("Pulling tracks and track artists.")
    return pull_library_tracks(spotify, logger=logger)


@flow(name="Pull library")
def pull_library(
    database_file: str = "spotify.db",
    cache_fernet_key: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    redirect_uri: Optional[str] = None,
):
    logger = get_run_logger()
    credentials: SpotifyCredentials | None = None
    if client_id and client_secret and redirect_uri:
        logger.info("Explicitly initializing credentials.")
        credentials = SpotifyCredentials(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
        )
    else:
        logger.info("Insufficient explicit credentials, using environment.")

    spotify = spotipy.Spotify(
        client_credentials_manager=spotify_auth(
            database_file, credentials=credentials, cache_key=cache_fernet_key
        )
    )

    logger.info("Connected to Spotify.")
    library_tracks, track_artists = pull_library_tracks_task(spotify)
    database = duckdb.connect(database_file)

    save_to_database(
        database=database,
        table="library_tracks",
        data_frame=library_tracks,
        create_or_replace=True,
    )
    save_to_database(
        database=database,
        table="track_artists",
        data_frame=track_artists,
        create_or_replace=True,
    )

    database.close()
    logger.info("Done updating library.")


if __name__ == "__main__":
    typer.run(pull_library)
