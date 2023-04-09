import typer
from prefect import task, flow, get_run_logger
from spotify_smart_playlists.extract import pull_library_tracks
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
import spotipy
import polars as pl
from typing import Tuple, Optional
import duckdb
from duckdb import DuckDBPyConnection


@task(name="Pull library tracks")
def pull_library_tracks_task(
    spotify: spotipy.Spotify,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger = get_run_logger()
    logger.info("Pulling tracks and track artists.")
    return pull_library_tracks(spotify, logger=logger)


@task(name="Save library tracks")
def save_library_tracks_task(
    database: DuckDBPyConnection, library_tracks_frame: pl.DataFrame
):
    logger = get_run_logger()
    logger.info(
        f"Saving {library_tracks_frame.shape[0]} "
        "library tracks to the database."
    )
    database.execute(
        """
        CREATE OR REPLACE TABLE library_tracks AS
        SELECT * FROM library_tracks_frame
        """
    )


@task(name="Save track artists")
def save_track_artists_task(
    database: DuckDBPyConnection, track_artists_frame: pl.DataFrame
):
    logger = get_run_logger()
    logger.info(
        f"Saving {track_artists_frame.shape[0]} track artists to the database."
    )
    database.execute(
        """
        CREATE OR REPLACE TABLE track_artists AS
        SELECT * FROM track_artists_frame
        """
    )


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

    save_library_tracks_task(database, library_tracks)
    save_track_artists_task(database, track_artists)

    logger.info("Done updating library.")


if __name__ == "__main__":
    typer.run(pull_library)
