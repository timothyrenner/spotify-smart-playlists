import typer
from prefect import flow, get_run_logger, task
from typing import Optional
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
from spotify_smart_playlists.extract import pull_recent_tracks
from database import table_exists, save_to_database
import spotipy
import duckdb
from duckdb import DuckDBPyConnection
from datetime import datetime
import polars as pl


@task(name="Get latest played_at value")
def get_latest_played_at_task(database: DuckDBPyConnection) -> datetime:
    logger = get_run_logger()
    logger.info("Getting latest played_at value from play_history table.")
    return database.sql(
        "SELECT MAX(played_at) AT TIME ZONE 'UTC' FROM play_history"
    ).fetchone()[0]


@task(name="Get recently played tracks")
def pull_recent_tracks_task(
    spotify: spotipy.Spotify, latest_played_at: datetime | None
) -> pl.DataFrame | None:
    logger = get_run_logger()
    return pull_recent_tracks(spotify, latest_played_at, logger=logger)


@flow(name="Pull recent tracks")
def update_recent_tracks(
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

    logger.info("Connected to spotify.")

    database = duckdb.connect(database_file)
    logger.info("Connected to database.")

    play_history_exists = table_exists(database, "play_history")
    latest_played_at: datetime | None = None
    if play_history_exists:
        latest_played_at = get_latest_played_at_task(database)
        logger.info(f"Latest played_at: {latest_played_at}.")

    recent_tracks = pull_recent_tracks_task(spotify, latest_played_at)

    if recent_tracks is not None:
        save_to_database(
            database=database,
            table="play_history",
            data_frame=recent_tracks,
            # if play_history exists, append.
            create_or_replace=(not play_history_exists),
        )
    else:
        logger.info("No new tracks to save. All done.")
    database.close()


if __name__ == "__main__":
    typer.run(update_recent_tracks)
