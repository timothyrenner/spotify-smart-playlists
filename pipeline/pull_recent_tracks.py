import typer
from prefect import flow, get_run_logger, task
from typing import Optional
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
from spotify_smart_playlists.extract import pull_recent_tracks
import spotipy
import duckdb
from duckdb import DuckDBPyConnection
from datetime import datetime
import polars as pl
import pytz


@task(name="Check if play_history table exists.")
def play_history_table_exists_exists(database: DuckDBPyConnection) -> bool:
    logger = get_run_logger()
    logger.info("Determining if the play_history table exists.")
    tables = {x[0] for x in database.sql("SHOW TABLES").fetchall()}
    return "play_history" in tables


@task(name="Get latest played_at value")
def get_latest_played_at_task(database: DuckDBPyConnection) -> datetime:
    logger = get_run_logger()
    logger.info("Getting latest played_at value from play_history table.")
    return (
        database.sql("SELECT MAX(played_at) FROM play_history").fetchone()[0]
        # Duckdb does not store time zones.
        # Spotify's api returns datetimes in UTC, which is what we save in the
        # db, so the tz needs to be added.
        .replace(tzinfo=pytz.UTC)
    )


@task(name="Get recently played tracks")
def pull_recent_tracks_task(
    spotify: spotipy.Spotify, latest_played_at: datetime | None
) -> pl.DataFrame | None:
    logger = get_run_logger()
    return pull_recent_tracks(spotify, latest_played_at, logger=logger)


@task(name="Save new track plays to database.")
def save_recent_track_plays_to_database(
    database: DuckDBPyConnection,
    recent_tracks: pl.DataFrame,
    history_exists: bool = True,
):
    logger = get_run_logger()
    logger.info(f"Saving {recent_tracks.shape[0]} to database.")
    if history_exists:
        database.execute(
            "INSERT INTO play_history SELECT * FROM recent_tracks"
        )
    else:
        database.execute(
            "CREATE TABLE play_history AS SELECT * FROM recent_tracks"
        )


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

    play_history_exists = play_history_table_exists_exists(database)
    latest_played_at: datetime | None = None
    if play_history_exists:
        latest_played_at = get_latest_played_at_task(database)

    recent_tracks = pull_recent_tracks_task(spotify, latest_played_at)

    if recent_tracks is not None:
        save_recent_track_plays_to_database(
            database, recent_tracks, history_exists=play_history_exists
        )
    else:
        logger.info("No new tracks to save. All done.")


if __name__ == "__main__":
    typer.run(update_recent_tracks)
