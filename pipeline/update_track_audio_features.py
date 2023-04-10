import typer
from prefect import flow, get_run_logger, task
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
from spotify_smart_playlists.extract import pull_audio_features
from database import table_exists, save_to_database
import spotipy
import duckdb
from duckdb import DuckDBPyConnection
import polars as pl
from typing import Optional, List


@task(name="Get tracks without audio features")
def get_tracks_without_audio_features_task(
    database: DuckDBPyConnection, track_audio_features_exists: bool
) -> List[str]:
    if track_audio_features_exists:
        tracks_without_audio_features = database.query(
            """
            SELECT lt.track_id
            FROM library_tracks AS lt
            LEFT JOIN track_audio_features AS taf
                ON lt.track_id = taf.track_id
            WHERE
                taf.track_id IS NULL
            """
        ).fetchall()
    else:
        tracks_without_audio_features = database.query(
            "SELECT DISTINCT track_id FROM library_tracks"
        ).fetchall()
    return [t[0] for t in tracks_without_audio_features]


@task(name="Pull audio features")
def pull_audio_features_task(
    spotify: spotipy.Spotify, track_audio_features_to_pull: List[str]
) -> pl.DataFrame:
    logger = get_run_logger()
    return pull_audio_features(
        spotify, track_audio_features_to_pull, logger=logger
    )


@flow(name="Update track audio features")
def update_track_audio_features(
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

    database = duckdb.connect(database_file)
    track_audio_features_exists = table_exists(
        database, "track_audio_features"
    )

    track_ids_without_audio_features = get_tracks_without_audio_features_task(
        database, track_audio_features_exists
    )

    track_audio_features = pull_audio_features_task(
        spotify, track_ids_without_audio_features
    )

    save_to_database(
        database=database,
        table="track_audio_features",
        data_frame=track_audio_features,
        create_or_replace=(not track_audio_features_exists),
    )


if __name__ == "__main__":
    typer.run(update_track_audio_features)
