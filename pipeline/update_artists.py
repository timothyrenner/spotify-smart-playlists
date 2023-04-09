import typer
from prefect import flow, get_run_logger, task
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
from spotify_smart_playlists.extract import pull_artists
from database import table_exists, save_to_database
import spotipy
import duckdb
from duckdb import DuckDBPyConnection
import polars as pl
from typing import Optional, List, Tuple


@task(name="Determine missing artists")
def determine_missing_artists(
    database: DuckDBPyConnection, artists_table_exists: bool
) -> List[str]:
    logger = get_run_logger()
    logger.info(
        "Determining which artists are missing from the artists table."
    )
    if artists_table_exists:
        missing_artists = database.sql(
            """
            SELECT DISTINCT artist_id
            FROM track_artists
            LEFT JOIN artists
                ON track_artists.artist_id = artists.id
            WHERE
                artists.id IS NULL
            """
        ).fetchall()
    else:
        logger.info(
            "artists table does not exist, pulling all track artist ids."
        )
        missing_artists = database.sql(
            """ SELECT DISTINCT artist_id FROM track_artists
            """
        ).fetchall()
    return [a[0] for a in missing_artists]


@task(name="Pull missing artists")
def pull_artists_task(
    spotify: spotipy.Spotify, artists_to_pull: List[str]
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger = get_run_logger()
    return pull_artists(spotify, artists_to_pull, logger=logger)


@flow(name="Update artists")
def update_artists(
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
    artists_table_exists = table_exists(database, "artists")
    artist_genres_table_exists = table_exists(database, "artist_genres")
    missing_artists = determine_missing_artists(database, artists_table_exists)
    if missing_artists:
        new_artists_frame, new_artist_genres_frame = pull_artists_task(
            spotify, missing_artists
        )
        save_to_database(
            database=database,
            table="artists",
            data_frame=new_artists_frame,
            create_or_replace=(not artists_table_exists),
        )
        save_to_database(
            database=database,
            table="artist_genres",
            data_frame=new_artist_genres_frame,
            create_or_replace=(not artist_genres_table_exists),
        )
    else:
        logger.info("No new artists to pull.")


if __name__ == "__main__":
    typer.run(update_artists)
