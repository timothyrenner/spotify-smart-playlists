import typer
from prefect import task, flow, get_run_logger
import duckdb
import polars as pl
from duckdb import DuckDBPyConnection
from typing import Optional, List
from spotify_smart_playlists.playlists import (
    get_playlist_from_spotify,
    create_playlist_on_spotify,
    load_playlist_to_spotify,
    get_tracks_for_playlist,
    get_recommended_tracks,
)
from spotify_smart_playlists.helpers import SpotifyCredentials, spotify_auth
import spotipy


@task(name="Get root playlist names")
def get_root_playlist_names_task(database: DuckDBPyConnection) -> List[str]:
    logger = get_run_logger()
    logger.info("Getting root playlist names.")
    return (
        database.sql("SELECT DISTINCT name FROM root_playlists")
        .pl()
        .get_column("name")
        .to_list()
    )


@task(name="Get tracks for playlist")
def get_tracks_for_playlist_task(
    database: DuckDBPyConnection, playlist_name: str
) -> List[str]:
    logger = get_run_logger()
    logger.info(f"Getting tracks for playlist {playlist_name}.")
    return get_tracks_for_playlist(database, playlist_name)


@task(name="Get recommended tracks from Spotify")
def get_recommended_tracks_task(
    spotify: spotipy.Spotify, playlist_tracks: List[str], tracks_to_pull: int
) -> List[str]:
    logger = get_run_logger()
    return get_recommended_tracks(
        spotify, playlist_tracks, tracks_to_pull, logger=logger
    )


@task(name="Get playlist from Spotify")
def get_playlist_from_spotify_task(
    spotify: spotipy.Spotify, playlist_name: str
) -> Optional[str]:
    logger = get_run_logger()
    return get_playlist_from_spotify(spotify, playlist_name, logger=logger)


@task(name="Create playlist on Spotify")
def create_playlist_on_spotify_task(
    spotify: spotipy.Spotify, playlist_name: str
) -> str:
    logger = get_run_logger()
    return create_playlist_on_spotify(spotify, playlist_name, logger=logger)


@task(name="Load playlist to Spotify")
def load_playlist_to_spotify_task(
    spotify: spotipy.Spotify, playlist_id: str, playlist_tracks: List[str]
):
    logger = get_run_logger()
    return load_playlist_to_spotify(
        spotify, playlist_id, playlist_tracks, logger=logger
    )


@task(name="Validate playlist tracks")
def validate_playlist_tracks_task(
    database: DuckDBPyConnection, playlist_tracks: List[str]
):
    logger = get_run_logger()
    logger.info("Checking playlist tracks to ensure they are valid.")
    playlist_tracks_frame = pl.DataFrame().with_columns(  # noqa
        track_id=pl.Series(playlist_tracks)
    )
    invalid_tracks = database.sql(
        """
            SELECT ph.track_id
            FROM play_history AS ph
            INNER JOIN playlist_tracks_frame AS ptf ON
            ph.track_id = ptf.track_id
            GROUP BY ph.track_id
            HAVING DATE_DIFF('day', MAX(ph.played_at), CURRENT_DATE) <= 14
        """
    ).pl()
    assert invalid_tracks.is_empty()


@flow(name="Load smart playlists")
def load_smart_playlists(
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

    root_playlist_names = get_root_playlist_names_task(database)

    for root_playlist_name in root_playlist_names:
        tracks = get_tracks_for_playlist_task(database, root_playlist_name)
        validate_playlist_tracks_task(database, tracks)
        num_recommended_tracks = 30 - len(tracks)
        tracks += get_recommended_tracks_task(
            spotify, tracks, num_recommended_tracks
        )
        playlist_id = get_playlist_from_spotify_task(
            spotify, root_playlist_name
        )
        if playlist_id is None:
            playlist_id = create_playlist_on_spotify_task(
                spotify, root_playlist_name
            )
        load_playlist_to_spotify_task(spotify, playlist_id, tracks)
        logger.info(f"Loaded new tracks for {root_playlist_name}.")
    database.close()


if __name__ == "__main__":
    typer.run(load_smart_playlists)
