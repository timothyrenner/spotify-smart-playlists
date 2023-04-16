import typer
from prefect import flow
from typing import Optional
from update_recent_tracks import update_recent_tracks
from pull_library import pull_library
from update_artists import update_artists
from update_track_audio_features import update_track_audio_features
from build_root_playlists import build_root_playlists
from load_smart_playlists import load_smart_playlists
from pathlib import Path


@flow(name="Update smart playlists")
def update_smart_playlists(
    database_file: str = "spotify.db",
    playlist_config_dir: Path = Path("playlists"),
    cache_fernet_key: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    redirect_uri: Optional[str] = None,
):
    update_recent_tracks(
        database_file=database_file,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    pull_library(
        database_file=database_file,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    update_artists(
        database_file=database_file,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    update_track_audio_features(
        database_file=database_file,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )
    build_root_playlists(
        database_file=database_file, playlist_config_dir=playlist_config_dir
    )
    load_smart_playlists(
        database_file=database_file,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )


if __name__ == "__main__":
    typer.run(update_smart_playlists)
