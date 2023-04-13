import typer
from prefect import flow, task, get_run_logger
from pathlib import Path
import duckdb
import polars as pl
from database import save_to_database
from duckdb import DuckDBPyConnection
from typing import List
from spotify_smart_playlists.playlists import (
    PlaylistConfig,
    playlist_config_from_dict,
)
import yaml


@task("Get playlist configs")
def get_playlist_configs_task(
    playlist_config_dir: Path,
) -> List[PlaylistConfig]:
    logger = get_run_logger()
    playlist_configs: List[PlaylistConfig] = []
    for playlist_config_file in playlist_config_dir.glob("*.yaml"):
        logger.info(f"Creating config from {playlist_config_file}")
        playlist_config_dict = yaml.safe_load(playlist_config_file.read_text())
        playlist_configs.append(
            playlist_config_from_dict(playlist_config_dict)
        )
    return playlist_configs


@flow(name="Build root playlists")
def build_root_playlists(
    database_file: str = "spotify.db",
    playlist_config_dir: Path = Path("playlists"),
):
    logger = get_run_logger()

    database = duckdb.connect(database_file)
