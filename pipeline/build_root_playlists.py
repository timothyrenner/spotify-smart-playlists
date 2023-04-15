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
    make_root_playlist,
)
import yaml


@task(name="Get playlist configs")
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


@task(name="Make root playlist")
def make_root_playlist_task(
    database: DuckDBPyConnection, playlist_config: PlaylistConfig
) -> pl.DataFrame:
    logger = get_run_logger()
    return make_root_playlist(database, playlist_config, logger=logger)


@flow(name="Build root playlists")
def build_root_playlists(
    database_file: str = "spotify.db",
    playlist_config_dir: Path = Path("playlists"),
):
    logger = get_run_logger()

    database = duckdb.connect(database_file)
    playlist_configs = get_playlist_configs_task(playlist_config_dir)

    root_playlists: List[pl.DataFrame] = []
    for playlist_config in playlist_configs:
        logger.info(f"Getting tracks for {playlist_config.name}")
        root_playlists.append(
            make_root_playlist_task(database, playlist_config).with_columns(
                name=pl.lit(playlist_config.name)
            )
        )
    root_playlists_frame = pl.concat(root_playlists)
    logger.info(f"Saving all root playlists to {database}.")
    save_to_database(
        database=database,
        table="root_playlists",
        data_frame=root_playlists_frame,
        create_or_replace=True,
    )
    database.close()


if __name__ == "__main__":
    typer.run(build_root_playlists)
