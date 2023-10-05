import typer
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials, GcsBucket
from update_recent_tracks import update_recent_tracks
from prefect import flow, get_run_logger
from prefect.filesystems import RemoteFileSystem
from pathlib import Path


@flow(name="Update recent tracks (Docker)")
def main(
    data_dir: Path = Path("data"), playlist_config_dir: str = "playlists"
):
    logger = get_run_logger()

    logger.info("Fetching cache fernet key.")
    cache_fernet_key_block = Secret.load("spotify-cache-fernet-key")
    cache_fernet_key = cache_fernet_key_block.get()

    logger.info("Fetching client id.")
    client_id_block = Secret.load("spotify-client-id")
    client_id = client_id_block.get()

    logger.info("Fetching client secret.")
    client_secret_block = Secret.load("spotify-client-secret")
    client_secret = client_secret_block.get()

    logger.info("Fetching redirect uri.")
    redirect_uri_block = Secret.load("spotify-redirect-uri")
    redirect_uri = redirect_uri_block.get()

    database_file = data_dir.absolute() / "spotify.db"
    logger.info(f"Downloading database to {database_file}")
    minio_local_dataset_storage = RemoteFileSystem.load(
        "minio-local-dataset-storage"
    )
    minio_local_dataset_storage.get_directory(
        from_path="spotify/data", local_path=str(data_dir)
    )

    logger.info("Executing pipeline.")
    update_recent_tracks(
        database_file=str(database_file),
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )

    logger.info("Saving to remote storage.")
    minio_local_dataset_storage.put_directory(
        local_path=str(data_dir), to_path="spotify/data"
    )

    logger.info("Fetching credentials.")
    gcp_credentials = GcpCredentials.load("prefect-gcs-rw")
    logger.info("Uploading database to GCS.")
    spotify_bucket = GcsBucket(
        bucket="trenner-datasets", gcp_credentials=gcp_credentials
    )
    spotify_bucket.upload_from_path(database_file, "spotify/spotify.db")
    logger.info("All done.")


if __name__ == "__main__":
    typer.run(main)
