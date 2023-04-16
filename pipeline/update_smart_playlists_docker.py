import typer
from prefect.blocks.system import Secret
from prefect_gcp import GcpCredentials, GcsBucket
from update_smart_playlists import update_smart_playlists
from prefect import flow, get_run_logger


@flow(name="Update smart playlists (Docker)")
def main(
    database_file: str = "spotify.db", playlist_config_dir: str = "playlists"
):
    logger = get_run_logger()

    logger.info("Fetching credentials.")
    gcp_credentials = GcpCredentials.load("prefect-gcs-rw")

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

    logger.info(f"Downloading database to {database_file}")
    spotify_bucket = GcsBucket(
        bucket="trenner-datasets", gcp_credentials=gcp_credentials
    )
    spotify_bucket.download_object_to_path("spotify/spotify.db", database_file)

    logger.info("Executing pipeline.")
    update_smart_playlists(
        database_file=database_file,
        playlist_config_dir=playlist_config_dir,
        cache_fernet_key=cache_fernet_key,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
    )

    logger.info("Uploading database to GCS.")
    spotify_bucket.upload_from_path(database_file, "spotify/spotify.db")
    logger.info("All done.")


if __name__ == "__main__":
    typer.run(main)
