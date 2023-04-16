import typer
from dotenv import load_dotenv, find_dotenv
from loguru import logger
import json
from prefect.infrastructure.process import Process
from prefect.blocks.system import Secret
from prefect.filesystems import GCS
from pydantic import SecretStr

from prefect_gcp import GcpCredentials
import os
from spotify_smart_playlists.helpers import (
    get_spotify_credentials_from_environment,
)

logger.info("Loading .env")
load_dotenv(find_dotenv())


def main():
    cache_fernet_key = os.getenv("SPOTIFY_CACHE_FERNET_KEY")
    if cache_fernet_key is None:
        raise ValueError("SPOTIFY_CACHE_FERNET_KEY not in environment or .env")

    spotify_credentials = get_spotify_credentials_from_environment()

    gcp_credentials_location = os.getenv("PREFECT_GCS_RW_PATH")
    if gcp_credentials_location is None:
        raise ValueError("PREFECT_GCS_RW_PATH not in environment or .env")

    logger.info("Creating block for the cache fernet key.")
    cache_fernet_key_secret = Secret(value=cache_fernet_key)
    cache_fernet_key_secret.save(
        name="spotify-cache-fernet-key", overwrite=True
    )

    logger.info("Creating block for the Spotify client id.")
    client_id_secret = Secret(value=spotify_credentials.client_id)
    client_id_secret.save(name="spotify-client-id", overwrite=True)

    logger.info("Creating block for the Spotify client secret.")
    client_secret_secret = Secret(value=spotify_credentials.client_secret)
    client_secret_secret.save(name="spotify-client-secret", overwrite=True)

    logger.info("Creating a block for the Spotify redirect URI.")
    redirect_uri_secret = Secret(value=spotify_credentials.redirect_uri)
    redirect_uri_secret.save(name="spotify-redirect-uri", overwrite=True)

    logger.info("Creating infrastructure block for local processes.")
    local_process_infrastructure = Process()
    local_process_infrastructure.save(name="spotify-local", overwrite=True)

    # This is really annoying. I can't use the GcpCredentials block the way
    # I want to because the json ser / deser puts in extra new lines. I have
    # to literally copy/paste it but obviously that's dumb and I want to
    # automate this. So I'm gonna slurp the credentials file.
    logger.info("Getting service account creds.")
    with open(gcp_credentials_location, "r") as f:
        gcp_credentials = f.read()

    logger.info("Configuring storage block.")
    gcs_recent_tracks = GCS(
        bucket_path="trenner-datasets/spotify-pipeline/recent-tracks",
        service_account_info=gcp_credentials,
    )
    gcs_recent_tracks.save(
        name="spotify-recent-tracks-storage", overwrite=True
    )
    gcs_smart_playlists = GCS(
        bucket_path="trenner-datasets/spotify-pipeline/smart-playlists",
        service_account_info=gcp_credentials,
    )

    gcs_smart_playlists.save(
        name="spotify-smart-playlists-storage", overwrite=True
    )

    logger.info("All blocks configured.")


if __name__ == "__main__":
    typer.run(main)
