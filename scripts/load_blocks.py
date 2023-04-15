import typer
from dotenv import load_dotenv, find_dotenv
from loguru import logger
from prefect.blocks.system import Secret
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

    logger.info("All blocks configured.")


if __name__ == "__main__":
    typer.run(main)
