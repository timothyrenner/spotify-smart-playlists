import typer
import os

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from ghapi.all import GhApi
from base64 import b64encode
from nacl import encoding, public
from typing import Tuple

logger.info("Loading dotenv file.")
load_dotenv(find_dotenv())

GH_SPOTIFY_ACCESS_TOKEN = os.getenv("GH_SPOTIFY_ACCESS_TOKEN")


def encrypt_secret(public_key, secret_value: str) -> str:
    public_key_obj = public.PublicKey(
        public_key.encode("utf-8"), encoding.Base64Encoder()
    )
    box = public.SealedBox(public_key_obj)
    encrypted_secret = box.encrypt(secret_value.encode("utf-8"))
    return b64encode(encrypted_secret).decode("utf-8")


def debugger(x):
    print(x.method)
    print(x.full_url)
    print(x.data)


def main(secret_name="MY_SECRET", secret_value="BOO!"):
    gh_api = GhApi(
        owner="timothyrenner",
        repo="spotify-smart-playlists",
        token=GH_SPOTIFY_ACCESS_TOKEN,
        # debug=debugger,
    )
    logger.info("Listing secrets before adding one.")
    print(gh_api.actions.list_repo_secrets())

    logger.info("Obtaining public key for repository.")
    public_key = gh_api.actions.get_repo_public_key()
    print(public_key)
    logger.info("Encrypting secret.")
    encrypted_secret = encrypt_secret(public_key.key, secret_value)
    logger.info("Attempting to load secret.")
    gh_api.actions.create_or_update_repo_secret(
        secret_name=secret_name,
        encrypted_value=encrypted_secret,
        key_id=public_key.key_id,
    )

    logger.info("Listing secrets again.")
    print(gh_api.actions.list_repo_secrets())


if __name__ == "__main__":
    typer.run(main)