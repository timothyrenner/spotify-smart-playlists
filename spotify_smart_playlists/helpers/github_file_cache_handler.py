import os
import json

from spotipy.cache_handler import CacheHandler, CacheFileHandler
from dotenv import load_dotenv, find_dotenv
from loguru import logger
from ghapi.all import GhApi
from base64 import b64encode
from nacl import encoding, public

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


class CacheFileGithubHandler(CacheHandler):
    def __init__(self):
        self.cache_file_handler = CacheFileHandler()
        self.gh_api = GhApi(
            # TODO maybe don't hard code this.
            owner="timothyrenner",
            repo="spotify-smart-playlists",
            token=GH_SPOTIFY_ACCESS_TOKEN,
        )

    def _update_github(self, token_info):
        logger.info("Obtaining public key from GitHub repository.")
        public_key = self.gh_api.actions.get_repo_public_key()

        logger.info("Encrypting secret.")
        encrypted_token_info = encrypt_secret(
            public_key.key, json.dumps(token_info)
        )

        logger.info("Loading secret to GitHub.")
        self.gh_api.actions.create_or_update_repo_secret(
            secret_name="SPOTIPY_CREDENTIALS",
            encrypted_value=encrypted_token_info,
            key_id=public_key.key_id,
        )

    def get_cached_token(self):
        return self.cache_file_handler.get_cached_token()

    def save_token_to_cache(self, token_info):
        self.cache_file_handler.save_token_to_cache(token_info)
        self._update_github(token_info)
        return None