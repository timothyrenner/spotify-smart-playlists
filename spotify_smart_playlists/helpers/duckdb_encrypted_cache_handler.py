import duckdb
import os
import json

from dotenv import load_dotenv, find_dotenv
from loguru import logger
from spotipy.cache_handler import CacheHandler
from typing import Dict, Optional
from cryptography.fernet import Fernet

logger.info("Loading dotenv file.")
load_dotenv(find_dotenv())
SPOTIFY_CACHE_FERNET_KEY = os.getenv("SPOTIFY_CACHE_FERNET_KEY")


def get_cache_fernet_key_from_environment() -> str:
    load_dotenv(find_dotenv())
    cache_fernet_key = os.getenv("SPOTIFY_CACHE_FERNET_KEY")
    if cache_fernet_key is None:
        raise ValueError(
            "Unable to find SPOTIFY_CACHE_FERNET_KEY in env or .env"
        )
    return cache_fernet_key


class DuckDBEncryptedCacheHandler(CacheHandler):
    def __init__(self, database: str, cache_fernet_key: str | None = None):
        self.database = database
        if cache_fernet_key is None:
            cache_fernet_key = get_cache_fernet_key_from_environment()
        self.fernet = Fernet(cache_fernet_key.encode("utf-8"))
        # Create the table if it doesn't exist.
        conn = duckdb.connect(self.database)
        try:
            conn.execute(
                """
            CREATE TABLE IF NOT EXISTS credentials(
                creds VARCHAR,
                service VARCHAR
            );
            """
            )
        finally:
            conn.close()

    def get_cached_token(self) -> Optional[Dict[str, str]]:
        conn = duckdb.connect(self.database)
        try:
            encrypted_credentials = conn.execute(
                "SELECT creds FROM credentials"
            ).fetchone()
            if not encrypted_credentials:
                return None
            credentials = self.fernet.decrypt(
                encrypted_credentials[0].encode("utf-8")
            ).decode("utf-8")
            return json.loads(credentials)
        finally:
            conn.close()

    def save_token_to_cache(self, token_info: Dict[str, str]):
        conn = duckdb.connect(self.database)
        encrypted_credentials = self.fernet.encrypt(
            json.dumps(token_info).encode("utf-8")
        )
        try:
            conn.execute(
                """
                DELETE FROM credentials WHERE service='spotify';
                INSERT INTO credentials (creds, service)
                VALUES (?, 'spotify');
                """,
                [encrypted_credentials.decode("utf-8")],
            )
        finally:
            conn.close()
