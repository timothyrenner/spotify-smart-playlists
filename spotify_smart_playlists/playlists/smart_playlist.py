import spotipy
from typing import List, Optional
from loguru import logger
from random import sample
from duckdb import DuckDBPyConnection


def get_recommended_tracks(
    spotify: spotipy.Spotify,
    playlist_tracks: List[str],
    tracks_to_pull: int = 5,
    logger=logger,
) -> List[str]:
    playlist_tracks_sample = sample(
        playlist_tracks, min(len(playlist_tracks), 5)
    )
    logger.info(f"Pulling {tracks_to_pull} tracks from Spotify.")
    recommended_tracks_response = spotify.recommendations(
        seed_tracks=playlist_tracks_sample, limit=tracks_to_pull
    )
    recommended_tracks = [
        t["id"] for t in recommended_tracks_response["tracks"]
    ]
    logger.info(
        f"Recommended tracks pull successful, got {len(recommended_tracks)}"
    )
    return recommended_tracks


def get_playlist_from_spotify(
    spotify: spotipy.Spotify, playlist_name: str, logger=logger
) -> Optional[str]:
    logger.info("Getting user playlists.")
    current_user_playlists_response = spotify.current_user_playlists()

    while current_user_playlists_response:
        for item in current_user_playlists_response["items"]:
            if item["name"] == playlist_name:
                return item["id"]

        logger.info(
            f"Target playlist {playlist_name} not found - "
            "getting next page of user playlists."
        )
        current_user_playlists_response = spotify.next(
            current_user_playlists_response
        )

    return None


def get_tracks_for_playlist(
    database: DuckDBPyConnection, playlist_name: str, num_tracks: int = 25
) -> List[str]:
    return (
        database.query(
            f"""
            WITH latest_played AS (
                SELECT 
                    track_id,
                    MAX(played_at) AS played_at
                FROM play_history
                GROUP BY track_id
            )

            SELECT
                rp.track_id
            FROM root_playlists AS rp
            INNER JOIN latest_played AS lp
                ON lp.track_id = rp.track_id
            WHERE
                rp.name = '{playlist_name}' AND
                DATE_DIFF('day', lp.played_at, CURRENT_DATE) > 14
            ORDER BY RANDOM()
            LIMIT {num_tracks}
        """
        )
        .pl()
        .get_column("track_id")
        .to_list()
    )


def create_playlist_on_spotify(
    spotify: spotipy.Spotify, playlist_name: str, logger=logger
) -> str:
    logger.info(f"Creating {playlist_name}")
    create_playlist_response = spotify.user_playlist_create(
        spotify.me()["id"], playlist_name
    )
    return create_playlist_response["id"]


def load_playlist_to_spotify(
    spotify: spotipy.Spotify,
    playlist_id: str,
    playlist_tracks: List[str],
    logger=logger,
):
    logger.info(
        f"Loading playlist {playlist_id} with {len(playlist_tracks)} tracks."
    )
    spotify.user_playlist_replace_tracks(
        spotify.me()["id"], playlist_id, playlist_tracks
    )
