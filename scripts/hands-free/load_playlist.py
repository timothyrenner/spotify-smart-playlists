import typer
import spotipy
import ibis
import warnings

from datetime import datetime
from datetime import timezone
from dateutil.relativedelta import relativedelta
from spotify_smart_playlists.helpers import spotify_auth
from loguru import logger
from random import sample
from typing import List, Optional, Any, Dict
from toolz import thread_first
from omegaconf import OmegaConf

# This gets old real damn quick idgaf about pandas indices that's why I'm using
# duck in the first place.
warnings.filterwarnings(
    "ignore", message="duckdb-engine doesn't yet support reflection on indices"
)


def get_recommended_tracks(
    all_seed_tracks: List[str],
    recommendations_to_fetch: int,
    spotify: spotipy.Spotify,
) -> List[str]:
    seed_tracks = thread_first(
        all_seed_tracks, (sample, min(len(all_seed_tracks), 5)), list
    )
    recommended_tracks_response = spotify.recommendations(
        seed_tracks=seed_tracks, limit=recommendations_to_fetch
    )
    return [rt["id"] for rt in recommended_tracks_response["tracks"]]


def get_playlist(
    playlist_name: str, spotify: spotipy.Spotify
) -> Optional[Dict[str, Any]]:
    logger.info("Fetching existing playlists.")
    current_user_playlists_response = spotify.current_user_playlists()
    seed_playlist_object = None

    while current_user_playlists_response:
        if seed_playlist_object:
            break

        for item in current_user_playlists_response["items"]:
            if item["name"] == playlist_name:
                seed_playlist_object = item
                break

        logger.info("Attempting to page over the next playlists.")
        current_user_playlists_response = spotify.next(
            current_user_playlists_response
        )
    return seed_playlist_object


def main(
    database: str,
    playlist_file: str,
    num_tracks: int = 25,
    num_recommendations: int = 5,
):
    logger.info(f"Getting playlist name from {playlist_file}.")
    playlist_config = OmegaConf.load(playlist_file)
    playlist_name = playlist_config.name
    logger.info("Connecting to database.")
    db = ibis.duckdb.connect(database)

    logger.info("Getting most recently played tracks.")
    play_history = db.table("play_history")
    window = ibis.window(group_by="track_id", order_by=ibis.desc("played_at"))
    play_history = play_history.mutate(
        play_rank=play_history.track_id.rank().over(window)
    )

    one_week_ago = datetime.now(timezone.utc) - relativedelta(weeks=1)
    tracks_recently_playesd = play_history.filter(
        # First predicate ensures that play is the latest play.
        # Second predicate isolates plays to less than one week ago.
        (play_history.play_rank == 0)
        & (play_history.played_at > one_week_ago)
    )

    root_playlist = db.table(playlist_name)

    logger.info(f"Removing tracks played after {one_week_ago}.")
    reduced_root_playlist = root_playlist.left_join(
        tracks_recently_playesd,
        predicates=(
            root_playlist.track_id == tracks_recently_playesd.track_id
        ),
        suffixes=("", "_r"),
    )
    reduced_root_playlist = reduced_root_playlist.filter(
        # Tracks that were played in the last week will not have null for
        # played_at, so this predicate removes them...
        reduced_root_playlist.played_at.isnull()
        # ... unless rotate is false, then it's kept in.
        | (reduced_root_playlist.rotate is False)
    )

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(
        client_credentials_manager=spotify_auth(database)
    )

    num_root_tracks = num_tracks - num_recommendations
    logger.info(f"Downsampling root playlist to {num_root_tracks}.")
    new_playlist_tracks = (
        reduced_root_playlist.mutate(sample=ibis.random())
        .sort_by("sample")
        .limit(num_root_tracks)
        .execute()
        .track_id.tolist()
    )
    while len(new_playlist_tracks) < num_tracks:
        num_recommendations_to_pull = min(
            20, num_tracks - len(new_playlist_tracks)
        )
        logger.info(
            f"Pulling {num_recommendations_to_pull} recommended tracks."
        )
        new_playlist_tracks += get_recommended_tracks(
            new_playlist_tracks, num_recommendations_to_pull, spotify
        )

    logger.info(f"Figuring out if {playlist_name} exists already.")
    new_playlist_object = get_playlist(playlist_name, spotify)
    if not new_playlist_object:
        logger.info(f"Playlist {playlist_name} doesn't exist. Creating.")
        new_playlist_object = spotify.user_playlist_create(
            spotify.me()["id"], playlist_name
        )

    logger.info(f"Updating tracks for {playlist_name}.")
    spotify.user_playlist_replace_tracks(
        spotify.me()["id"], new_playlist_object["id"], new_playlist_tracks
    )
    logger.info("All done!")


if __name__ == "__main__":
    typer.run(main)
