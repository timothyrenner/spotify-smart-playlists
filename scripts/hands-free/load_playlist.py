import typer
import spotipy
import pandas as pd
import os

from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.tz import tz
from spotify_smart_playlists.helpers import spotify_auth
from loguru import logger
from random import sample, shuffle
from typing import List, Optional, Any, Dict
from toolz import thread_last, get, thread_first


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
    root_playlist_file: str,
    play_history_file: str,
    num_tracks: int = 25,
    num_recommendations: int = 5,
):
    logger.info(f"Loading play history from {play_history_file}.")
    play_history = pd.read_csv(play_history_file)

    logger.info("Getting most recently played tracks.")
    play_history.loc[:, "last_played"] = pd.to_datetime(
        play_history.last_played
    )
    play_history.loc[:, "rank"] = play_history.groupby("track_id")[
        "last_played"
    ].rank(method="first", ascending=False, na_option="top")

    tracks_latest_played = play_history.query("rank==1")

    logger.info(f"Loading root playlist from {root_playlist_file}.")
    root_playlist = pd.read_csv(root_playlist_file)

    one_week_ago = datetime.now(tz.tzutc()) - relativedelta(weeks=1)
    logger.info(f"Removing tracks played after {one_week_ago}.")
    root_playlist = root_playlist.merge(
        tracks_latest_played, on="track_id", how="left"
    ).query("last_played.isnull() | (last_played<@one_week_ago)")

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    num_root_tracks = num_tracks - num_recommendations
    logger.info(f"Downsampling root playlist to {num_root_tracks}.")
    new_playlist_tracks = list(
        sample(
            root_playlist.track_id.tolist(),
            min(root_playlist.shape[0], num_root_tracks),
        )
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

    new_playlist_name = thread_last(
        root_playlist_file, os.path.basename, os.path.splitext, (get, 0)
    )
    logger.info(f"Figuring out if {new_playlist_name} exists already.")
    new_playlist_object = get_playlist(new_playlist_name, spotify)
    if not new_playlist_object:
        logger.info(f"Playlist {new_playlist_name} doesn't exist. Creating.")
        new_playlist_object = spotify.user_playlist_create(
            spotify.me()["id"], new_playlist_name
        )

    logger.info(f"Updating tracks for {new_playlist_name}.")
    spotify.user_playlist_replace_tracks(
        spotify.me()["id"], new_playlist_object["id"], new_playlist_tracks
    )
    logger.info("All done!")


if __name__ == "__main__":
    typer.run(main)