import typer
import os
import spotipy

from typing import Optional, Dict, Any, List
from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth

from toolz import pluck, thread_first
from random import sample, shuffle


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


def get_playlist_tracks(
    playlist_id: str, spotify: spotipy.Spotify
) -> List[Dict[str, str]]:
    logger.info(f"Obtaining tracks for playlist {playlist_id}.")
    # NOTE The spotify.playlist doesn't work quite right. The next parameter
    # is inside the tracks object, which is not what we want. IDK why that
    # doesn't work as intended or why spotify did me wrong like that.
    playlist_tracks_response = spotify.playlist_tracks(
        playlist_id, fields="items.track(id,name),total,next"
    )

    playlist_tracks: List[Dict[str, str]] = []

    while playlist_tracks_response:
        playlist_tracks.extend(
            [item["track"] for item in playlist_tracks_response["items"]]
        )

        logger.info("Checking if there are more tracks.")
        playlist_tracks_response = spotify.next(playlist_tracks_response)
    return playlist_tracks


def get_recent_tracks(spotify: spotipy.Spotify) -> List[Dict[str, str]]:

    recent_tracks_response = spotify.current_user_recently_played()
    recent_tracks: List[Dict[str, str]] = []

    while recent_tracks_response:
        recent_tracks.extend(
            [
                {"id": rt["track"]["id"], "name": rt["track"]["name"]}
                for rt in recent_tracks_response["items"]
            ]
        )

        logger.info("Checking if there are more tracks.")
        recent_tracks_response = spotify.next(recent_tracks_response)
    return recent_tracks


def main(
    seed_playlist: str, size: int = 50, new_playlist: Optional[str] = None
):
    if not new_playlist:
        new_playlist = f"auto-{seed_playlist}"
        logger.info(f"New playlist name defaulting to {new_playlist}.")

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    seed_playlist_object = get_playlist(seed_playlist, spotify)

    if not seed_playlist_object:
        raise ValueError(f"Couldn't find seed playlist {seed_playlist}.")

    seed_playlist_tracks = get_playlist_tracks(
        seed_playlist_object["id"], spotify
    )

    if not seed_playlist_tracks:
        raise ValueError(f"Couldn't obtain tracks for {seed_playlist}.")

    recent_tracks = get_recent_tracks(spotify)

    recent_track_ids = set(pluck("id", recent_tracks))
    seed_playlist_track_ids = set(pluck("id", seed_playlist_tracks))
    logger.info("Removing recently played tracks.")
    seed_playlist_non_recent_track_ids = (
        seed_playlist_track_ids - recent_track_ids
    )

    num_tracks = 25
    tracks_to_sample = min(
        len(seed_playlist_non_recent_track_ids), num_tracks - 5
    )
    logger.info(f"Downsampling to {tracks_to_sample} tracks.")
    new_playlist_tracks = list(
        sample(seed_playlist_non_recent_track_ids, tracks_to_sample)
    )
    recommendations_to_fetch = num_tracks - len(new_playlist_tracks)
    recommendation_seed = thread_first(
        new_playlist_tracks, (sample, min(len(new_playlist_tracks), 5)), list
    )
    logger.info(f"Fetching {recommendations_to_fetch} recommended tracks.")
    recommended_tracks_response = spotify.recommendations(
        seed_tracks=recommendation_seed, limit=recommendations_to_fetch
    )

    new_playlist_tracks += [
        rt["id"] for rt in recommended_tracks_response["tracks"]
    ]
    shuffle(new_playlist_tracks)

    new_playlist_object = get_playlist(new_playlist, spotify)
    if not new_playlist_object:
        logger.info(f"Playlist {new_playlist} doesn't exist. Creating.")
        new_playlist_object = spotify.user_playlist_create(
            spotify.me()["id"], new_playlist
        )

    logger.info(f"Replacing tracks for {new_playlist}.")
    spotify.user_playlist_replace_tracks(
        spotify.me()["id"], new_playlist_object["id"], new_playlist_tracks
    )


if __name__ == "__main__":
    typer.run(main)