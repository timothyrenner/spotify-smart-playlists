import typer
import spotipy
import pandas as pd

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from typing import List, Dict, Any


def process_track(track: Dict[str, Any]) -> Dict[str, str]:
    return {
        "track_id": track["id"],
        "artist_ids": "|".join([artist["id"] for artist in track["artists"]]),
        "name": track["name"],
    }


def main(library_file: str):

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    logger.info("Pulling library tracks.")
    library_tracks_response = spotify.current_user_saved_tracks(limit=50)

    library_tracks: List[Dict[str, str]] = []
    while library_tracks_response:
        library_tracks.extend(
            [
                {
                    "date_added": item["added_at"],
                    **process_track(item["track"]),
                }
                for item in library_tracks_response["items"]
            ]
        )
        logger.info("Attempting to fetch another page.")
        library_tracks_response = spotify.next(library_tracks_response)

    logger.info(
        f"Found {len(library_tracks)} tracks. Marshalling into data frame."
    )
    library_tracks_frame = pd.DataFrame.from_dict(library_tracks)

    logger.info(f"Saving {library_tracks_frame.shape[0]} to {library_file}.")
    library_tracks_frame.to_csv(library_file, index=False)
    logger.info("Done!")


if __name__ == "__main__":
    typer.run(main)
