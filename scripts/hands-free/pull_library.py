import typer
import ibis
import spotipy
import pandas as pd
import warnings

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from typing import List, Dict
from dateutil.parser import parse

# This gets old real damn quick idgaf about pandas indices that's why I'm using
# duck in the first place.
warnings.filterwarnings(
    "ignore", message="duckdb-engine doesn't yet support reflection on indices"
)


def main(database: str):

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(
        client_credentials_manager=spotify_auth(database)
    )

    logger.info("Pulling library tracks.")
    library_tracks_response = spotify.current_user_saved_tracks(limit=50)

    library_tracks: List[Dict[str, str]] = []
    track_artists: List[Dict[str, str]] = []
    while library_tracks_response:
        for track in library_tracks_response["items"]:
            library_tracks.append(
                {
                    "date_added": parse(track["added_at"]),
                    "track_id": track["track"]["id"],
                    "track_name": track["track"]["name"],
                }
            )
            for artist in track["track"]["artists"]:
                track_artists.append(
                    {
                        "track_id": track["track"]["id"],
                        "artist_id": artist["id"],
                    }
                )

        logger.info("Attempting to fetch another page.")
        library_tracks_response = spotify.next(library_tracks_response)

    logger.info(
        f"Found {len(library_tracks)} tracks. Marshalling into data frame."
    )
    library_tracks_frame = pd.DataFrame.from_dict(library_tracks)
    logger.info(
        f"Found {len(track_artists)} artists. Marshalling into data frame."
    )
    track_artists_frame = pd.DataFrame.from_dict(track_artists)

    logger.info(f"Connecting to {database}.")
    db = ibis.duckdb.connect(database)

    logger.info("Saving to database.")
    db.load_data("library_tracks", library_tracks_frame, if_exists="replace")
    db.load_data("track_artists", track_artists_frame, if_exists="replace")
    logger.info("Done!")


if __name__ == "__main__":
    typer.run(main)
