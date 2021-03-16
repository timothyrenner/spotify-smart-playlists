import typer
import spotipy
import pandas as pd
import os

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from toolz import thread_last, mapcat, partition_all
from typing import List


def main(library_file: str, artists_file: str):
    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    logger.info(f"Reading library from {library_file}.")
    library_frame = pd.read_csv(library_file)

    if not os.path.exists(artists_file):
        logger.warning(
            f"{artists_file} doesn't exist. " "Obtaining all artists."
        )
        artists_frame = pd.DataFrame()
        artists_with_data = set()
    else:
        logger.info(f"Reading existing artists from {artists_file}.")
        artists_frame = pd.read_csv(artists_file)
        artists_with_data = set(artists_frame.artist_id.tolist())

    batches = thread_last(
        library_frame.artist_ids.tolist(),
        (mapcat, lambda x: x.split("|")),
        set,
        (filter, lambda x: x not in artists_with_data),
        (partition_all, 50),
    )

    new_artists_frames: List[pd.DataFrame] = []
    new_artists_pulled: int = 0

    for artist_batch in batches:
        logger.info(f"Pulling features for {len(artist_batch)} artists.")
        artists_response = spotify.artists(artist_batch)
        new_artists_frame = pd.DataFrame(
            [
                {
                    "artist_id": artist["id"],
                    "artist_name": artist["name"],
                    "genres": "|".join(artist["genres"]),
                }
                for artist in artists_response["artists"]
            ]
        )
        new_artists_frames.append(new_artists_frame)
        new_artists_pulled += new_artists_frame.shape[0]

    logger.info(
        f"Pulled {new_artists_pulled} new artists. Adding to existing artists."
    )
    artists_frame = pd.concat([artists_frame] + new_artists_frames)
    logger.info(f"Saving updated artists to {artists_file}.")
    artists_frame.to_csv(artists_file, index=False)
    logger.info("Done.")


if __name__ == "__main__":
    typer.run(main)