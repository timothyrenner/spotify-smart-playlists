import typer
import spotipy
import pandas as pd
import os

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from toolz import thread_last, partition_all
from typing import List


def main(library_file: str, audio_features_file: str):

    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    logger.info(f"Reading library from {library_file}.")
    library_frame = pd.read_csv(library_file)

    if not os.path.exists(audio_features_file):
        logger.warning(
            f"{audio_features_file} doesn't exist. "
            "Obtaining all audio features."
        )
        audio_features_frame = pd.DataFrame()
        tracks_with_audio_features = set()
    else:
        logger.info(
            f"Reading existing audio features from {audio_features_file}."
        )
        audio_features_frame = pd.read_csv(audio_features_file)
        tracks_with_audio_features = set(audio_features_frame.track_id)
        logger.info(
            "Audio features already exist for "
            f"{len(tracks_with_audio_features)}."
        )

    batches = thread_last(
        library_frame.track_id.tolist(),
        (filter, lambda x: x not in tracks_with_audio_features),
        (partition_all, 100),
    )

    new_audio_feature_frames: List[pd.DataFrame] = []
    audio_features_pulled: int = 0
    for track_batch in batches:
        logger.info(f"Pulling audio features for {len(track_batch)} tracks.")
        audio_features_response = spotify.audio_features(tracks=track_batch)
        new_audio_feature_frames.append(
            pd.DataFrame.from_dict(audio_features_response)
            .rename(columns={"id": "track_id"})
            .drop(columns=["uri", "track_href", "analysis_url", "type"])
        )
        audio_features_pulled += len(audio_features_response)

    logger.info(
        f"Pulled {audio_features_pulled} audio features. "
        "Adding to existing features."
    )
    audio_features_frame = pd.concat(
        [audio_features_frame] + new_audio_feature_frames
    )
    logger.info(f"Saving updated audio features to {audio_features_file}.")
    audio_features_frame.to_csv(audio_features_file, index=False)
    logger.info("Done.")


if __name__ == "__main__":
    typer.run(main)