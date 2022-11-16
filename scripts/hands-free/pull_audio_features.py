import typer
import spotipy
import pandas as pd
import ibis
import warnings

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from toolz import partition_all
from typing import List
from sqlalchemy.exc import NoSuchTableError

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

    logger.info("Connecting to database.")
    db = ibis.duckdb.connect(database)

    library_tracks = db.table("library_tracks")
    track_audio_features_to_pull: List[str] = []
    try:
        track_audio_features = db.table("track_audio_features")
        joined_tracks = library_tracks.left_join(
            track_audio_features,
            predicates=(
                library_tracks["track_id"] == track_audio_features["track_id"]
            ),
            suffixes=["_l", "_af"],
        )
        track_audio_features_to_pull = (
            joined_tracks.filter(joined_tracks.track_id_l.isnull())
            .execute()
            .track_id_l.tolist()
        )
    except NoSuchTableError:
        logger.info(
            "Table 'track_audio_features' doesn't exist. "
            "Pulling audio features for all tracks."
        )
        track_audio_features_to_pull = (
            library_tracks.execute().track_id.tolist()
        )

    for track_batch in partition_all(100, track_audio_features_to_pull):
        logger.info(f"Pulling audio features for {len(track_batch)} tracks.")
        audio_features_response = spotify.audio_features(tracks=track_batch)

        new_audio_feature_frame = (
            pd.DataFrame.from_dict(audio_features_response)
            .rename(columns={"id": "track_id"})
            .drop(columns=["uri", "track_href", "analysis_url", "type"])
        )
        logger.info(
            f"Inserting {new_audio_feature_frame.shape[0]} "
            "audio features into db."
        )
        db.load_data(
            "track_audio_features", new_audio_feature_frame, if_exists="append"
        )

    logger.info("Done.")


if __name__ == "__main__":
    typer.run(main)
