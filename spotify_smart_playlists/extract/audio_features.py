import spotipy
import polars as pl
from loguru import logger
from typing import List
from dataclasses import dataclass, asdict
from toolz import partition_all


@dataclass
class TrackAudioFeatures:
    track_id: str
    acousticness: float
    danceability: float
    duration_ms: int
    energy: float
    instrumentalness: float
    key: int
    liveness: float
    loudness: float
    mode: int
    speechiness: float
    tempo: float
    time_signature: int
    valence: float


def pull_audio_features(
    spotify: spotipy.Spotify,
    track_audio_features_to_pull: List[str],
    logger=logger,
) -> pl.DataFrame:
    logger.info("Pulling track audio features in batches of 100.")

    all_track_audio_features: List[TrackAudioFeatures] = []
    for track_batch in partition_all(100, track_audio_features_to_pull):
        logger.info(f"Pulling audio features for {len(track_batch)} tracks.")

        audio_features_response = spotify.audio_features(tracks=track_batch)
        for track_audio_features in audio_features_response:
            all_track_audio_features.append(
                TrackAudioFeatures(
                    track_id=track_audio_features["id"],
                    acousticness=track_audio_features["acousticness"],
                    danceability=track_audio_features["danceability"],
                    duration_ms=track_audio_features["duration_ms"],
                    energy=track_audio_features["energy"],
                    instrumentalness=track_audio_features["instrumentalness"],
                    key=track_audio_features["key"],
                    liveness=track_audio_features["liveness"],
                    loudness=track_audio_features["loudness"],
                    mode=track_audio_features["mode"],
                    speechiness=track_audio_features["speechiness"],
                    tempo=track_audio_features["tempo"],
                    time_signature=track_audio_features["time_signature"],
                    valence=track_audio_features["valence"],
                )
            )
    return pl.from_dicts(map(asdict, all_track_audio_features))
