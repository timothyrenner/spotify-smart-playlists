import typer
import pandas as pd

from omegaconf import OmegaConf
from loguru import logger
from typing import Dict, Any, List
from toolz import get


def split_pipe_delimited_string(pipe_delimited_string: str) -> List[str]:
    # The instance check makes this nan-safe.
    if isinstance(pipe_delimited_string, str):
        return pipe_delimited_string.split("|")
    else:
        return []


# This will compose via pipes.
def pipe_compose(series):
    # Dropna is for the nan values,
    # unique is because we blow out multiple artists per track
    # and multiple genres per artist, so lots of dupes possible.
    return "|".join(series.dropna().unique())


def process_audio_features(
    audio_features: pd.DataFrame, audio_features_config: Dict[str, Any]
) -> pd.DataFrame:
    queries: List[str] = []
    for feature, values in audio_features_config.items():
        if "min" in values:
            queries.append(f"({feature}>={values['min']})")
        if "max" in values:
            queries.append(f"({feature}<={values['max']})")

    query = "&".join(queries)
    return audio_features.query(query)


def process_artists(
    artists: pd.DataFrame, artists_config: List[str]
) -> pd.DataFrame:
    return artists.query("artist_name.isin(@artists_config)")


def process_genres(
    artists: pd.DataFrame, genres_config: List[str]
) -> pd.DataFrame:
    queries: List[str] = []
    for genre in genres_config:
        queries.append(f"genres.str.contains('{genre}')")
    query = "|".join(queries)
    return (
        artists.assign(genre=artists.genres.apply(split_pipe_delimited_string))
        .explode("genre")
        .query(query)
    )


def create_playlist_frame(
    library: pd.DataFrame, artists: pd.DataFrame, audio_features: pd.DataFrame
) -> pd.DataFrame:
    exploded_library = library.assign(
        artist_id=library.artist_ids.apply(split_pipe_delimited_string)
    ).explode("artist_id")

    final_tracks = (
        exploded_library.merge(artists, on="artist_id")
        # We merge on audio_features because it is filtered.
        # We'll hydrate with the full information later.
        .merge(audio_features, on="track_id")[
            ["track_id", "name", "artist_ids"]
        ]
        .drop_duplicates()
        .assign(rotate=True)  # Rotate tracks by default.
        .reset_index()
    )

    return final_tracks


def get_additional_tracks(
    library: pd.DataFrame,
    artists: pd.DataFrame,
    additional_tracks: List[Dict[str, str]],
) -> pd.DataFrame:
    additional_track_frames: List[pd.DataFrame] = []
    for additional_track in additional_tracks:
        additional_track_name = additional_track["name"]
        # First get all tracks that share the track name.
        library_track = library.query(
            f"name.str.lower()=='{additional_track_name.lower()}'"
        )
        if "artist" in additional_track:
            artist_name = additional_track["artist"]
            # Now merge with the artist and filter.
            # ! I'm doing this a _lot_.
            library_track = (
                library_track.assign(
                    artist_id=library_track.artist_ids.apply(
                        split_pipe_delimited_string
                    )
                )
                .explode("artist_id")
                .merge(artists, on="artist_id")
                # We just need to filter the artist down to disambiguate duped
                # track names. Once we've done that we just get it into the
                # regular playlist format we need before we hydrate.
                .query(f"artist_name.str.lower()=='{artist_name.lower()}'")[
                    ["track_id", "name", "artist_ids"]
                ]
                .drop_duplicates()
                .assign(rotate=get("rotate", additional_track, False))
            )
        additional_track_frames.append(library_track)

    return pd.concat(additional_track_frames).reset_index()


def hydrate_track_frame(
    track_frame: pd.DataFrame,
    artists: pd.DataFrame,
    audio_features: pd.DataFrame,
) -> pd.DataFrame:
    exploded_tracks = track_frame.assign(
        artist_id=track_frame.artist_ids.apply(split_pipe_delimited_string)
    ).explode("artist_id")
    tracks_artists = (
        exploded_tracks.merge(artists, on="artist_id")[
            ["track_id", "artist_name", "genres"]
        ]
        .groupby("track_id")
        .agg(
            artist_names=pd.NamedAgg("artist_name", pipe_compose),
            genres=pd.NamedAgg("genres", pipe_compose),
        )
    )
    return track_frame.merge(tracks_artists, on="track_id").merge(
        audio_features, on="track_id"
    )


def main(
    playlist_config_file: str,
    library_file: str,
    audio_features_file: str,
    artists_file: str,
    root_playlist_file: str,
):
    logger.info(f"Loading playlist conf from {playlist_config_file}.")
    playlist_config = OmegaConf.load(playlist_config_file)

    logger.info(f"Loading library from {library_file}.")
    library = pd.read_csv(library_file)

    logger.info(f"Loading audio features from {audio_features_file}.")
    audio_features = pd.read_csv(audio_features_file)

    logger.info(f"Loading artists from {artists_file}.")
    artists = pd.read_csv(artists_file)

    if "audio_features" in playlist_config:
        logger.info("Processing audio features.")
        audio_features_processed = process_audio_features(
            audio_features, dict(playlist_config.audio_features)
        )
    else:
        audio_features_processed = audio_features.copy()

    if "artists" in playlist_config:
        logger.info("Processing artists.")
        artists_processed = process_artists(
            artists, list(playlist_config.artists)
        )
    else:
        artists_processed = artists.copy()

    if "genres" in playlist_config:
        logger.info("Processing genres.")
        artists_processed = process_genres(
            artists_processed, list(playlist_config.genres)
        )
    else:
        artists_processed = artists_processed.copy()

    # Do the joins to get the almost-final list of track / track names.
    logger.info("Creating core playlist frame.")
    playlist_frame = create_playlist_frame(
        library, artists_processed, audio_features_processed
    )

    # Add additional tracks.
    logger.info("Adding additional tracks to the core playlist frame.")
    if "additional_tracks" in playlist_config:
        additional_tracks = get_additional_tracks(
            library, artists, list(playlist_config.additional_tracks)
        )

        playlist_frame = pd.concat([playlist_frame, additional_tracks])

    # Hydrate the playlist frame with additional info for debugging.
    logger.info("Hydrating the playlist frame.")
    hydrated_playlist_frame = hydrate_track_frame(
        playlist_frame, artists, audio_features
    )

    logger.info(
        f"Writing a root playlist with {hydrated_playlist_frame.shape[0]} "
        f"to {root_playlist_file}."
    )
    hydrated_playlist_frame.to_csv(root_playlist_file, index=False)


if __name__ == "__main__":
    typer.run(main)