import typer
import ibis
import warnings

from omegaconf import OmegaConf
from loguru import logger
from typing import Dict, Any, List
from functools import reduce
from toolz import get
from ibis.expr.types import Table

# This gets old real damn quick idgaf about pandas indices that's why I'm using
# duck in the first place.
warnings.filterwarnings(
    "ignore", message="duckdb-engine doesn't yet support reflection on indices"
)


def get_additional_tracks(
    library: Table,
    artist_tracks: Table,
    artists: Table,
    additional_tracks: List[Dict[str, str]],
) -> Table:
    tracks_with_artists = library.join(
        artist_tracks,
        predicates=(library["track_id"] == artist_tracks["track_id"]),
        suffixes=("", "_r"),
    )
    tracks_with_artists = tracks_with_artists.join(
        artists, predicates=(artists["id"] == tracks_with_artists["artist_id"])
    ).select("track_id", "track_name", "name")
    # There _has_ to be an easier way to rename a column but I cannot find it
    # anywhere in the docs.
    tracks_with_artists = tracks_with_artists.mutate(
        artist_name=tracks_with_artists["name"]
    ).drop("name")

    additional_track_tables: List[Table] = []
    for additional_track in additional_tracks:
        additional_track_name = additional_track["name"]
        # First get all tracks that share the track name.
        additional_track_expr = (
            tracks_with_artists["track_name"] == additional_track_name
        )
        # If there's an artist disambiguate with an additional predicate.
        if "artist" in additional_track:
            artist_name = additional_track["artist"]
            additional_track_expr = additional_track_expr & (
                tracks_with_artists["artist_name"] == artist_name
            )
        additional_track = tracks_with_artists.filter(
            additional_track_expr
        ).select("track_id")
        # Add whether to rotate the track.
        rotate = get("rotate", additional_track, True)
        additional_track = additional_track.mutate(rotate=rotate)
        additional_track_tables.append(additional_track)
    # Now union them all.
    return reduce(lambda a, x: a.union(x), additional_track_tables)


def create_audio_feature_filter(
    audio_features: Table, audio_features_config: Dict[str, Any]
) -> ibis.Expr:
    filters: List[ibis.Expr] = []
    for feature, values in audio_features_config.items():
        if "min" in values:
            filters.append(audio_features[feature] >= values["min"])
        if "max" in values:
            filter.append(audio_features[feature] <= values["max"])

    # Combines each individual filter expression by "and"-ing them.
    audio_feature_filter = reduce(lambda a, x: a & x, filters)
    return audio_feature_filter


def main(database: str, playlist_config_file: str):
    logger.info(f"Loading playlist conf from {playlist_config_file}.")
    playlist_config = OmegaConf.load(playlist_config_file)

    logger.info("Connecting to database.")
    db = ibis.duckdb.connect(database)

    audio_features = db.table("track_audio_features")
    if "audio_features" in playlist_config:
        logger.info("Processing audio features.")
        audio_feature_filter = create_audio_feature_filter(
            audio_features, playlist_config["audio_features"]
        )
        audio_features_filtered = audio_features.filter(audio_feature_filter)
    else:
        audio_features_filtered = audio_features

    artists = db.table("artists")
    if "artists" in playlist_config:
        logger.info("Processing artists.")
        artists_filtered = artists.filter(
            artists["name"].isin(list(playlist_config["artists"]))
        )
    else:
        artists_filtered = artists

    artist_genres = db.table("artist_genres")
    if "genres" in playlist_config:
        logger.info("Processing genres.")
        artist_genres_filtered = artist_genres.filter(
            artist_genres["genre"].isin(list(playlist_config["genres"]))
        )
    else:
        artist_genres_filtered = artist_genres

    # Do the joins to get the almost-final list of track / track names.
    # First get all tracks that match the genres.
    artist_tracks = db.table("track_artists")
    artists_genres_filtered = artists_filtered.join(
        artist_genres_filtered,
        predicates=(
            artists_filtered["id"] == artist_genres_filtered["artist_id"]
        ),
    ).select("artist_id")
    # Now filter out all of those that don't match the artists we have
    # filters for (if no filter, this is a no-op).
    artists_filtered_with_track_ids = artists_genres_filtered.join(
        artist_tracks,
        predicates=(
            artists_genres_filtered["artist_id"] == artist_tracks["artist_id"]
        ),
        suffixes=("", "_r"),
    ).select("artist_id", "track_id")

    # Now filter out all tracks that don't match audio feature conditions.
    # This output is the final playlist.
    playlist_tracks = (
        artists_filtered_with_track_ids.join(
            audio_features_filtered,
            predicates=(
                artists_filtered_with_track_ids["track_id"]
                == audio_features_filtered["track_id"]
            ),
            suffixes=("", "_r"),
        )
        .select("track_id")
        .mutate(rotate=True)
    )

    # Add additional tracks that are specified.
    logger.info("Adding additional tracks to the core playlist frame.")
    library_tracks = db.table("library_tracks")
    if "additional_tracks" in playlist_config:
        playlist_tracks = playlist_tracks.union(
            get_additional_tracks(
                library_tracks,
                artist_tracks,
                artists,
                list(playlist_config.additional_tracks),
            )
        )

    logger.info("Executing query.")
    playlist_frame = playlist_tracks.execute()

    logger.info(
        f"Writing a root playlist with {playlist_frame.shape[0]} tracks "
        f"to {playlist_config['name']}."
    )
    db.load_data(playlist_config["name"], playlist_frame, if_exists="replace")


if __name__ == "__main__":
    typer.run(main)
