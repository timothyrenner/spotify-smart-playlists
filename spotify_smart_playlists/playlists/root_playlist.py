from dataclasses import dataclass, asdict, field
from duckdb import DuckDBPyConnection
from loguru import logger
import polars as pl
from typing import List, Dict, Any
from toolz import get


@dataclass
class AudioFeatureConfig:
    min: float | int | None = None
    max: float | int | None = None


@dataclass
class AudioFeaturesConfig:
    acousticness: AudioFeatureConfig | None = None
    danceability: AudioFeatureConfig | None = None
    duration_ms: AudioFeatureConfig | None = None
    energy: AudioFeatureConfig | None = None
    instrumentalness: AudioFeatureConfig | None = None
    key: AudioFeatureConfig | None = None
    liveness: AudioFeatureConfig | None = None
    loudness: AudioFeatureConfig | None = None
    mode: AudioFeatureConfig | None = None
    speechiness: AudioFeatureConfig | None = None
    tempo: AudioFeatureConfig | None = None
    time_signature: AudioFeatureConfig | None = None
    valence: AudioFeatureConfig | None = None


@dataclass
class TrackConfig:
    name: str
    artist: str
    rotate: bool


@dataclass
class PlaylistConfig:
    name: str
    audio_features: AudioFeaturesConfig | None = None
    genres: List[str] = field(default_factory=list)
    artists: List[str] = field(default_factory=list)
    additional_tracks: List[TrackConfig] = field(default_factory=list)


def playlist_config_from_dict(
    playlist_config_dict: Dict[str, Any]
) -> PlaylistConfig:
    audio_features_config: AudioFeaturesConfig | None = None
    if "audio_features" in playlist_config_dict:
        audio_features_config = AudioFeaturesConfig(
            **{
                audio_feature: AudioFeatureConfig(**audio_feature_vals)
                for audio_feature, audio_feature_vals in playlist_config_dict[
                    "audio_features"
                ].items()
            }
        )
    additional_tracks: List[TrackConfig] = [
        TrackConfig(**tc)
        for tc in get("additional_tracks", playlist_config_dict, [])
    ]
    return PlaylistConfig(
        name=playlist_config_dict["name"],
        audio_features=audio_features_config,
        genres=get("genres", playlist_config_dict, []),
        artists=get("artists", playlist_config_dict, []),
        additional_tracks=additional_tracks,
    )


def audio_feature_where(audio_features: AudioFeaturesConfig) -> str:
    clauses: List[str] = []
    for audio_feature, audio_feature_config in asdict(audio_features).items():
        if audio_feature_config is None:
            continue
        if audio_feature_config["min"] is not None:
            clauses.append(
                f"({audio_feature} >= {audio_feature_config['min']})"
            )
        if audio_feature_config["max"] is not None:
            clauses.append(
                f"({audio_feature} <= {audio_feature_config['max']})"
            )
    return " AND ".join(clauses)


def additional_tracks(
    database: DuckDBPyConnection, additional_tracks: List[TrackConfig]
) -> pl.DataFrame:
    # fmt: off
    additional_track_frame = pl.from_dicts( # noqa
        map(asdict, additional_tracks)
    )  
    # fmt: on
    return database.sql(
        """
    SELECT
        lt.track_id,
        atf.rotate
    FROM
        additional_track_frame AS atf
    INNER JOIN
        library_tracks AS lt
        ON lt.track_name = atf.name
    INNER JOIN
        track_artists AS at
        ON at.artist_name = atf.artist
    """
    ).pl()


def make_root_playlist(
    database: DuckDBPyConnection,
    playlist_config: PlaylistConfig,
    logger=logger,
) -> pl.DataFrame:
    genres_filter: str = ""
    if playlist_config.genres:
        genres_filter_list = ", ".join(
            map(lambda x: f"'{x}'", playlist_config.genres)
        )
        genres_filter = f"artist_genres.genre IN ({genres_filter_list})"

    artists_filter: str = ""
    if playlist_config.artists:
        artists_filter_list = ", ".join(
            map(lambda x: f"'{x}'", playlist_config.artists)
        )
        artists_filter = f"artists.name IN ({artists_filter_list})"

    audio_features_filter: str = ""
    if playlist_config.audio_features is not None:
        audio_features_filter = audio_feature_where(
            playlist_config.audio_features
        )

    base_query = """
    SELECT DISTINCT library_tracks.track_id 
    FROM library_tracks
    LEFT JOIN track_artists
        ON library_tracks.track_id = track_artists.track_id
    LEFT JOIN artists
        ON track_artists.artist_id = artists.id
    LEFT JOIN artist_genres
        ON track_artists.artist_id = artist_genres.artist_id
    LEFT JOIN track_audio_features
        ON library_tracks.track_id = track_audio_features.track_id
    """

    filters = " AND\n".join(
        filter(
            lambda x: x, [genres_filter, artists_filter, audio_features_filter]
        )
    )

    query = "\n".join(
        [
            base_query,
            "WHERE",
            filters,
        ]
    )

    logger.info(f"Executing query: {query}")

    base_root_playlist_tracks = database.sql(query).pl()
    return base_root_playlist_tracks


# for testing, this is a fairly complicated query to automate.
def main(database_file: str, playlist_config_file: str):
    import duckdb
    import yaml

    database = duckdb.connect(database_file)
    with open(playlist_config_file, "r") as f:
        playlist_config_dict = yaml.safe_load(f)
    print(playlist_config_dict)
    playlist_config = playlist_config_from_dict(playlist_config_dict)

    make_root_playlist(database, playlist_config)


if __name__ == "__main__":
    import typer

    typer.run(main)
