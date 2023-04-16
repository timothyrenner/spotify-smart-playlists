import spotipy
import polars as pl
from loguru import logger
from typing import List, Tuple
from dataclasses import dataclass, asdict
from toolz import partition_all


@dataclass
class Artist:
    id: str
    name: str


@dataclass
class ArtistGenre:
    artist_id: str
    genre: str


def pull_artists(
    spotify: spotipy.Spotify, artists_to_pull: List[str], logger=logger
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.info(f"Batching {len(artists_to_pull)} into batches of 50.")
    new_artists: List[Artist] = []
    new_artist_genres: List[ArtistGenre] = []
    for artist_batch in partition_all(50, artists_to_pull):
        logger.info(f"Pulling {len(artist_batch)} artists.")
        artists_response = spotify.artists(artist_batch)
        for artist in artists_response["artists"]:
            new_artists.append(Artist(id=artist["id"], name=artist["name"]))
            for genre in artist["genres"]:
                new_artist_genres.append(
                    ArtistGenre(artist_id=artist["id"], genre=genre)
                )
    new_artists_frame = pl.from_dicts(map(asdict, new_artists))
    new_artist_genres_frame = pl.from_dicts(map(asdict, new_artist_genres))

    return new_artists_frame, new_artist_genres_frame
