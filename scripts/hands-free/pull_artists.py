import typer
import spotipy
import ibis
import pandas as pd

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from toolz import partition_all
from typing import List, Dict
from sqlalchemy.exc import NoSuchTableError


def main(database: str):
    logger.info("Initializing Spotify client.")
    spotify = spotipy.Spotify(
        client_credentials_manager=spotify_auth(database)
    )

    logger.info("Connecting to database.")
    db = ibis.duckdb.connect(database)

    logger.info("Determining which artists need to be pulled.")
    track_artists = db.table("track_artists")
    artists_to_pull: List[str] = []
    try:
        artists = db.table("artists")
        joined_artists = track_artists.left_join(
            artists,
            predicates=track_artists["artist_id"] == artists["id"],
            suffixes=["_ta", "_a"],
        )
        artists_to_pull = (
            joined_artists.filter(joined_artists.id.isnull())
            .execute()
            .artist_id.unique()
            .tolist()
        )
    except NoSuchTableError:
        logger.info("Table 'artists' doesn't exist. Pulling all artists.")
        artists_to_pull = track_artists.execute().artist_id.unique().tolist()

    logger.info(f"Pulling {len(artists_to_pull)} artists.")
    for artist_batch in partition_all(50, artists_to_pull):
        logger.info(f"Pulling features for {len(artist_batch)} artists.")
        artists_response = spotify.artists(artist_batch)
        new_artists: List[Dict[str, str]] = []
        new_artist_genres: List[Dict[str, str]] = []
        for artist in artists_response["artists"]:
            new_artists.append(
                {
                    "id": artist["id"],
                    "name": artist["name"],
                }
            )
            for genre in artist["genres"]:
                new_artist_genres.append(
                    {
                        "artist_id": artist["id"],
                        "genre": genre,
                    }
                )
        new_artists_frame = pd.DataFrame(new_artists)
        new_artist_genres_frame = pd.DataFrame(new_artist_genres)
        logger.info(f"Inserting {new_artists_frame.shape[0]} artists into db.")
        db.load_data("artists", new_artists_frame, if_exists="append")
        logger.info(
            f"Inserting {new_artist_genres_frame.shape[0]} "
            "artist genres into db."
        )
        db.load_data(
            "artist_genres", new_artist_genres_frame, if_exists="append"
        )

    logger.info("Done.")


if __name__ == "__main__":
    typer.run(main)
