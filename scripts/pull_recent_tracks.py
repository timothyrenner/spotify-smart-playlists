import typer
import ibis
import spotipy
import pandas as pd

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from sqlalchemy.exc import NoSuchTableError


def main(database: str):
    logger.info("Initializing spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    logger.info(f"Connecting to {database}.")
    db = ibis.duckdb.connect(database)

    logger.info("Getting last played tracks.")
    try:
        max_date = db.table("play_history").played_at.max().execute()
    except NoSuchTableError:
        logger.info("Table 'play_history' doesn't exist. It will be created.")
        max_date = "1970-01-01"

    logger.info("Pulling recent tracks.")
    recent_tracks_response = spotify.current_user_recently_played()
    recent_tracks = [
        {"track_id": rt["track"]["id"], "played_at": rt["played_at"]}
        for rt in recent_tracks_response["items"]
        if rt["played_at"] > max_date
    ]
    recent_tracks_frame = pd.DataFrame(recent_tracks)
    recent_tracks_frame["played_at"] = pd.to_datetime(
        recent_tracks_frame.played_at
    )
    logger.info(
        f"Adding {recent_tracks_frame.shape[0]} new tracks to play history."
    )
    db.load_data("play_history", recent_tracks_frame, if_exists="append")

    logger.info(" 🎹 All done! 🎹 ")


if __name__ == "__main__":
    typer.run(main)
