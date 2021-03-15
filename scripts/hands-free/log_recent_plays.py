import typer
import pandas as pd
import numpy as np
import os
import spotipy

from loguru import logger
from spotify_smart_playlists.helpers import spotify_auth
from datetime import datetime


def main(play_history_file: str):
    if not os.path.exists(play_history_file):
        logger.warning(f"Recent plays file {play_history_file} doesn't exist.")
        play_history_frame = pd.DataFrame(
            {"track_id": [], "last_played": [], "last_pulled": []}
        )
    else:
        logger.info(f"Loading recent plays from {play_history_file}.")
        play_history_frame = pd.read_csv(play_history_file)

    logger.info("Initializing spotify client.")
    spotify = spotipy.Spotify(client_credentials_manager=spotify_auth())

    last_pull = play_history_frame.last_pulled.max()

    if not np.isnan(last_pull):
        logger.info(f"Pulling recently played tracks after {last_pull}.")
        recent_tracks_response = spotify.current_user_recently_played(
            after=last_pull
        )
    else:
        logger.info("Pulling all recently played tracks.")
        recent_tracks_response = spotify.current_user_recently_played()

    recent_tracks = [
        {
            "track_id": rt["track"]["id"],
            "last_played": rt["played_at"],
        }
        for rt in recent_tracks_response["items"]
    ]
    logger.info(
        f"Pulled {len(recent_tracks)} new tracks. "
        "Marshalling into data frame."
    )
    current_pull_time = int(datetime.utcnow().timestamp()) * 1000
    recent_tracks_frame = pd.DataFrame(recent_tracks).assign(
        last_pulled=current_pull_time
    )

    logger.info("Updating play history with recent plays.")
    play_history_frame = pd.concat([play_history_frame, recent_tracks_frame])
    # This is awful.
    play_history_frame.loc[
        :, "last_pulled"
    ] = play_history_frame.last_pulled.astype(int)

    logger.info(f"Saving play history to {play_history_file}.")
    play_history_frame.to_csv(play_history_file, index=False)

    logger.info(" 🎹  All done!  🎹")


if __name__ == "__main__":
    typer.run(main)
