import spotipy
import polars as pl
import loguru
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import List
from toolz import get
from dateutil.parser import parse


@dataclass
class TrackPlay:
    track_id: str
    played_at: datetime


def pull_recent_tracks(
    spotify: spotipy.Spotify,
    max_played_at: datetime | None,
    logger=loguru.logger,
) -> pl.DataFrame | None:
    logger.info("Getting most recently played tracks.")
    recent_tracks_response = spotify.current_user_recently_played()

    if not max_played_at:
        max_played_at = datetime(
            year=1970, month=1, day=1, hour=0, minute=0, second=0
        )

    recent_tracks: List[TrackPlay] = []
    for recent_track in get("items", recent_tracks_response, []):
        played_at = parse(recent_track["played_at"])
        if played_at > max_played_at:
            recent_tracks.append(
                TrackPlay(
                    track_id=recent_track["track"]["id"], played_at=played_at
                )
            )
    if recent_tracks:
        logger.info(f"Found {len(recent_tracks)} new track plays.")
        return pl.from_dicts(map(asdict, recent_tracks))
    else:
        logger.info("No new track plays.")
        return None
