import spotipy
import polars as pl
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Tuple
from dateutil.parser import parse
import loguru


@dataclass
class LibraryTrack:
    date_added: datetime
    track_id: str
    track_name: str


@dataclass
class TrackArtist:
    track_id: str
    artist_id: str


def pull_library_tracks(
    spotify: spotipy.Spotify, logger=loguru.logger
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    logger.info("Calling Spotify for library tracks.")
    library_tracks_response = spotify.current_user_saved_tracks(limit=50)

    library_tracks: List[LibraryTrack] = []
    track_artists: List[TrackArtist] = []
    while library_tracks_response:
        for track in library_tracks_response["items"]:
            library_tracks.append(
                LibraryTrack(
                    date_added=parse(track["added_at"]),
                    track_id=track["track"]["id"],
                    track_name=track["track"]["name"],
                )
            )
            for artist in track["track"]["artists"]:
                track_artists.append(
                    TrackArtist(
                        track_id=track["track"]["id"], artist_id=artist["id"]
                    )
                )
        logger.info(
            "Calling Spotify for library tracks: "
            f"{len(library_tracks)} pulled so far."
        )
        library_tracks_response = spotify.next(library_tracks_response)
    logger.info(f"Found {len(library_tracks)} tracks in total.")
    library_tracks_frame = pl.from_dicts(
        map(lambda x: asdict(x), library_tracks)
    )
    track_artists_frame = pl.from_dicts(
        map(lambda x: asdict(x), track_artists)
    )
    return library_tracks_frame, track_artists_frame
