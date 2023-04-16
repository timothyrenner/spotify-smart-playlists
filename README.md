# spotify-smart-playlists
Personal project for automatically building out some playlists.
Readme is here so I don't forget how this works in a month.
I threw this together very quickly.
Code probably needs at least a couple of once-overs but it should mostly work at least.

## Why?
I used to have these iTunes smart playlists that were really sophisticated, removing tracks out of rotation at varying intervals, categorizing them, etc all automatically.
I want something like that for Spotify cause maintaining curated playlists is a lot of work.
I mean not really, but ... I'm lazy.
I always wanted a setup where I could just add tracks as I encounter them (from discover or whatever), and they'd start making their way into the right playlist without much effort.

This project is broken into several steps cause I wanted to start simple and get fancier gradually, learning through the inevitable hiccups around credentials management, Spotify's APIs, etc.

## Setup

Make the environment.

```
conda env create -f environment.yaml
make install-dev
```

Helper functions are in the `spotify_smart_playlists` module, pipelines for gluing everything together are in `pipelines/`.

I've switched this project from using DVC pipelines / github actions to a self hosted [Prefect](https://www.prefect.io/) deployment.
Prefect ain't perfect but it's a better fit for this than DVC pipelines were.
You don't need a Prefect deployment to run the code though, which is something I like about it.

In order to run it, you do need a `.env` file in the project root with the following four entries:

```
SPOTIFY_CLIENT_ID="xxxxxxx"
SPOTIFY_CLIENT_SECRET="xxxxxxxx"
SPOTIFY_REDIRECT_URI="xxxxxxxx"
SPOTIFY_CACHE_FERNET_KEY="xxxxxxxx"
```

Those first three come right from [spotipy](https://spotipy.readthedocs.io/en/2.22.1/), the last one is how I encrypt the credentials in the database.
You'll need to generate that manually via pynacl and obvs ... not share that.

I also personally have a variable pointing to a GCS service account, because my deployment is coupled to GCP.

```
PREFECT_GCS_RW_PATH="xxxxxxx"
```

That's only used by two pipelines - the ones I actually deploy - `pipeline/update_recent_tracks_docker.py` and `pipeline/update_smart_playlists_docker.py`.
I'm not actually using Docker because I can't get that to work yet, but eventually that's what will happen.


The `pipeline/load_smart_playlists.py` pipeline runs everything, end to end, locally.
I also deploy `pipelnie/update_recent_tracks.py` (but the "docker" version) separately because it runs more frequently, since Spotify only saves 50 recent tracks to pull.

## Hands free autorotating playlists

Define playlists as yaml files in `pipeline/playlists`.
Here's a complete schema.

```yaml
name: fast-angry
# Select audio features.
features:
    valence:
        min: 0.5
        max: 0.8
    energy:
        min: 0.6
    instrumentalness:
        max: 0.5	
# Select artists to pick songs from.
artists:
    - "Rancid"
    - "NoFX"
# Select genres to pick songs from.
genres:
    - "punk"
# Select tracks to add no matter what.
additional_tracks:
    - name: "Killing Zone"
      artist: "Rancid"
```

genres, artists, and additional_tracks are all pretty self explanatory.
audio_features are features of the actual songs themselves, and there are a lot of them.
[Spotify](https://developer.spotify.com/documentation/web-api/reference/#objects-index) - look under "Audio Features Object" - has detailed what they are and what they mean in their API documentation.

Save those to `pipeline/playlists` - the Prefect pipeline will make the playlists.
It removes any tracks played in the last week and salts the playlists with recommendations too.
Just like the tracks you like and they'll make their way into these playlists, hands free.
Hence the name 😄.

What the pipeline does is finds all tracks in the library that matches the criteria defined in the file and puts them in what I call a "root" playlist.
This enables me to inspect the playlists (they're tables in the database) to debug.
When it's time to push a playlist to Spotify, the pipeline will remove tracks from the root that were recently played, downsample to about 20, throw in some recommended tracks based on that sample, and push it to Spotify.
It does not save that playlist in the DB but I am considering adding that if I have to do any more debugging.

## Machine learned auto rotating playlists

This is my phase 3.
I haven't even started, and I might not ever get to it, but basically you can treat playlists as labels for songs that you can train classifiers on based on the audio features, etc that Spotify supplies.
That would be a fun, less hands-on way of creating playlists than the config based approach.
