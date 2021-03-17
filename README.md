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

Obviously this is just for me but if you're forking this or something these are the actual things you need to do, but you might want to change the repo secret names.
Just be sure to update the workflows that use them (`.github/workflows`).

In order to persist the secrets to github (meaning the token can refresh entirely within actions), you need a personal access token with repo-level scope.
Store it as `GH_SPOTIFY_ACCESS_TOKEN`.

In order to access Spotify's API, you'll need a client / secret ID.
Hit up the Spotify developer dashboard to create an app and get one.
You also need to set up a redirect URI (see [this article](https://spotipy.readthedocs.io/en/2.17.1/#authorization-code-flow)) - this requires the authorization flow.
Store those as `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `SPOTIFY_REDIRECT_URI` in repository secrets.

In order to use Google Cloud as a DVC remote, you need a service account with "Object Admin" permissions on the bucket for your DVC remote.
Download those keys and store them as a repo secret as `MUSIC_DVC_PUSHER_SA_CREDS`.
You'll also need the project id as a secret, `PERSONAL_PROJECT_ID`.

The last thing you need to do is run the scripts locally at least once, so that you can perform the authorization flow and grant the app the Spotify scopes it needs.
Actions can't do this for you.

## Simple autorotating playlists

This one's pretty simple - it takes some seed playlists and rotates tracks, salting them with recommendations.
Any tracks in "recently played" (up to 50), will be removed with each run.
Playlists and stuff are hard coded in the `simple-playlists` GH workflow.

## Hands free autorotating playlists

This one's a lot cooler, but it's also a lot more complicated.
Define playlists as yaml files in `playlists`.
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

Once those playlists are defined, add them to `params.yaml`, and the DVC pipeline will pick them up and build playlists out of them.
It removes any tracks played in the last week and salts the playlists with recommendations too.
Just like the tracks you like and they'll make their way into these playlists, hands free.
Hence the name 😄.

## Machine learned auto rotating playlists

This is my phase 3.
I haven't even started, and I might not ever get to it, but basically you can treat playlists as labels for songs that you can train classifiers on based on the audio features, etc that Spotify supplies.
That would be a fun, less hands-on way of creating playlists than the config based approach.