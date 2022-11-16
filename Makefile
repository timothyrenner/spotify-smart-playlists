DB = spotify.db
.PHONY: library
library:
	python scripts/hands-free/pull_library.py $(DB)
	python scripts/hands-free/pull_artists.py $(DB)
	python scripts/hands-free/pull_audio_features.py $(DB)
	@$(foreach file, $(wildcard playlists/*.yaml), python scripts/hands-free/make_root_playlist.py $(DB) $(file);)

.PHONY: recent_tracks
recent_tracks:
	python scripts/hands-free/pull_recent_tracks.py $(DB)

.PHONY: load_playlists
load_playlists: 
	@$(foreach file, $(wildcard playlists/*.yaml), python scripts/hands-free/load_playlist.py $(DB) $(file);)
