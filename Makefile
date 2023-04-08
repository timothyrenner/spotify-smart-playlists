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

####### NEWER COMMANDS HERE #######

.PHONY: env dev-env lint format check

## Install for deployment.
install:
	python -m pip install -e .

## Install for development.
install-dev: install
	python -m pip install -e ".[dev]"

build:
	pip-compile pyproject.toml \
		--resolver=backtracking \
		--output-file=deps/requirements.txt
	pip-compile pyproject.toml \
		--resolver=backtracking \
		--extra dev \
		--output-file=deps/dev-requirements.txt

## Install non-dev dependencies.
env:
	pip-sync deps/requirements.txt

## Install dev and non-dev dependencies.
dev-env:
	pip-sync deps/dev-requirements.txt

## Lint project with ruff.
lint:
	python -m ruff .

## Format imports and code.
format:
	python -m ruff . --fix
	python -m black .

## Check linting and formatting.
check:
	python -m ruff check .
	python -m black --check .

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available commands:$$(tput sgr0)"
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')