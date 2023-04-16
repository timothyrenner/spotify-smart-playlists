.PHONY: env
## Install for deployment.
install: env
	python -m pip install -e .

.PHONY: install-dev
## Install for development.
install-dev: install dev-env
	python -m pip install -e ".[dev]"

.PHONY: build
## Compile the dependency files from pyproject.
build: pyproject.toml
	pip-compile pyproject.toml \
		--output-file=deps/requirements.txt
	pip-compile pyproject.toml \
		--extra dev \
		--output-file=deps/dev-requirements.txt

.PHONY: env
## Install non-dev dependencies.
env: build
	pip-sync deps/requirements.txt

.PHONY: dev-env
## Install dev and non-dev dependencies.
dev-env: build
	pip-sync deps/dev-requirements.txt

.PHONY: lint
## Lint project with ruff.
lint:
	python -m ruff .

.PHONY: format
## Format imports and code.
format:
	python -m ruff . --fix
	python -m black .

.PHONY: check
## Check linting and formatting.
check:
	python -m ruff check .
	python -m black --check .
	python -m mypy .

.PHONY: build-deployments
## Builds the two deployments for this project.
build-deployments:
	cd pipeline && \
	prefect deployment build \
		update_smart_playlists_docker:main \
		--name update-smart-playlists \
		--output update-smart-playlists-deployment.yaml \
		--pool spotify-agent-pool \
		--work-queue default \
		--infra-block process/spotify-local \
		--storage-block gcs/spotify-smart-playlists-storage && \
	prefect deployment build \
		update_recent_tracks_docker:main \
		--name update-recent-tracks \
		--output update-recent-tracks-deployment.yaml \
		--pool spotify-agent-pool \
		--work-queue default \
		--infra-block process/spotify-local \
		--storage-block gcs/spotify-recent-tracks-storage

.PHONY: apply-deployments
## Applies the two deployments for this project.
apply-deployments:
	prefect deployment apply pipeline/update-recent-tracks-deployment.yaml
	prefect deployment apply pipeline/update-smart-playlists-deployment.yaml

.PHONY: pull-database
## Pulls database for testing.
pull-database:
	gsutil cp gs://trenner-datasets/spotify/spotify.db pipeline/spotify.db
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