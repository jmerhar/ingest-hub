.PHONY: install run check-config up down logs build test coverage lint check release clean

# help, devtools-update and coverage-tooling come from jmerhar/devtools; make fetches them on the
# first run and whenever devtools.toml changes.
.DEFAULT_GOAL := help
include .devtools/make/devtools.mk
.devtools/make/devtools.mk: devtools.toml bin/devtools-fetch.sh
	bin/devtools-fetch.sh

##@ Setup

install: ## Create the backend virtualenv with the test tools
	python3 -m venv backend/.venv
	backend/.venv/bin/pip install --quiet --upgrade pip
	backend/.venv/bin/pip install --quiet -e "backend[test]"

##@ Running

run: ## Run every enabled source from ./data/config/sources.toml (ARGS pass through, e.g. ARGS="--only x")
	DATA_DIR=data backend/.venv/bin/ingest-hub run $(ARGS)

check-config: ## Validate ./data/config/sources.toml and test each destination
	DATA_DIR=data backend/.venv/bin/ingest-hub check $(ARGS)

up: ## Build and start the container
	docker compose -f docker-compose.yml -f docker-compose.build.yml up --build -d

down: ## Stop the container, keeping the data
	docker compose down

logs: ## Follow the container's log
	docker compose logs -f

build: ## Build the image without starting anything
	docker compose -f docker-compose.yml -f docker-compose.build.yml build

##@ Testing

test: ## Run the suite (ARGS pass through to pytest)
	bin/test-backend.sh $(ARGS)

coverage: ## Run the suite with coverage and print the summary
	bin/coverage.sh --format md

lint: ## ShellCheck, ruff, versions, compose files
	.devtools/bin/lint.sh

check: lint coverage ## Everything (gate a commit on this)

##@ Release

release: ## Cut a release from CHANGELOG.md (usage: make release VERSION=X.Y.Z; then push both)
	.devtools/bin/release.sh $(VERSION)

clean: ## Remove build and coverage output (all regenerable)
	rm -rf backend/htmlcov backend/coverage.xml backend/coverage.json backend/.coverage \
	       backend/junit coverage-upload
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
