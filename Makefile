.PHONY: help install run check-config up down logs build test coverage lint check release clean

help: ## Show available commands
	@grep -E '^[a-zA-Z_-]+:.*##|^##@' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5); next} {printf "  \033[36mmake %-14s\033[0m %s\n", $$1, $$2}'

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
	bin/lint.sh

check: lint coverage ## Everything (gate a commit on this)

##@ Release

release: ## Cut a release from CHANGELOG.md (usage: make release VERSION=X.Y.Z; then push both)
	bin/release.sh $(VERSION)

clean: ## Remove build and coverage output (all regenerable)
	rm -rf backend/htmlcov backend/coverage.xml backend/coverage.json backend/.coverage \
	       backend/junit coverage-upload .coverage-report.py
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
