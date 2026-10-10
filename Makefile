# Standalone module: not part of ../go.work, so every go command runs with
# GOWORK=off.
#   make run                       # uses .env
#   make run ENV=../noc_automation/.env
#   make build && ./bin/pipeline-report --env .env
export GOWORK := off

ENV ?= .env

.PHONY: run
run:
	go run . --env $(ENV)

.PHONY: build
build:
	go build -o bin/pipeline-report .

.PHONY: test
test:
	go vet ./...
	go test ./...

.PHONY: tidy
tidy:
	go mod tidy

# Docker: config from .env, host network (see compose.yaml).
.PHONY: docker-up
docker-up:
	docker compose up -d --build

.PHONY: docker-down
docker-down:
	docker compose down

.PHONY: docker-logs
docker-logs:
	docker compose logs -f
