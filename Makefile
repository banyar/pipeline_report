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
