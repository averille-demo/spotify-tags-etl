#!/bin/bash
# updated: 2023-03-22
SCRIPT_NAME=$(basename "${BASH_SOURCE[0]}")
DOCKER_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
printf "%s starting: %s\n" "$SCRIPT_NAME" "$(date "+%Y-%m-%d %H:%M:%S %p")"

PROJECT_PATH="$(dirname "$DOCKER_PATH")"
if [ ! -f "$PROJECT_PATH/pyproject.toml" ]; then
   printf "ERROR: project '%s' does not contain pyproject.toml\n" "$PROJECT_NAME"
   exit 1
fi
PROJECT=$(sed -n 's/^ *name.*=.*"\([^"]*\)".*/\1/p' "$PROJECT_PATH/pyproject.toml")
VERSION=$(sed -n 's/^ *version.*=.*"\([^"]*\)".*/\1/p' "$PROJECT_PATH/pyproject.toml")

ENV_FILE="$DOCKER_PATH/config/postgres_secret_dev.env"
if [ ! -f "$ENV_FILE" ]; then
   cp "$DOCKER_PATH/config/postgres_example_dev.env" "$ENV_FILE"
   printf "ERROR: update your credentials: %s\n" "$(basename "$ENV_FILE")"
   exit 1
fi

COMPOSE_FILE="$DOCKER_PATH/docker-compose-dev.yaml"
if [ -f "$COMPOSE_FILE" ];
then
  printf "\nRebuilding container: %s v%s\n" "$PROJECT" "$VERSION"
  docker-compose -f "$COMPOSE_FILE" --env-file "$ENV_FILE" down --remove-orphans
  docker-compose -p "$PROJECT" -f "$COMPOSE_FILE" --env-file "$ENV_FILE" up --build -d
fi

printf "%s completed: %s\n" "$SCRIPT_NAME" "$(date "+%Y-%m-%d %H:%M:%S %p")"
