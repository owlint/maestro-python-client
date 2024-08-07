#!/bin/sh

set -eu

container="$(docker compose -p "$1" ps -q "$2")"
format="{{ (index (index .NetworkSettings.Ports \"$3\") 0).HostPort }}"
docker inspect --format="$format" "$container"
