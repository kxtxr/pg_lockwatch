#!/usr/bin/env bash
set -e

if [ "${1:0:1}" = "-" ]; then
    set -- postgres "$@"
fi

if [ "$1" = "postgres" ]; then
    set -- "$@" \
        -c shared_preload_libraries=pg_lockwatch \
        -c "lockwatch.database=${POSTGRES_DB:-postgres}"
fi

exec docker-entrypoint.sh "$@"
