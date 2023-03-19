#!/bin/bash

psql --no-password \
      -h "$PG_PORT_5432_TCP_ADDR" -p "$PG_PORT_5432_TCP_PORT" \
      -U "$PG_ENV_POSTGRES_USER" "$PG_ENV_POSTGRES_DB" \
      -c "CREATE EXTENSION hstore"

osm2pgsql -v \
            -k \
            --create \
            --slim \
            --cache 4000 \
            --extra-attributes \
            --host "$PG_PORT_5432_TCP_ADDR" \
            --database "$PG_ENV_POSTGRES_DB" \
            --username "$PG_ENV_POSTGRES_USER" \
            --port "$PG_PORT_5432_TCP_PORT" \
            /usr/local/bin/berlin.osm.pbf