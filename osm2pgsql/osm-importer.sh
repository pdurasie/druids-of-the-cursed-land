#!/bin/bash

osm2pgsql -v \
            -k \
            --create \
            --slim \
            --cache 4000 \
            --extra-attributes \
            --host 5432 \
            --database docker \
            --username docker \
            --port 5432 \
            berlin.osm.pbf