FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update
RUN apt-get install -y \
        osm2pgsql \
        postgresql-client-15 \
        osmctools

# clean up
RUN apt-get clean \
        && rm -rf /var/lib/apt/lists/*

COPY --chmod=755 ./osm-importer.sh /usr/local/bin/osm-importer.sh
COPY --chmod=755 ./osm-entrypoint.sh /usr/local/bin/osm-entrypoint.sh
COPY berlin.osm.pbf /usr/local/bin/berlin.osm.pbf

WORKDIR /osm