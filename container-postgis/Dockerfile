FROM postgis/postgis
RUN apt-get update
RUN apt install -y osm2pgsql
WORKDIR /data
COPY berlin.osm.pbf .
RUN osm2pgsql berlin.osm.pbf --database=postgres --user=postgis --host=postgis --port=5432