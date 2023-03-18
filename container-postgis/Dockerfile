FROM postgis/postgis
RUN apt-get update
RUN apt-get install -y python3
RUN pip3 install shapely
RUN pip3 install geohash2
RUN apt install osm2pgsql
WORKDIR /data
COPY berlin.osm.pbf .
RUN osm2pgsql berlin.osm.pbf --database=postgres --user=postgis --host=postgis --port=5432