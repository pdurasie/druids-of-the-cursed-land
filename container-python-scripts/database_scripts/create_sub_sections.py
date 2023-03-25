import psycopg2
from wkt_geometry_util import get_intersecting_line, get_intersecting_polygon
from geohash import bbox
from shapely.geometry import Polygon
from shapely.geometry import LineString
import csv
import shapely.wkt as wkt


def get_SQL_command(xmin: float, ymin: float, xmax: float, ymax: float) -> str:
    return """WITH pois_query AS
  (SELECT DISTINCT ON (\"name\") *
   FROM planet_osm_point
   WHERE \"amenity\" IN ('arts_centre',
                       'art_school',
                       'auditorium',
                       'college',
                       'contemporary_art_gallery',
                       'library',
                       'meditation_centre',
                       'music_school',
                       'language_school',
                       'school',
                       'theatre',
                       'university')
     OR \"tourism\" IN ('arts_centre',
                      'artwork',
                      'attraction',d
                      'gallery',
                      'museum',
                      'viewpoint',
                      'zoo')), polygons_query AS (
SELECT *
FROM public.planet_osm_polygon
WHERE (\"access\" IN ('yes',
                    'permissive',
                    'permit',
                    'destination',
                    'designated')
       OR \"access\" IS NULL)
  AND (\"foot\" IN ('yes',
                  'designated',
                  'permissive')
       OR \"leisure\"='park'
       OR \"landuse\"='forest'
       OR \"natural\"='water'
       OR \"highway\" IN ('residential',
                        'living_street',
                        'pedestrian',
                        'footway',
                        'bridleway',
                        'path',
                        'sidewalk'))), roads_query AS (
SELECT t.*
FROM (SELECT *, row_number()
                 OVER(
                   ORDER BY osm_id ASC) AS row
FROM   PUBLIC.planet_osm_line
WHERE  ( \"access\" IN ( 'yes', 'designated' )
          OR \"access\" IS NULL )
       AND ( \"highway\" IN ( 'living_street', 'bridleway' ) )
       AND St_length(St_transform(way, 3857)) > 100
	  ) t
UNION ALL
SELECT t.*
FROM   (SELECT *,
               Row_number()
                 OVER(
                   ORDER BY osm_id ASC) AS row
        FROM   PUBLIC.planet_osm_line
        WHERE  ( \"access\" IN ( 'yes', 'designated' )
                  OR \"access\" IS NULL )
		AND St_length(St_transform(way, 3857)) > 250
               AND ( \"highway\" = 'footway' ))
t WHERE  t.row % 2 = 0
)

SELECT \"osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",\"way\"
FROM polygons_query
WHERE ST_Contains(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\")
   OR ST_Intersects(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\");
UNION ALL
SELECT \"osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",\"way\"
FROM roads_query
WHERE ST_Contains(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\")
   OR ST_Intersects(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\");
UNION ALL
SELECT \"osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", NULL AS \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",\"way\"
FROM pois_query
WHERE ST_Contains(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\")
   OR ST_Intersects(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\");""".format(
        min_lon=xmin, min_lat=ymin, max_lon=xmax, max_lat=ymax
    )


# Connect to the database
conn = psycopg2.connect(
    database="docker", user="docker", password="docker", host="postgis_db", port="5432"
)
cur = conn.cursor()

geo_hashes = []

with open("../berlin_data/geohashes_berlin_7.csv", newline="") as csvfile:
    objects = csv.reader(csvfile, delimiter=",")
    for object in objects:
        geo_hashes.append(object)

# If berlin_regions table does not exist, create it
# If it exists, drop it and create it again
cur.execute("DROP TABLE IF EXISTS berlin_regions")
cur.execute(
    """
CREATE TABLE berlin_regions(
    id SERIAL PRIMARY KEY,
    osm_id bigint,
    geohash text,
    access text,
    amenity text,
    area text,
    barrier text,
    bicycle text,
    brand text,
    bridge text,
    boundary text,
    building text,
    culvert text,
    embankment text,
    foot text,
    harbour text,
    highway text,
    landuse text,
    leisure text,
    lock text,
    name text,
    "natural" text,
    place text,
    surface text,
    tourism text,
    tracktype text,
    water text,
    waterway text,
    wetland text,
    wood text,
    tags text,
    area_fraction float,
    way geometry
)
"""
)


for geohash in geo_hashes:
    # Retrieve the polygons from the database that fit the given geohash
    # also save some logs when there is a geohash that has no region in it
    xmin, ymin, xmax, ymax = bbox(geohash)
    # cur.execute(get_SQL_command(xmin, ymin, xmax, ymax))
    cur.execute(
        f"""
    WITH geohash_bbox AS (
  SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('{geohash}'), 4326), 3857) AS geom
)
SELECT
\"osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",ST_AsText(ST_Transform(planet_osm_polygon.way, 4326)) AS \"way\"
FROM
  planet_osm_polygon,
  geohash_bbox
WHERE
  ST_Intersects(planet_osm_polygon.way, geohash_bbox.geom);

    """
    )
    rows = cur.fetchall()

    # Process the polygons
    for row in rows:
        osm_id = row[0]
        access = row[1]
        amenity = row[2]
        area = row[3]
        barrier = row[4]
        bicycle = row[5]
        brand = row[6]
        bridge = row[7]
        boundary = row[8]
        buildings = row[9]
        culvert = row[10]
        embankment = row[11]
        foot = row[12]
        harbour = row[13]
        highway = row[14]
        landuse = row[15]
        leisure = row[16]
        lock = row[17]
        name = row[18]
        natural = row[19]
        place = row[20]
        surface = row[21]
        tourism = row[22]
        tracktype = row[23]
        watermark = row[24]
        waterway = row[25]
        wetland = row[26]
        wood = row[27]
        tags = row[28]
        geometry = wkt.loads(row[29])

        intersecting_geometry = geometry
        # separate the part that intersects with the geohash box from the entire geometry
        if isinstance(intersecting_geometry, LineString):
            intersecting_geometry = get_intersecting_line(geohash, geometry)
        elif isinstance(intersecting_geometry, Polygon):
            intersecting_geometry = get_intersecting_polygon(geohash, geometry)

        # Create a new row which holds all the information of the original row, only replacing the geometry with the intersecting geometry if that is non null, otherwise the original geometry. Also add the geohash to the row and the fraction of the area the geometry covers of the geohash.
        to_be_inserted_geometry = (
            intersecting_geometry if intersecting_geometry is not None else geometry
        )

        new_row = (
            osm_id,
            geohash,
            access,
            amenity,
            area,
            barrier,
            bicycle,
            brand,
            bridge,
            boundary,
            buildings,
            culvert,
            embankment,
            foot,
            harbour,
            highway,
            landuse,
            leisure,
            lock,
            name,
            natural,
            place,
            surface,
            tourism,
            tracktype,
            watermark,
            waterway,
            wetland,
            wood,
            tags,
            to_be_inserted_geometry.area / geometry.area,
            to_be_inserted_geometry.wkt,
        )

        # Insert the new row into the database
        cur.execute(
            """
            INSERT INTO berlin_regions (
                osm_id, geohash, access, amenity, area, barrier, bicycle, brand, bridge, boundary,
                building, culvert, embankment, foot, harbour, highway, landuse, leisure, lock,
                name, "natural", place, surface, tourism, tracktype, water, waterway, wetland,
                wood, tags, area_fraction, way
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            new_row,
        )

    # Print out the number of rows that were inserted and the original number of rows
    cur.execute("SELECT COUNT(*) FROM berlin_regions")
    print("Number of rows in berlin_regions: ", cur.fetchone()[0])
    print("\n")
    print("Number of rows in original table: ", len(rows))
    # cur.execute(
    # "INSERT INTO berlin_regions (\"osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", NULL AS \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",\"way\") VALUES (%s, ST_GeomFromText(%s));", (parent_id, geometry_wkt))

# Commit the changes and close the connection
conn.commit()
conn.close()
