import psycopg2
from database_scripts.wkt_geometry_util import get_intersecting_line, get_intersecting_polygon
from geohashes.geohash import bbox
from shapely.geometry import Polygon
from shapely.geometry import LineString
import csv
import shapely.wkt as wkt


def get_geohashes_of_higher_precision(geohash: str, precision: int) -> list[str]:
    # Define the base32 character set used by geohash
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

    # Check if the input geohash and precision are valid
    if not isinstance(geohash, str) or not all(c in BASE32 for c in geohash):
        raise ValueError("Invalid geohash")
    if not isinstance(precision, int) or precision < 0:
        raise ValueError("Invalid precision")

    # Get the latitude and longitude range for the current geohash
    lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
    for i in range(precision):
        if i % 2 == 0:
            lon_mid = sum(lon_range) / 2
            if geohash[i] in "bcfguvyz":
                lon_range = (lon_mid, lon_range[1])
            else:
                lon_range = (lon_range[0], lon_mid)
        else:
            lat_mid = sum(lat_range) / 2
            if geohash[i] in "prxz":
                lat_range = (lat_mid, lat_range[1])
            else:
                lat_range = (lat_range[0], lat_mid)

    # Generate all possible geohashes of precision x+1
    geohashes = []
    for c in BASE32:
        geohash_x1 = geohash + c
        lat_range_x1, lon_range_x1 = lat_range, lon_range
        if len(geohash_x1) % 2 == 0:
            lon_mid = sum(lon_range_x1) / 2
            if geohash_x1[-1] in "bcfguvyz":
                lon_range_x1 = (lon_mid, lon_range_x1[1])
            else:
                lon_range_x1 = (lon_range_x1[0], lon_mid)
        else:
            lat_mid = sum(lat_range_x1) / 2
            if geohash_x1[-1] in "prxz":
                lat_range_x1 = (lat_mid, lat_range_x1[1])
            else:
                lat_range_x1 = (lat_range_x1[0], lat_mid)
        if lat_range_x1[0] < lat_range_x1[1] and lon_range_x1[0] < lon_range_x1[1]:
            geohashes.append(geohash_x1)

    return geohashes


def get_SQL_command(xmin: float, ymin: float, xmax: float, ymax: float, srd: int) -> str:
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
                      'attraction',
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
   OR ST_Intersects(ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326), \"way\");""".format(min_lon=xmin, min_lat=ymin, max_lon=xmax, max_lat=ymax)


# Connect to the database
conn = psycopg2.connect(database="your_db_name", user="your_db_user",
                        password="your_db_password", host="localhost", port="5432")
cur = conn.cursor()

geo_hashes = ["u33677", "u33dh4", "u33e5h"]

# with open('./berlin_data/geohashes_berlin_7.csv', newline='') as csvfile:
#     objects = csv.reader(csvfile, delimiter=',')
#     for object in objects:
#         geo_hashes.append(object)

# cur.execute("CREATE TABLE berlin_regions(SERIAL PRIMARY KEY)")

for geohash in geo_hashes:
    # Retrieve the polygons from the database that fit the given geohash
    # also save some logs when there is a geohash that has no region in it
    xmin, ymin, xmax, ymax = bbox(geohash)
    cur.execute(get_SQL_command(xmin, ymin, xmax, ymax))
    rows = cur.fetchall()

    # Process the polygons
    for row in rows:
        print(str(row))
        # parent_id = row[0]
        # access = row[1]
        # amenity = row[2]
        # area = row[3]
        # barrier = row[4]
        # bicycle = row[5]
        # brand = row[6]
        # bridge = row[7]
        # boundary = row[8]
        # buildings = row[9]
        # culvert = row[10]
        # embankment = row[11]
        # foot = row[12]
        # harbour = row[13]
        # highway = row[14]
        # landuse = row[15]
        # leisure = row[16]
        # lock = row[17]
        # name = row[18]
        # natural = row[19]
        # place = row[20]
        # surface = row[21]
        # tourism = row[22]
        # tracktype = row[23]
        # watermark = row[24]
        # waterway = row[25]
        # wetland = row[26]
        # wood = row[27]
        # tags = row[28]
        # geometry = wkt.loads(row[29])

        intersecting_geometry = geometry
        if (isinstance(geometry, LineString)):
            intersecting_geometry = get_intersecting_line(geohash, geometry)
        elif (isinstance(geometry, Polygon)):
            intersecting_geometry = get_intersecting_polygon(geohash, geometry)

        # # Insert the sub polygons into the database
        # # TODO: create the table
        #     geometry_wkt = intersecting_geometry.wkt
        # TODO add more columns to this than id and polygon, also the area it covers!
        # cur.execute(
        #     "INSERT INTO your_table_name (\"id\",\"parent_osm_id\",\"access\",\"amenity\",\"area\",\"barrier\",\"bicycle\",\"brand\",\"bridge\",\"boundary\",\"building\",\"culvert\",\"embankment\",\"foot\",\"harbour\",\"highway\",\"landuse\",\"leisure\",\"lock\",\"name\",\"natural\",\"place\",\"surface\",\"tourism\", NULL AS \"tracktype\",\"water\",\"waterway\",\"wetland\",\"wood\",\"tags\",\"way\") VALUES (%s, ST_GeomFromText(%s));", (parent_id, geometry_wkt))

# Commit the changes and close the connection
conn.commit()
conn.close()
