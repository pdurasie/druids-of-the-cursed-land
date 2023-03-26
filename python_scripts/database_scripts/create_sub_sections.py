from sql_commands import (
    get_insert_command,
    get_sql_query,
    get_table_creation_query,
)
import psycopg2
import csv
from shapely.geometry import Polygon, LineString
from shapely.wkt import loads as wkt_loads
from wkt_geometry_util import (
    get_intersecting_polygon,
    get_intersecting_line,
    get_polygon_from_geohash,
)


def process_osm_data(source_table_name, target_table_name, geohashes):
    # Connect to the database
    conn = psycopg2.connect(
        database="docker",
        user="docker",
        password="docker",
        host="postgis_db",
        port="5432",
    )
    cur = conn.cursor()

    # If the table exists, drop it and create it again
    cur.execute(f"DROP TABLE IF EXISTS {target_table_name}")
    cur.execute(get_table_creation_query(source_table_name))

    for index, geohash in enumerate(geohashes):
        # Print progress every 1000 geohashes
        if index % 1000 == 0:
            print(f"Processing {source_table_name}...({index/len(geohashes)*100:.2f}%)")

        # Fetch the data
        cur.execute(get_sql_query(source_table_name, geohash))
        rows = cur.fetchall()

        # Process the geometries
        for row in rows:
            new_row = _create_new_row(row, geohash, target_table_name)
            if new_row is not None:
                cur.execute(get_insert_command(target_table_name), new_row)

    print("\nDone processing " + source_table_name + ".\n")
    # Commit the changes and close the connection
    conn.commit()
    conn.close()


def _create_new_row(row, geohash, table_name):
    if "point" in table_name:
        return _create_new_point_row(row, geohash)
    else:
        return _create_new_line_or_polygon_row(row, geohash, table_name)


def _create_new_line_or_polygon_row(row, geohash, geometry_type):
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
    geometry = wkt_loads(row[29])

    if "line" in geometry_type:
        intersecting_geometry = get_intersecting_line(geohash, geometry)
    else:  # geometry_type == 'polygon'
        intersecting_geometry = get_intersecting_polygon(geohash, geometry)

    to_be_inserted_geometry = (
        intersecting_geometry if intersecting_geometry is not None else geometry
    )

    len_or_area_fraction = None
    if "polygon" in geometry_type:
        # Get the fraction of the area of the intersecting geometry to the area of the geohash bounding box
        geohash_poly = get_polygon_from_geohash(geohash)
        area_fraction = to_be_inserted_geometry.area / geohash_poly.area

        # Only insert the row if the area fraction is greater than 0.005 (for polygons)
        if area_fraction is not None and area_fraction < 0.005:
            return
        else:
            len_or_area_fraction = area_fraction
    elif "line" in geometry_type:
        len_or_area_fraction = to_be_inserted_geometry.length

    return (
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
        len_or_area_fraction,
        to_be_inserted_geometry.wkt,
    )


def _create_new_point_row(row, geohash):
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
    watermark = row[23]
    waterway = row[24]
    wetland = row[25]
    wood = row[26]
    tags = row[27]
    geometry = wkt_loads(row[28])

    return (
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
        watermark,
        waterway,
        wetland,
        wood,
        tags,
        geometry.wkt,
    )


# Create dictionaries of source table name + target table name
tables = [
    # ["planet_osm_polygon", "berlin_polygons"],
    # ["planet_osm_point", "berlin_points"],
    ["planet_osm_line", "berlin_lines"],
]
geo_hashes = []

with open("berlin_data/geohashes_berlin_7.csv", newline="") as csvfile:
    objects = csv.reader(csvfile, delimiter=",")
    for object in objects:
        for geohash in object:
            geo_hashes.append(geohash)

for table in tables:
    process_osm_data(table[0], table[1], geo_hashes)
