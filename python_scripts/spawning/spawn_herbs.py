import math
import psycopg2
import random
from shapely import wkt
from shapely.geometry import Point, MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry.polygon import Polygon

from geohashes.geohash_util import get_neighbor_pairs
from wkt.combine_wkt_objects import merge_wkt_objects


def generate_random_point_in_geometry(geometry):
    if isinstance(geometry, Point):
        random_point = geometry
    elif isinstance(geometry, Polygon):
        random_point = generate_random_point_in_polygon(geometry)
    else:  # LineString
        random_point = generate_random_point_on_linestring(geometry)
    return random_point


def generate_random_point_in_polygon(polygon):
    min_x, min_y, max_x, max_y = polygon.bounds
    while True:
        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)]
        )
        if random_point.within(polygon):
            return random_point


def generate_random_point_on_linestring(linestring):
    if isinstance(linestring, MultiLineString):
        linestring = random.choice(list(linestring.geoms))

    point_on_line = random.choice(list(linestring.coords))
    random_point = Point(point_on_line)
    return random_point


def insert_herb_into_database(latitude, longitude, cursor):
    print(f"Inserting herb at {latitude}, {longitude}")
    # cursor.execute(
    #     """
    #     INSERT INTO berlin_herbs (latitude, longitude)
    #     VALUES (%s, %s);
    # """,
    #     (latitude, longitude),
    # )


def spawn_herbs_in_geohash(geohash: str, cursor: psycopg2.extensions.cursor):
    # Get all the neighbor pairs of inner geohashes
    neighbor_pairs = get_neighbor_pairs(
        geohash, orientation=random.choice(["vertical", "horizontal"])
    )
    TEST_geoms = []

    # Go through all the neighbor pairs
    for neighbor_pair in neighbor_pairs:
        hash1 = neighbor_pair[0]
        hash2 = neighbor_pair[1]
        # Read the records from the database which are within the two geohashes
        cursor.execute(
            """
            (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_polygons
                WHERE geohash IN (%s, %s)
            ) UNION ALL (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_lines
                WHERE geohash IN (%s, %s)
            ) UNION ALL (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_points
                WHERE geohash IN (%s, %s)
            );
        """,
            (hash1, hash2, hash1, hash2, hash1, hash2),
        )

        records = cursor.fetchall()
        data = []

        for record in records:
            data.append(
                {
                    "osm_id": record[0],
                    "geometry": wkt.loads(record[1]),
                    "leisure": record[2],
                    "landuse": record[3],
                }
            )

        if not data:
            continue

        random_region_data = random.choice(data)
        random_geometry = random_region_data["geometry"]
        random_point = generate_random_point_in_geometry(random_geometry)

        lat, lon = random_point.y, random_point.x

        # insert_herb_into_database(lat, lon, cursor)
        TEST_geoms.append(Point(lon, lat))

        # Add bonus herb in parks and forests
        park_forest_regions = [
            region
            for region in data
            if region["leisure"] == "park" or region["landuse"] == "forest"
        ]
        if park_forest_regions:
            random_region = random.choice(park_forest_regions)["geometry"]
            random_point = generate_random_point_in_geometry(random_region)
            lat, lon = random_point.y, random_point.x
            # insert_herb_into_database(lat, lon, cursor)
            TEST_geoms.append(Point(lon, lat))

    print(str(MultiPoint(TEST_geoms)))
