import math
import psycopg2
import random
from shapely import wkt
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

from python_scripts.geohashes.geohash_util import get_neighbor_pairs


def generate_random_point_in_geometry(geometry):
    if isinstance(geometry, Point):
        random_point = geometry
    elif isinstance(geometry, Polygon):
        random_point = generate_random_point_in_polygon(geometry)
    else:  # LineString
        random_point = generate_random_point_near_linestring(geometry)
    return random_point


def generate_random_point_in_polygon(polygon):
    min_x, min_y, max_x, max_y = polygon.bounds
    while True:
        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)]
        )
        if random_point.within(polygon):
            return random_point


def generate_random_point_near_linestring(linestring, distance=0.0001):
    point_on_line = random.choice(list(linestring.coords))
    angle = random.uniform(0, 2 * math.pi)
    random_point = Point(
        point_on_line[0] + distance * math.cos(angle),
        point_on_line[1] + distance * math.sin(angle),
    )
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
        geohash, orientation=random.choice["vertical, horizontal"]
    )

    # Go through all the neighbor pairs
    for neighbor_pair in neighbor_pairs:
        hash1 = neighbor_pair[0]
        hash2 = neighbor_pair[1]
        # Read the records from the database which are within the two geohashes
        cursor.execute(
            """
            SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
            FROM berlin_regions
            WHERE geohash IN (%s, %s);
        """,
            (hash1, hash2),
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

        random_geometry = random.choice(data.geometry)
        random_point = generate_random_point_in_geometry(random_geometry)

        lat, lon = random_point.y, random_point.x

        insert_herb_into_database(lat, lon, cursor)

        # Add bonus herb in parks and forests
        park_forest_regions = [
            region
            for region in data
            if region.leisure == "park" or region.landuse == "forest"
        ]
        if park_forest_regions:
            random_region = random.choice(park_forest_regions).geometry
            random_point = generate_random_point_in_geometry(random_region)
            lat, lon = random_point.y, random_point.x
            insert_herb_into_database(lat, lon, cursor)
