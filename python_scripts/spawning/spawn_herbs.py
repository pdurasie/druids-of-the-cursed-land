import math
import psycopg2
import random
from shapely import wkt
from shapely.geometry import Point, MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import Polygon, MultiPolygon

from geohashes.geohash_util import get_geohash_quadrants


def generate_random_point_in_geometry(geometry):
    if isinstance(geometry, Point):
        random_point = geometry
    elif isinstance(geometry, Polygon) or isinstance(geometry, MultiPolygon):
        random_point = generate_random_point_in_polygon(geometry)
    else:  # LineString
        random_point = generate_random_point_on_linestring(geometry)
    return random_point


def generate_random_point_in_polygon(polygon):
    if isinstance(polygon, MultiPolygon):
        polygon = random.choice(list(polygon.geoms))

    min_x, min_y, max_x, max_y = polygon.bounds
    while True:
        random_point = Point(
            [random.uniform(min_x, max_x), random.uniform(min_y, max_y)]
        )
        if random_point.within(polygon):
            return random_point


def generate_random_point_on_linestring(linestring):
    # The function first checks if the input geometry is a MultiLineString. If it is, it selects one of its LineString components randomly. Then, it calculates the length of the selected LineString, generates a random distance between 0 and the length of the LineString, and uses the interpolate method to find a point on the LineString at the given distance. Finally, it returns the generated Point.

    if isinstance(linestring, MultiLineString):
        linestring = random.choice(list(linestring.geoms))

    length = linestring.length
    random_distance = random.uniform(0, length)
    random_point = linestring.interpolate(random_distance)
    return random_point


def insert_herb_into_database(latitude, longitude, cursor):
    cursor.execute(
        """
            INSERT INTO herbs (location)
            VALUES (ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        """,
        (
            longitude,
            latitude,
        ),
    )


def spawn_herbs_in_geohash(geohash: str, cursor: psycopg2.extensions.cursor):
    # Get all the neighbor pairs of inner geohashes
    neighbor_pairs = get_geohash_quadrants(geohash)

    # Go through all the neighbor pairs
    for quadrant in neighbor_pairs:
        # Read the records from the database which are within the quadrant geohashes
        cursor.execute(
            """
            (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_polygons
                WHERE geohash IN (%s, %s, %s, %s)
            ) UNION ALL (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_lines
                WHERE geohash IN (%s, %s, %s, %s)
            ) UNION ALL (
                SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
                FROM berlin_points
                WHERE geohash IN (%s, %s, %s, %s)
            );
        """,
            (*quadrant, *quadrant, *quadrant),
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

        park_forest_regions = [
            region
            for region in data
            if region["leisure"] == "park" or region["landuse"] == "forest"
        ]

        other_regions = [region for region in data if region not in park_forest_regions]

        # Spawn 2 herbs in the quadrant
        for _ in range(2):
            if park_forest_regions and other_regions:
                # Give a 50% higher chance for park and forest regions
                random_region_data = random.choices(
                    park_forest_regions + other_regions,
                    weights=[1.5] * len(park_forest_regions) + [1] * len(other_regions),
                    k=1,
                )[0]
            elif park_forest_regions:
                random_region_data = random.choice(park_forest_regions)
            else:
                random_region_data = random.choice(other_regions)

            random_geometry = random_region_data["geometry"]
            random_point = generate_random_point_in_geometry(random_geometry)

            lat, lon = random_point.y, random_point.x

            insert_herb_into_database(lat, lon, cursor)
