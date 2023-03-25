import psycopg2

from python_scripts.database_scripts.wkt_geometry_util import get_polygon_from_geohash


def spawn_herbs_in_geohash(geohash, cursor):
    # Get the polygon for the geohash
    polygon = get_polygon_from_geohash(geohash)

    # Get the number of herbs to spawn
    number_of_herbs = _get_number_of_herbs_to_spawn(polygon, cursor)

    # Spawn the herbs
    for _ in range(number_of_herbs):
        # Get a random point in the polygon
        point = polygon.representative_point()

        # Insert the point into the database
        cursor.execute(
            """
            INSERT INTO herbs (geohash, point)
            VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            """,
            (geohash, point.x, point.y),
        )


# connect to the database using psycopg2
conn = psycopg2.connect(
    database="docker",
    user="docker",
    password="docker",
    host="postgis_db",
    port="5432",
)
cur = conn.cursor()
