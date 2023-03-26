import psycopg2

from geohashes.lower_prec_geohashes import get_geohashes_of_higher_precision

from .spawn_herbs import spawn_herbs_in_geohash


def main():
    # Connect to the database
    conn = psycopg2.connect(
        database="docker",
        user="docker",
        password="docker",
        host="postgis_db",
        port="5432",
    )
    cursor = conn.cursor()

    # Spawn herbs in all children geohashes of u336r:
    geohashes = get_geohashes_of_higher_precision("u336r")
    for geohash in geohashes:
        spawn_herbs_in_geohash(geohash, cursor)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
