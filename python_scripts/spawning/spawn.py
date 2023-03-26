import psycopg2

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

    spawn_herbs_in_geohash("u336r8", cursor)


if __name__ == "__main__":
    main()
