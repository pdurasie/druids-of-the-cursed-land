import psycopg2

from python_scripts.spawning.spawn_herbs import spawn_herbs_in_geohash

# Connect to the database
conn = psycopg2.connect(
    database="docker",
    user="docker",
    password="docker",
    host="postgis_db",
    port="5432",
)
cursor = conn.cursor()

spawn_herbs_in_geohash("u33d", cursor)
