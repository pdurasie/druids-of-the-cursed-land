def get_table_creation_query(source_table_name):
    if source_table_name == "planet_osm_point":
        table_creation_query = _get_points_creation_command()
    elif source_table_name == "planet_osm_line":
        table_creation_query = _get_lines_creation_command()
    elif source_table_name == "planet_osm_polygon":
        table_creation_query = _get_polygons_creation_command()
    return table_creation_query


def get_sql_query(source_table_name, geohash):
    if source_table_name == "planet_osm_point":
        sql_query = _get_points_sql_query(geohash)
    elif source_table_name == "planet_osm_line":
        sql_query = _get_lines_sql_query(geohash)
    elif source_table_name == "planet_osm_polygon":
        sql_query = _get_polygons_sql_query(geohash)
    return sql_query


def get_insert_command(target_table_name):
    if "point" in target_table_name:
        insert_command = _get_points_insert_command(target_table_name)
    else:
        insert_command = _get_lines_or_polygons_insert_command(target_table_name)
    return insert_command


def _get_points_insert_command(table_name):
    return f"""
    INSERT INTO {table_name} (
        osm_id, geohash, access, amenity, area, barrier, bicycle, brand, bridge, boundary,
        building, culvert, embankment, foot, harbour, highway, landuse, leisure, lock,
        name, "natural", place, surface, tourism, water, waterway, wetland,
        wood, tags, way
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326));
    """


def _get_lines_or_polygons_insert_command(table_name):
    if "line" in table_name:
        column_name = "length"
    else:
        column_name = "area_fraction"

    return f"""
    INSERT INTO {table_name} (
        osm_id, geohash, access, amenity, area, barrier, bicycle, brand, bridge, boundary,
        building, culvert, embankment, foot, harbour, highway, landuse, leisure, lock,
        name, "natural", place, surface, tourism, tracktype, water, waterway, wetland,
        wood, tags, {column_name}, way
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326));
    """


def _get_points_creation_command():
    return """
        CREATE TABLE berlin_points(
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
            water text,
            waterway text,
            wetland text,
            wood text,
            tags text,
            way geometry
        )
        """


def _get_lines_creation_command():
    return """
        CREATE TABLE berlin_lines(
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
            length float,
            way geometry
        )
        """


def _get_polygons_creation_command():
    return """
        CREATE TABLE berlin_polygons(
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


def _get_points_sql_query(geohash):
    return f"""
 WITH geohash_bbox AS
  (SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('{geohash}'), 4326), 3857) AS geom)
SELECT DISTINCT ON (\"name\") 
        osm_id,
       "access",
       amenity,
       area,
       barrier,
       bicycle,
       brand,
       bridge,
       boundary,
       building,
       culvert,
       embankment,
       foot,
       harbour,
       highway,
       landuse,
       leisure,
       "lock",
       name,
       "natural",
       place,
       surface,
       tourism,
       water,
       waterway,
       wetland,
       wood,
       tags,
       ST_AsText(ST_Transform(way, 4326)) AS "way"
   FROM planet_osm_point, geohash_bbox
   WHERE ST_Intersects(way, geohash_bbox.geom) AND (\"amenity\" IN ('arts_centre',
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
                        'zoo'))
    """


def _get_lines_sql_query(geohash):
    return f"""
WITH geohash_bbox AS
  (SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('{geohash}'), 4326), 3857) AS geom)
SELECT osm_id,
       "access",
       amenity,
       area,
       barrier,
       bicycle,
       brand,
       bridge,
       boundary,
       building,
       culvert,
       embankment,
       foot,
       harbour,
       highway,
       landuse,
       leisure,
       "lock",
       name,
       "natural",
       place,
       surface,
       tourism,
       tracktype,
       water,
       waterway,
       wetland,
       wood,
       tags,
       ST_AsText(ST_Transform(way, 4326)) AS "way"
FROM PUBLIC.planet_osm_line,
     geohash_bbox
WHERE ST_Intersects(way, geohash_bbox.geom)
  AND ("access" IN ('yes',
                    'designated')
       OR "access" IS NULL)
  AND ("highway" IN ('living_street',
                     'bridleway'))
  AND St_length(St_transform(way, 3857)) > 100
UNION ALL
SELECT osm_id,
       ACCESS,
       amenity,
       area,
       barrier,
       bicycle,
       brand,
       bridge,
       boundary,
       building,
       culvert,
       embankment,
       foot,
       harbour,
       highway,
       landuse,
       leisure,
       "lock",
       name,
       "natural",
       place,
       surface,
       tourism,
       tracktype,
       water,
       waterway,
       wetland,
       wood,
       tags,
       ST_AsText(ST_Transform(way, 4326)) AS "way"
FROM PUBLIC.planet_osm_line,
     geohash_bbox
WHERE ST_Intersects(way, geohash_bbox.geom)
  AND ("access" IN ('yes',
                    'designated')
       OR "access" IS NULL)
  AND St_length(St_transform(way, 3857)) > 250
  AND ("highway" = 'footway')
    """


def _get_polygons_sql_query(geohash):
    return f"""
            WITH geohash_bbox AS (
                SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('{geohash}'), 4326), 3857) AS geom
            )
            SELECT
                osm_id, "access", amenity, area, barrier, bicycle, brand, bridge, boundary,
                building, culvert, embankment, foot, harbour, highway, landuse, leisure, "lock",
                name, "natural", place, surface, tourism, tracktype, water, waterway, wetland,
                wood, tags, ST_AsText(ST_Transform(way, 4326)) AS "way"
            FROM
                planet_osm_polygon,
                geohash_bbox
            WHERE
                ST_Intersects(way, geohash_bbox.geom) AND (access IN ('yes',
                            'permissive',
                            'permit',
                            'destination',
                            'designated')
                OR access IS NULL)
            AND (foot IN ('yes',
                          'designated',
                          'permissive')
                OR leisure='park'
                OR landuse='forest'
                OR "natural"='water'
                OR highway IN ('residential',
                            'living_street',
                            'pedestrian',
                            'footway',
                            'bridleway',
                            'path',
                            'sidewalk'));
            """
