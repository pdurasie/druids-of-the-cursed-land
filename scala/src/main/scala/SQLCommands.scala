object SQLCommands {
  def getTableCreationQuery(sourceTableName: String): String =
    sourceTableName match {
      case "planet_osm_point"   => getPointsCreationCommand
      case "planet_osm_line"    => getLinesCreationCommand
      case "planet_osm_polygon" => getPolygonsCreationCommand
    }

  private def getPointsCreationCommand: String =
    s"""
    |CREATE TABLE berlin_points(
      |    id SERIAL PRIMARY KEY,
      |    osm_id bigint,
      |    geohash text,
      |    access text,
      |    amenity text,
      |    area text,
      |    barrier text,
      |    bicycle text,
      |    brand text,
      |    bridge text,
      |    boundary text,
      |    building text,
      |    culvert text,
      |    embankment text,
      |    foot text,
      |    harbour text,
      |    highway text,
      |    landuse text,
      |    leisure text,
      |    lock text,
      |    name text,
      |    "natural" text,
  |    place text,
  |    surface text,
  |    tourism text,
  |    water text,
  |    waterway text,
  |    wetland
    |    wood text,
    |    tags text,
    |    way geometry
    |)
    |""".stripMargin

  private def getLinesCreationCommand: String =
    """
      |CREATE TABLE berlin_lines(
      |    id SERIAL PRIMARY KEY,
      |    osm_id bigint,
      |    geohash text,
      |    access text,
      |    amenity text,
      |    area text,
      |    barrier text,
      |    bicycle text,
      |    brand text,
      |    bridge text,
      |    boundary text,
      |    building text,
      |    culvert text,
      |    embankment text,
      |    foot text,
      |    harbour text,
      |    highway text,
      |    landuse text,
      |    leisure text,
      |    lock text,
      |    name text,
      |    "natural" text,
      |    place text,
      |    surface text,
      |    tourism text,
      |    tracktype text,
      |    water text,
      |    waterway text,
      |    wetland text,
      |    wood text,
      |    tags text,
      |    length float,
      |    way geometry
      |)
      |""".stripMargin

  private def getPolygonsCreationCommand: String =
    """
      |CREATE TABLE berlin_polygons(
      |    id SERIAL PRIMARY KEY,
      |    osm_id bigint,
      |    geohash text,
      |    access text,
      |    amenity text,
      |    area text,
      |    barrier text,
      |    bicycle text,
      |    brand text,
      |    bridge text,
      |    boundary text,
      |    building text,
      |    culvert text,
      |    embankment text,
      |    foot text,
      |    harbour text,
      |    highway text,
      |    landuse text,
      |    leisure text,
      |    lock text,
      |    name text,
      |    "natural" text,
      |    place text,
      |    surface text,
      |    tourism text,
      |    tracktype text,
      |    water text,
      |    waterway text,
      |    wetland text,
      |    wood text,
      |    tags text,
      |    area_fraction float,
      |    way geometry
      |)
      |""".stripMargin

  def getSqlQuery(sourceTableName: String, geohash: String): String =
    sourceTableName match {
      case "planet_osm_point"   => getPointsSqlQuery(geohash)
      case "planet_osm_line"    => getLinesSqlQuery(geohash)
      case "planet_osm_polygon" => getPolygonsSqlQuery(geohash)
    }

  private def getPointsSqlQuery(geohash: String): String =
    s"""
       |WITH geohash_bbox AS
       |  (SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('$geohash'), 4326), 3857) AS geom)
       |SELECT DISTINCT ON ("name")
       |        osm_id,
       |       "access",
       |       amenity,
       |       area,
       |       barrier,
       |       bicycle,
       |       brand,
       |       bridge,
       |       boundary,
       |       building,
       |       culvert,
       |       embankment,
       |       foot,
       |       harbour,
       |       highway,
       |       landuse,
       |       leisure,
       |       "lock",
       |       name,
       |       "natural",
       |       place,
       |       surface,
       |       tourism,
       |       water,
       |       waterway,
       |       wetland,
       |       wood,
       |       tags,
       |       ST_AsText(ST_Transform(way, 4326)) AS "way"
       |
       |   FROM planet_osm_point, geohash_bbox
       |   WHERE ST_Intersects(way, geohash_bbox.geom) AND ("amenity" IN ('arts_centre',
       |                       'art_school',
       |                       'auditorium',
       |                       'college',
       |                       'contemporary_art_gallery',
       |                       'library',
       |                       'meditation_centre',
       |                       'music_school',
       |                       'language_school',
       |                       'school',
       |                       'theatre',
       |                       'university')
       |     OR "tourism" IN ('arts_centre',
       |                        'artwork',
       |                        'attraction',
       |                        'gallery',
       |                        'museum',
       |                        'viewpoint',
       |                        'zoo'))
       |""".stripMargin

  private def getLinesSqlQuery(geohash: String): String =
    s"""
       |WITH geohash_bbox AS
       |  (SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('$geohash'), 4326), 3857) AS geom),
       |temp_query AS (
       |SELECT osm_id,
       |       "access",
       |       amenity,
       |       area,
       |       barrier,
       |       bicycle,
       |       brand,
       |       bridge,
       |       boundary,
       |       building,
       |       culvert,
       |       embankment,
       |       foot,
       |       harbour,
       |       highway,
       |       landuse,
       |       leisure,
       |       "lock",
       |       name,
       |       "natural",
       |       place,
       |       surface,
       |       tourism,
       |       tracktype,
       |       water,
       |       waterway,
       |       wetland,
       |       wood,
       |       tags,
       |       ST_AsText(ST_Transform(way, 4326)) AS "way",
       |       ROW_NUMBER() OVER () AS row_number
       |FROM PUBLIC.planet_osm_line,
       |     geohash_bbox
       |WHERE ST_Intersects(way, geohash_bbox.geom)
       |  AND ("access" IN ('yes',
       |                    'designated')
       |       OR "access" IS NULL)
       |  AND ("highway" IN ('living_street',
       |                     'bridleway')
       |       OR "highway" = 'footway')
       |)
       |SELECT * FROM temp_query WHERE row_number % 2 <> 0
       |""".stripMargin

  private def getPolygonsSqlQuery(geohash: String): String =
    s"""
       |            WITH geohash_bbox AS (
       |                SELECT ST_Transform(ST_SetSRID(ST_GeomFromGeoHash('$geohash'), 4326), 3857) AS geom
       |            )
       |            SELECT
       |                osm_id, "access", amenity, area, barrier, bicycle, brand, bridge, boundary,
       |                building, culvert, embankment, foot, harbour, highway, landuse, leisure, "lock",
       |                name, "natural", place, surface, tourism, tracktype, water, waterway, wetland,
       |                wood, tags, ST_AsText(ST_Transform(way, 4326)) AS "way"
       |            FROM
       |                planet_osm_polygon,
       |                geohash_bbox
       |
       |            WHERE
       |                ST_Intersects(way, geohash_bbox.geom) AND (access IN ('yes',
       |                            'permissive',
       |                            'permit',
       |                            'destination',
       |                            'designated')
       |                OR access IS NULL)
       |            AND (foot IN ('yes',
       |                          'designated',
       |                          'permissive')
       |                OR leisure='park'
       |                OR landuse='forest'
       |                OR "natural"='water'
       |                OR highway IN ('residential',
       |                            'living_street',
       |                            'pedestrian',
       |                            'footway',
       |                            'bridleway',
       |                            'path',
       |                            'sidewalk'));
       |""".stripMargin

  def getInsertCommand(targetTableName: String): String = {
    if (targetTableName.contains("point"))
      getPointsInsertCommand(targetTableName)
    else getLinesOrPolygonsInsertCommand(targetTableName)
  }

  private def getPointsInsertCommand(tableName: String): String =
    s"""
       |INSERT INTO $tableName (
       |    osm_id, geohash, access, amenity, area, barrier, bicycle, brand, bridge, boundary,
       |    building, culvert, embankment, foot, harbour, highway, landuse, leisure, lock,
       |    name, "natural", place, surface, tourism, water, waterway, wetland,
       |    wood, tags, way
       |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?), 4326));
       |""".stripMargin

  private def getLinesOrPolygonsInsertCommand(tableName: String): String = {
    val columnName =
      if (tableName.contains("line")) "length" else "area_fraction"

    s"""
       |INSERT INTO $tableName (
       |    osm_id, geohash, access, amenity, area, barrier, bicycle, brand, bridge, boundary,
       |    building, culvert, embankment, foot, harbour, highway, landuse, leisure, lock,
       |    name, "natural", place, surface, tourism, tracktype, water, waterway, wetland,
       |    wood, tags, $columnName, way
       |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_SetSRID(ST_GeomFromText(?), 4326));
       |""".stripMargin
  }

}
