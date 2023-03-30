import scala.util.Random
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.ListBuffer
import doobie.*
import doobie.implicits.*
import doobie.postgres.*
import doobie.postgres.implicits.*
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.locationtech.jts.io.WKTReader

object Spawner {
  def spawnHerbsInGeohash(geohash: String, xa: Transactor[IO]): Unit = {
    // Get all the neighbor pairs of inner geohashes
    val neighborPairs = GeohashUtil.getGeohashQuadrants(geohash)

    val wktReader = WKTReader()

    // Go through all the neighbor pairs
    for (quadrant <- neighborPairs) {
      // Read the records from the database which are within the quadrant geohashes
      val query = sql"""
    (
        SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
        FROM berlin_polygons
        WHERE geohash IN (${quadrant.head}, ${quadrant(1)}, ${quadrant(2)}, ${quadrant(
        3
      )})
    ) UNION ALL (
        SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
        FROM berlin_lines
        WHERE geohash IN (${quadrant.head}, ${quadrant(1)}, ${quadrant(2)}, ${quadrant(
        3
      )})
    ) UNION ALL (
        SELECT osm_id, ST_AsText(way) as geometry, leisure, landuse
        FROM berlin_points
        WHERE geohash IN (${quadrant.head}, ${quadrant(1)}, ${quadrant(2)}, ${quadrant(
        3
      )})
    );
  """.query[(Int, String, Option[String], Option[String])]

      val records: List[(Int, String, Option[String], Option[String])] =
        query.to[List].transact(xa).unsafeRunSync()
      val data = ListBuffer[Map[String, Any]]()

      for (record <- records) {
        data += Map(
          "osm_id" -> record._1,
          "geometry" -> wktReader.read(record._2),
          "leisure" -> record._3,
          "landuse" -> record._4
        )
      }

      if (data.isEmpty) return

      val parkForestRegions = data
        .filter(
          region => region("leisure") == "park" || region("landuse") == "forest"
        )
        .toList
      val otherRegions =
        data.filter(region => !parkForestRegions.contains(region)).toList

      val numHerbsToSpawn =
        if (parkForestRegions.isEmpty) 1 else Random.nextInt(2) + 2

      // Spawn the first herb in the quadrant
      if (parkForestRegions.nonEmpty && otherRegions.nonEmpty) {
        // Give a 70% higher chance for park and forest regions
        val parkForestWeight = 1.7 * otherRegions.length / parkForestRegions.length
        val randomRegionData = Random
          .shuffle(
            parkForestRegions.map((_, parkForestWeight)) ::: otherRegions
              .map((_, 1.0))
          )
          .head
          ._1

        val randomGeometry = randomRegionData("geometry").asInstanceOf[Geometry]
        val randomPoint =
          PointGeneration.generateRandomPointInGeometry(randomGeometry)

        val lat = randomPoint.getY
        val lon = randomPoint.getX

        insertHerbIntoDatabase(lat, lon, xa)
      } else if (parkForestRegions.nonEmpty) {
        val randomRegionData = Random.shuffle(parkForestRegions).head

        val randomGeometry = randomRegionData("geometry").asInstanceOf[Geometry]
        val randomPoint =
          PointGeneration.generateRandomPointInGeometry(randomGeometry)

        val lat = randomPoint.getY
        val lon = randomPoint.getX

        insertHerbIntoDatabase(lat, lon, xa)
      } else {
        val randomRegionData = Random.shuffle(otherRegions).head

        val randomGeometry = randomRegionData("geometry").asInstanceOf[Geometry]
        val randomPoint =
          PointGeneration.generateRandomPointInGeometry(randomGeometry)

        val lat = randomPoint.getY
        val lon = randomPoint.getX

        insertHerbIntoDatabase(lat, lon, xa)
      }

      // Spawn the second herb in a park or forest if available
      if (numHerbsToSpawn == 2) {
        val randomRegionData = Random.shuffle(parkForestRegions).head
        val randomGeometry = randomRegionData("geometry").asInstanceOf[Geometry]
        val randomPoint =
          PointGeneration.generateRandomPointInGeometry(randomGeometry)

        val lat = randomPoint.getY
        val lon = randomPoint.getX

        insertHerbIntoDatabase(lat, lon, xa)
      }
    }
  }

  private def insertHerbIntoDatabase(latitude: Double,
                                     longitude: Double,
                                     xa: Transactor[IO]): Unit = {
    val insertQuery =
      sql"""
        INSERT INTO herbs (location)
        VALUES (ST_SetSRID(ST_MakePoint($longitude, $latitude), 4326))
      """.update

    insertQuery.run.transact(xa).unsafeRunSync()
  }
}
