import doobie.*
import doobie.implicits.*
import cats.effect.*
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader

import scala.io.Source
import java.io.File
import scala.util.Using

object OsmDataProcessor {
  def main(args: Array[String]): Unit = {
    val tables = List(
      ("planet_osm_polygon", "berlin_polygons"),
      ("planet_osm_point", "berlin_points"),
      ("planet_osm_line", "berlin_lines")
    )

    val geoHashes: List[String] = Using(
      Source
        .fromFile("berlin_data/geohashes_berlin_7.csv")
    ) { source =>
      source.getLines().toList
    }.get

    for ((sourceTableName, targetTableName) <- tables) {
      processOsmData(sourceTableName, targetTableName, geoHashes)
    }
  }

  private def processOsmData(sourceTableName: String,
                             targetTableName: String,
                             geohashes: List[String]): Unit = {
    val xa = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://postgis_db:5432/docker",
      "docker",
      "docker"
    )

    val dropTable = sql"DROP TABLE IF EXISTS $targetTableName".update.run
    val createTable = sql"${SQLCommands
      .getTableCreationQuery(targetTableName)}".update.run

    (dropTable, createTable).mapN(_ + _).transact(xa).unsafeRunSync()

    for ((geohash, index) <- geohashes.zipWithIndex) {
      if (index % 1000 == 0) {
        println(
          s"Processing $sourceTableName...(${index.toDouble / geohashes.length * 100}%)"
        )
      }

      val rows = sql"${SQLCommands
        .getSqlQuery(sourceTableName, geohash)}"
        .to[List[()]]
        .transact(xa)
        .unsafeRunSync()

      for (row <- rows) {
        val newRow = createNewRow(row, geohash, targetTableName)
        newRow.foreach { values =>
          getInsertCommand(targetTableName)
            .updateWith(values)
            .run
            .transact(xa)
            .unsafeRunSync()
        }
      }
    }

    println(s"\nDone processing $sourceTableName.\n")
  }

  // Implement the rest of the functions (_createNewRow, _createNewLineOrPolygonRow, _createNewPointRow)
  // in Scala using similar logic as the original Python code.
}
