import doobie.*
import doobie.implicits.*
import doobie.util.Read
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.implicits.catsSyntaxTuple2Semigroupal
import models.{DruidsGeometryRecord, GeometryRecord}
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader

import scala.io.Source
import java.io.File
import scala.util.Using
import doobie.util.Meta
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

object OsmDataProcessor {
  implicit val geometryMeta: Meta[Geometry] =
    Meta[String].timap[Geometry](str => WKTReader().read(str))(
      geometry => geometry.toText
    )
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

      val rows: List[GeometryRecord] =
        getRows[GeometryRecord](sourceTableName, geohash, xa)

      for (row <- rows) {
        val newRow = createNewRow(row, geohash)
        newRow.foreach { record =>
          SQLCommands.getInsertCommand(targetTableName, record).update
            .run
            .transact(xa)
            .unsafeRunSync()
        }
      }
    }

    println(s"\nDone processing $sourceTableName.\n")
  }

  private def createNewRow(row: GeometryRecord,
                           geohash: String): Option[DruidsGeometryRecord] = {
    row.geometry match
      case _: Point =>
        Some(DruidsGeometryRecord.fromGeometryRecord(row, geohash))
      case _: LineString =>
        Some(createNewLineRow(row, geohash))
      case _: Polygon =>
        Some(createNewPolygonRow(row, geohash))
      case _ => None
  }

  private def createNewLineRow(row: GeometryRecord,
                                geohash: String): DruidsGeometryRecord = {
    val intersectingLineString: Option[LineString] = GeometryUtil.getIntersectingLine(geohash, row.geometry.asInstanceOf[LineString])
    DruidsGeometryRecord.fromGeometryRecord(row, geohash, intersectingLineString)
  }

  private def createNewPolygonRow(row: GeometryRecord,
                               geohash: String): DruidsGeometryRecord = {
    val intersectingPolygon: Option[Geometry] = GeometryUtil.getIntersectingPolygon(geohash, row.geometry.asInstanceOf[Polygon])
    DruidsGeometryRecord.fromGeometryRecord(row, geohash, intersectingPolygon)
  }

  private def getRows[T: Read](sourceTableName: String,
                               geohash: String,
                               xa: Transactor[IO]): List[T] = {
    val fragment = fr"${SQLCommands.getSqlQuery(sourceTableName, geohash)}"
    fragment.query[T].to[List].transact(xa).unsafeRunSync()
  }
}