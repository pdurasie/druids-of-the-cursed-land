import doobie.*
import doobie.implicits.*
import doobie.util.Read
import cats.effect.*
import cats.effect.unsafe.implicits.global
import cats.effect.implicits.concurrentParTraverseOps
import cats.Parallel
import cats.implicits.catsSyntaxTuple2Semigroupal
import druids.util.{SQLCommands, GeometryUtil}
import org.locationtech.jts.geom.*
import org.locationtech.jts.io.WKTReader

import scala.io.Source
import java.io.File
import scala.util.Using
import doobie.util.Meta
import druids.models.{DruidsGeometryRecord, GeometryRecord}
import org.locationtech.jts.geom.{Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader

object OsmDataProcessor extends IOApp {
  implicit val geometryMeta: Meta[Geometry] =
    Meta[String].timap[Geometry](str => WKTReader().read(str))(
      geometry => geometry.toText
    )

  def main(args: Array[String]): IO[ExitCode] = {
    // TODO make this more flexible
    val tables = List(
      ("planet_osm_polygon", "berlin_polygons"),
      ("planet_osm_point", "berlin_points"),
      ("planet_osm_line", "berlin_lines")
    )

    val geoHashes: List[String] = Using(
      Source
        .fromFile("berlin_data/geohashes_berlin_7.csv") //TODO this needs to be more flexible
    ) { source =>
      source.getLines().toList
    }.get

    val processAllTables = tables.foldLeft(IO.unit) { (acc, tablePair) =>
      val (sourceTableName, targetTableName) = tablePair
      acc.flatMap(_ => processOsmData(sourceTableName, targetTableName, geoHashes))
    }

    processAllTables.as(ExitCode.Success)
  }

  private def processOsmData(sourceTableName: String,
                             targetTableName: String,
                             geohashes: List[String]): IO[Unit] = {
    val xa = Transactor.fromDriverManager[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://postgis_db:5432/docker",
      "docker",
      "docker"
    )

    val setup = for {
      _ <- sql"DROP TABLE IF EXISTS $targetTableName".update.run.transact(xa)
      _ <- sql"${SQLCommands.getTableCreationQuery(targetTableName)}".update.run.transact(xa)
    } yield ()

    def processGeohash(geohash: String, index: Int): IO[Unit] = {
      val logProgress = if (index % 1000 == 0) {
        IO(println(s"Processing $sourceTableName...(${index.toDouble / geohashes.length * 100}%)"))
      } else {
        IO.unit
      }

      for {
        _ <- logProgress
        rows <- getRows[GeometryRecord](sourceTableName, geohash, xa)
        _ <- rows.map { row =>
          createNewRow(row, geohash).map { record =>
            SQLCommands.getInsertCommand(targetTableName, record).update.run.transact(xa)
          }
        }
      } yield ()
    }

    for {
      _ <- setup
      _ <- geohashes.zipWithIndex.parTraverseN(2) { case (geohash, index) => processGeohash(geohash, index) }
      _ <- IO(println(s"\nDone processing $sourceTableName.\n"))
    } yield ()
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
