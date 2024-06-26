package druids

import cats.data.OptionT
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import doobie.Transactor
import doobie.implicits._
import doobie.util.Read
import doobie.util.meta.Meta
import druids.models.{DruidsGeometryRecord, GeometryRecord}
import druids.util.{GeometryUtil, SQLCommands}
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.io.Source
import scala.util.Using

object OsmDataProcessor extends IOApp {
  implicit val geometryMeta: Meta[Geometry] =
    Meta[String].timap[Geometry](str => new WKTReader().read(str))(
      geometry => geometry.toText
    )

  override def run(args: List[String]): IO[ExitCode] = {
    // TODO make this more flexible
    val tables = List(
      ("planet_osm_polygon", "berlin_polygons"),
      ("planet_osm_point", "berlin_points"),
      ("planet_osm_line", "berlin_lines")
    )

    val geoHashes: List[String] = Using(
      Source
        .fromFile("../berlin_data/geohashes_berlin_7.csv") //TODO this needs to be more flexible
    ) { source =>
      source.getLines().toList
    }.get

    val processAllTables = tables.foldLeft(IO.unit) { (acc, tablePair) =>
      val (sourceTableName, targetTableName) = tablePair
      acc.flatMap(
        _ =>
          processOsmData(sourceTableName, targetTableName, geoHashes.take(500))
      )
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

    val setup: IO[Unit] = for {
      _ <- sql"DROP TABLE IF EXISTS $targetTableName".update.run.transact(xa)
      _ <- sql"${SQLCommands.getTableCreationQuery(targetTableName)}".update.run
        .transact(xa)
    } yield ()

    def processGeohash(geohash: String): IO[Unit] = {
      for {
        rows <- getRows[GeometryRecord](sourceTableName, geohash, xa)
        _ <- rows.traverse { row =>
          (for {
            newRow <- OptionT.fromOption[IO](createNewRow(row, geohash))
            _ <- OptionT.liftF(
              SQLCommands
                .getInsertCommand(targetTableName, newRow)
                .update
                .run
                .transact(xa)
            )
          } yield ()).value
        }
      } yield ()
    }

    // Remove the extra set of curly braces here
    for {
      _ <- setup
      _ <- geohashes.traverse(geohash => processGeohash(geohash))
      _ <- IO(println(s"\nDone processing $sourceTableName.\n"))
    } yield ()
  }

  private def createNewRow(row: GeometryRecord,
                           geohash: String): Option[DruidsGeometryRecord] = {
    row.geometry match {
      case _: Point =>
        Some(DruidsGeometryRecord.fromGeometryRecord(row, geohash))
      case _: LineString =>
        Some(createNewLineRow(row, geohash))
      case _: Polygon =>
        Some(createNewPolygonRow(row, geohash))
      case _ => None
    }
  }

  private def createNewLineRow(row: GeometryRecord,
                               geohash: String): DruidsGeometryRecord = {
    val intersectingLineString: Option[LineString] = GeometryUtil
      .getIntersectingLine(geohash, row.geometry.asInstanceOf[LineString])
    DruidsGeometryRecord.fromGeometryRecord(
      row,
      geohash,
      intersectingLineString
    )
  }

  private def createNewPolygonRow(row: GeometryRecord,
                                  geohash: String): DruidsGeometryRecord = {
    val intersectingPolygon: Option[Geometry] = GeometryUtil
      .getIntersectingPolygon(geohash, row.geometry.asInstanceOf[Polygon])
    DruidsGeometryRecord.fromGeometryRecord(row, geohash, intersectingPolygon)
  }

  private def getRows[T](
    sourceTableName: String,
    geohash: String,
    xa: Transactor[IO]
  )(implicit read: Read[T]): IO[List[T]] = {
    val fragment = fr"${SQLCommands.getSqlQuery(sourceTableName, geohash)}"
    fragment.query[T].to[List].transact(xa)
  }
}
