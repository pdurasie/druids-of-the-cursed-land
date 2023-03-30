import scala.util.Random
import org.locationtech.jts.geom._

object PointGeneration {
  def generateRandomPointInGeometry(geometry: Geometry): Point = {
    geometry match
      case point: Point => point
      case _ => if (geometry.isInstanceOf[Polygon] || geometry
        .isInstanceOf[MultiPolygon]) {
        generateRandomPointInPolygon(geometry)
      } else { // LineString
        generateRandomPointOnLineString(geometry.asInstanceOf[LineString])
      }
  }

  def generateRandomPointOnLineString(linestring: Geometry): Point = {
    val line = linestring match {
      case multiLine: MultiLineString =>
        val geometries = multiLine.getNumGeometries
        val randomIndex = Random.nextInt(geometries)
        multiLine.getGeometryN(randomIndex).asInstanceOf[LineString]
      case ls: LineString => ls
    }

    val randomPoint = line.getPointN(Random.nextInt(line.getNumPoints))
    randomPoint
  }

  def generateRandomPointInPolygon(inputPolygon: Geometry): Point = {
    val polygon = inputPolygon match {
      case multiPolygon: MultiPolygon =>
        multiPolygon
          .getGeometryN(Random.nextInt(multiPolygon.getNumGeometries))
          .asInstanceOf[Polygon]
      case singlePolygon: Polygon => singlePolygon
    }

    val envelope = polygon.getEnvelopeInternal
    val (minX, minY, maxX, maxY) =
      (envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)

    LazyList
      .continually {
        val x = minX + Random.nextDouble * (maxX - minX)
        val y = minY + Random.nextDouble * (maxY - minY)
        val coordinateSequence = polygon.getFactory.getCoordinateSequenceFactory
          .create(Array(Coordinate(x, y)))
        Point(coordinateSequence, polygon.getFactory)
      }
      .find(_.within(polygon))
      .get
  }
}
