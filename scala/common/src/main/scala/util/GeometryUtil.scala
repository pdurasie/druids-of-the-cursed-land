package util

import GeohashUtil.getPolygonFromGeohash

import scala.util.Random
import org.locationtech.jts.geom.*
import org.locationtech.jts.operation.union.UnaryUnionOp

object GeometryUtil {
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

  def getIntersectingLine(geohash: String, line: LineString): Option[LineString] = {
    // Convert geohash to bounding box polygon
    GeohashUtil.getPolygonFromGeohash(geohash).flatMap { polygon =>
      // Get intersection between the line and the bounding box polygon
      val intersection = line.intersection(polygon)

      if (intersection.equalsExact(line, 0)) {
        None
      } else if (intersection.getGeometryType == "LineString") {
        Some(intersection.asInstanceOf[LineString])
      } else {
        // If the intersection is a MultiLineString, convert it to a LineString by merging all lines together
        Some(
          if (intersection.getGeometryType == "MultiLineString") {
            UnaryUnionOp.union(intersection.asInstanceOf[MultiLineString]).asInstanceOf[LineString]
          } else {
            intersection.asInstanceOf[LineString]
          }
        )
      }
    }
  }

  def getIntersectingPolygon(geohash: String, polygon: Polygon): Option[Geometry] = {
    // Check for intersection and get intersecting part as a new polygon
    getPolygonFromGeohash(geohash).flatMap(bboxPolygon => {
      if (polygon.intersects(bboxPolygon)) {
        val intersection = polygon.intersection(bboxPolygon)
        if (!intersection.equalsExact(polygon, 0)) {
          intersection match {
            case polygonIntersection: Polygon =>
              Some(polygonIntersection)
            case multiPolygonIntersection: MultiPolygon =>
              Some(multiPolygonIntersection)
            case _ => None
          }
        } else None
      } else None
    })
  }
}
