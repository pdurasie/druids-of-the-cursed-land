package models

import org.locationtech.jts.geom.Geometry

trait GeometryRecord {
  val osmId: String
  val access: String
  val amenity: String
  val area: String
  val barrier: String
  val bicycle: String
  val brand: String
  val bridge: String
  val boundary: String
  val buildings: String
  val culvert: String
  val embankment: String
  val foot: String
  val harbour: String
  val highway: String
  val landuse: String
  val leisure: String
  val lock: String
  val name: String
  val natural: String
  val place: String
  val surface: String
  val tourism: String
  val tracktype: Option[String]
  val watermark: String
  val waterway: String
  val wetland: String
  val wood: String
  val tags: String
  val geometry: Geometry
}
