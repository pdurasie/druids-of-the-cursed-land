package druids.models

import doobie._
import doobie.implicits._
import doobie.postgres._
import doobie.postgres.implicits._
import org.locationtech.jts.io.{WKBReader, WKBWriter}
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

case class GeometryRecordImpl(osmId: String,
                              access: String,
                              amenity: String,
                              area: String,
                              barrier: String,
                              bicycle: String,
                              brand: String,
                              bridge: String,
                              boundary: String,
                              buildings: String,
                              culvert: String,
                              embankment: String,
                              foot: String,
                              harbour: String,
                              highway: String,
                              landuse: String,
                              leisure: String,
                              lock: String,
                              name: String,
                              natural: String,
                              place: String,
                              surface: String,
                              tourism: String,
                              tracktype: Option[String],
                              watermark: String,
                              waterway: String,
                              wetland: String,
                              wood: String,
                              tags: String,
                              geometry: Geometry)
    extends GeometryRecord

object GeometryRecord {
  implicit val geometryRead: Read[Geometry] = Read[Array[Byte]].map { wkb =>
    val reader = new WKBReader()
    reader.read(wkb)
  }

  implicit val geometryRecordRead: Read[GeometryRecord] = {
    Read[
      (String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       String,
       Option[String],
       String,
       String,
       String,
       String,
       String,
       Geometry)
    ].map {
      case (
          osmId: String,
          access: String,
          amenity: String,
          area: String,
          barrier: String,
          bicycle: String,
          brand: String,
          bridge: String,
          boundary: String,
          buildings: String,
          culvert: String,
          embankment: String,
          foot: String,
          harbour: String,
          highway: String,
          landuse: String,
          leisure: String,
          lock: String,
          name: String,
          natural: String,
          place: String,
          surface: String,
          tourism: String,
          tracktype: Option[String],
          watermark: String,
          waterway: String,
          wetland: String,
          wood: String,
          tags: String,
          geometry: Geometry
          ) =>
        GeometryRecordImpl(
          osmId,
          access,
          amenity,
          area,
          barrier,
          bicycle,
          brand,
          bridge,
          boundary,
          buildings,
          culvert,
          embankment,
          foot,
          harbour,
          highway,
          landuse,
          leisure,
          lock,
          name,
          natural,
          place,
          surface,
          tourism,
          tracktype,
          watermark,
          waterway,
          wetland,
          wood,
          tags,
          geometry
        )
    }
  }
}
