package models

import org.locationtech.jts.geom.Geometry

case class DruidsGeometryRecord(osmId: String,
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
                                geometry: Geometry,
                                geohash: String)
    extends GeometryRecord

object DruidsGeometryRecord {
  def fromGeometryRecord(
    record: GeometryRecord,
    geohash: String,
    geometry: Option[Geometry] = null
  ): DruidsGeometryRecord = {
    DruidsGeometryRecord(
      osmId = record.osmId,
      access = record.access,
      amenity = record.amenity,
      area = record.area,
      barrier = record.barrier,
      bicycle = record.bicycle,
      brand = record.brand,
      bridge = record.bridge,
      boundary = record.boundary,
      buildings = record.buildings,
      culvert = record.culvert,
      embankment = record.embankment,
      foot = record.foot,
      harbour = record.harbour,
      highway = record.highway,
      landuse = record.landuse,
      leisure = record.leisure,
      lock = record.lock,
      name = record.name,
      natural = record.natural,
      place = record.place,
      surface = record.surface,
      tourism = record.tourism,
      tracktype = record.tracktype,
      watermark = record.watermark,
      waterway = record.waterway,
      wetland = record.wetland,
      wood = record.wood,
      tags = record.tags,
      geometry = geometry.getOrElse(record.geometry),
      geohash = geohash
    )
  }

}
