from geohash import bbox
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
import shapely.wkt as wkt


def get_intersecting_polygon(
    geohash: str, polygon: Polygon
) -> Polygon | MultiPolygon | None:
    # Convert geohash to bounding box polygon
    # Example value of bbox(geohash): {'s': 52.4432373046875, 'w': 13.150634765625, 'n': 52.44873046875, 'e': 13.16162109375}
    # This means that the southern boundary of the bounding box is at latitude 52.4432373046875, the western boundary is at longitude 13.150634765625, the northern boundary is at latitude 52.44873046875, and the eastern boundary is at longitude 13.16162109375.
    # The lines that the lat/long boundaries draw intersect, forming the geohash bounding box.
    min_lat, min_lon, max_lat, max_lon = bbox(geohash).values()
    bbox_polygon = Polygon(
        [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
    )

    # Check for intersection and get intersecting part as a new polygon
    if polygon.intersects(bbox_polygon):
        intersection = polygon.intersection(bbox_polygon)
        if not (intersection.equals_exact(polygon, 0)):
            return intersection


def get_intersecting_line(geohash: str, line: LineString) -> LineString | None:
    # Convert geohash to bounding box polygon
    min_lat, min_lon, max_lat, max_lon = bbox(geohash)
    bbox_polygon = Polygon(
        [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
    )

    # Get intersection between the line and the bounding box polygon
    intersection = line.intersection(bbox_polygon)
    if intersection.is_empty:
        return None
    elif intersection.geom_type == "LineString":
        return intersection
    else:
        # If the intersection is a MultiLineString, convert it to a LineString by merging all lines together
        return (
            intersection[0].merged
            if intersection.geom_type == "MultiLineString"
            else intersection
        )
