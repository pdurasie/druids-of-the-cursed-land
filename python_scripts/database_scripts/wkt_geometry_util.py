from geohash import bbox
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
from shapely.ops import unary_union
import shapely.wkt as wkt


# Create a function to get a polygon from a geohash
def get_polygon_from_geohash(geohash: str) -> Polygon:
    # Convert geohash to bounding box polygon
    # Example value of bbox(geohash): {'s': 52.4432373046875, 'w': 13.150634765625, 'n': 52.44873046875, 'e': 13.16162109375}
    # This means that the southern boundary of the bounding box is at latitude 52.4432373046875, the western boundary is at longitude 13.150634765625, the northern boundary is at latitude 52.44873046875, and the eastern boundary is at longitude 13.16162109375.
    # The lines that the lat/long boundaries draw intersect, forming the geohash bounding box.
    min_lat, min_lon, max_lat, max_lon = bbox(geohash).values()
    bbox_polygon = Polygon(
        [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
    )
    return bbox_polygon


def get_intersecting_polygon(
    geohash: str, polygon: Polygon
) -> Polygon | MultiPolygon | None:
    # Check for intersection and get intersecting part as a new polygon
    bbox_polygon = get_polygon_from_geohash(geohash)
    if polygon.intersects(bbox_polygon):
        intersection = polygon.intersection(bbox_polygon)
        if not (intersection.equals_exact(polygon, 0)):
            return intersection


def get_intersecting_line(geohash: str, line: LineString) -> LineString | None:
    # Convert geohash to bounding box polygon
    bbox_polygon = get_polygon_from_geohash(geohash)
    # Get intersection between the line and the bounding box polygon
    intersection = line.intersection(bbox_polygon)

    if intersection.equals_exact(line, 0):
        return None

    if intersection.geom_type == "LineString":
        return intersection
    else:
        # If the intersection is a MultiLineString, convert it to a LineString by merging all lines together
        return (
            unary_union(intersection)
            if intersection.geom_type == "MultiLineString"
            else intersection
        )
