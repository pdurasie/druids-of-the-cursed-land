from geohashes.geohash import bbox
from shapely.geometry import Polygon
from shapely.geometry import LineString


def get_intersecting_polygon(geohash: str, polygon: Polygon) -> Polygon | None:
    # Convert geohash to bounding box polygon
    min_lat, min_lon, max_lat, max_lon = bbox(geohash)
    bbox_polygon = Polygon(
        [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)])

    # Check for intersection and get intersecting part as a new polygon
    if polygon.intersects(bbox_polygon):
        return polygon.intersection(bbox_polygon)


def get_intersecting_line(geohash: str, line: LineString) -> LineString | None:
    # Convert geohash to bounding box polygon
    min_lat, min_lon, max_lat, max_lon = bbox(geohash)
    bbox_polygon = Polygon(
        [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)])

    # Get intersection between the line and the bounding box polygon
    intersection = line.intersection(bbox_polygon)
    if intersection.is_empty:
        return None
    elif intersection.geom_type == 'LineString':
        return intersection
    else:
        # If the intersection is a MultiLineString, convert it to a LineString by merging all lines together
        return intersection[0].merged if intersection.geom_type == 'MultiLineString' else intersection
