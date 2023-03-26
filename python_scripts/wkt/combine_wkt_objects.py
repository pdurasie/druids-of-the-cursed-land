import csv
import json
import shapely.wkt as wkt
import shapely.geometry as geometry
from shapely.geometry import MultiPoint, MultiLineString, MultiPolygon


def merge_wkt_objects(wkt_list) -> str:
    geoms = []

    print("Received list: " + str(wkt_list))
    for geometry_object in wkt_list:
        geoms.append(wkt.loads(geometry_object))

    points = [geom for geom in geoms if isinstance(geom, geometry.Point)]
    lines = [geom for geom in geoms if isinstance(geom, geometry.LineString)]
    polygons = [geom for geom in geoms if isinstance(geom, geometry.Polygon)]

    # merge points into a single MultiPoint object
    merged_points = MultiPoint(points)

    # merge lines into a single MultiLineString object
    merged_lines = MultiLineString(lines)

    # merge polygons into a single MultiPolygon object
    merged_polygons = MultiPolygon(polygons)

    # convert the merged geometries to WKT or GeoJSON
    merged_wkt = (
        merged_points.wkt + "\n" + merged_lines.wkt + "\n" + merged_polygons.wkt
    )

    merged_geojson = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "geometry": geom.__geo_interface__, "properties": {}}
            for geom in [merged_points, merged_lines, merged_polygons]
        ],
    }

    return merged_geojson


# load geometries from csv
# wkt_list = []
# with open("../berlin_data/combined_berlin.csv", newline="") as csvfile:
#     objects = csv.reader(csvfile, delimiter=",")
#     for object in objects:
#         cleaned_object = object[0].replace('"', "")
#         wkt_list.append(cleaned_object)

# merged_geojson = merge_wkt_objects(wkt_list)

# with open("../berlin_data/combined_berlin.txt", "w") as txt:
#     txt.write(json.dumps(merged_geojson))
