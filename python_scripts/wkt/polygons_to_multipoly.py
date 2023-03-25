import csv
import shapely.wkt as wkt
from shapely.geometry import MultiPolygon

polygon_list = []

with open('./berlin_data/big_areas.csv', newline='') as csvfile:
    polygons = csv.reader(csvfile, delimiter=',')
    for polygon in polygons:
        cleaned_poly = polygon[0].replace('"', '')
        polygon_list.append(cleaned_poly)

multi_poly = MultiPolygon(map(wkt.loads, polygon_list))

with open('./berlin_data/multipoly_big_berlin.txt', "w") as txt:
    txt.write(str(multi_poly))
