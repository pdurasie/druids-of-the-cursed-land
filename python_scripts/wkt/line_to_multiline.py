import csv
import shapely.wkt as wkt
from shapely.geometry import MultiLineString

road_list = []

with open('./berlin_data/roads_berlin.csv', newline='') as csvfile:
    roads = csv.reader(csvfile, delimiter=',')
    for road in roads:
        cleaned_road = road[0].replace('"', '')
        road_list.append(cleaned_road)


multi_line_string = MultiLineString(list(map(wkt.loads, road_list)))

with open('./berlin_data/multi_line_string_berlin.txt', "w") as txt:
    txt.write(str(multi_line_string))
