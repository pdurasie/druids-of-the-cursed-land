import csv
import shapely.wkt as wkt
from shapely.geometry import MultiPoint

poi_list = []

with open('./berlin_data/pois_berlin.csv', newline='') as csvfile:
    pois = csv.reader(csvfile, delimiter=',')
    for poi in pois:
        cleaned_poi = poi[0].replace('"', '')
        poi_list.append(cleaned_poi)


multi_point_string = MultiPoint(list(map(wkt.loads, poi_list)))

with open('./berlin_data/pois_berlin.txt', "w") as txt:
    txt.write(str(multi_point_string))
