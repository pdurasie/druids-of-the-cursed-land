import time
import geohash2

# Define the bounding box for Berlin OUTDATED! This is not precise enough
berlin_bbox = (52.338234, 13.088451, 52.675454, 13.761160)

precision = 7

# Generate all geohashes within the bounding box with precision
start_time = time.time()
geohashes = set()
for lat in range(int(berlin_bbox[0]*10**precision), int(berlin_bbox[2]*10**precision), 89):
    for lon in range(int(berlin_bbox[1]*10**precision), int(berlin_bbox[3]*10**precision), 89):
        geohash = geohash2.encode(
            lat/10**precision, lon/10**precision, precision=precision)
        geohashes.add(geohash)

with open('./geohashes_berlin.txt', "w") as txt:
    txt.write(str(geohashes))

print("Determined all geohashes with precision " +
      precision + " in time: " + time.time() - start_time)
