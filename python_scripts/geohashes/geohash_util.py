import time
import geohash2


def get_all_geohashes_in_bbox(bbox):
    precision = 7

    # Generate all geohashes within the bounding box with precision
    start_time = time.time()
    geohashes = set()
    for lat in range(
        int(bbox[0] * 10**precision), int(bbox[2] * 10**precision), 89
    ):
        for lon in range(
            int(bbox[1] * 10**precision), int(bbox[3] * 10**precision), 89
        ):
            geohash = geohash2.encode(
                lat / 10**precision, lon / 10**precision, precision=precision
            )
            geohashes.add(geohash)

    with open("./geohashes_berlin.txt", "w") as txt:
        txt.write(str(geohashes))

    print(
        "Determined all geohashes with precision "
        + precision
        + " in time: "
        + time.time()
        - start_time
    )


def get_geohash_quadrants(geohash):
    subdivision = [
        ["b", "c", "f", "g", "u", "v", "y", "z"],
        ["8", "9", "d", "e", "s", "t", "w", "x"],
        ["2", "3", "6", "7", "k", "m", "q", "r"],
        ["0", "1", "4", "5", "h", "j", "n", "p"],
    ]

    quadrants = []

    for row in range(0, len(subdivision), 2):
        for col in range(0, len(subdivision[row]), 2):
            quadrant = [
                geohash + subdivision[row][col],
                geohash + subdivision[row][col + 1],
                geohash + subdivision[row + 1][col],
                geohash + subdivision[row + 1][col + 1],
            ]
            quadrants.append(quadrant)

    return quadrants
