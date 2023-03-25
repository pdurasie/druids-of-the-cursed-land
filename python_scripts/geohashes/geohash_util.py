#!/opt/homebrew/bin/python3
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


# This method returns an array containing arrays of geohashes that are neighbors pairs.
def get_neighbor_pairs(geohash, orientation="horizontal"):
    if orientation not in ("horizontal", "vertical"):
        raise ValueError("Orientation must be either 'horizontal' or 'vertical'.")

    subdivision = {
        "horizontal": [
            ["b", "c", "f", "g", "u", "v", "y", "z"],
            ["8", "9", "d", "e", "s", "t", "w", "x"],
            ["2", "3", "6", "7", "k", "m", "q", "r"],
            ["0", "1", "4", "5", "h", "j", "n", "p"],
        ],
        "vertical": [
            ["p", "r", "x", "z", "n", "q", "w", "y"],
            ["j", "m", "t", "v", "h", "k", "s", "u"],
            ["5", "7", "e", "g", "4", "6", "d", "f"],
            ["1", "3", "9", "c", "0", "2", "8", "b"],
        ],
    }

    pattern = subdivision[orientation]
    pairs = []

    for row in range(len(pattern)):
        for col in range(0, len(pattern[row]), 2):
            pairs.append([geohash + pattern[row][col], geohash + pattern[row][col + 1]])

    return pairs
