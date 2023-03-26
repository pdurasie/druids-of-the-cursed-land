import csv


def get_geohashes_of_higher_precision(geohash: str) -> list[str]:
    # Define the base32 character set used by geohash
    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
    precision = len(geohash)

    # Check if the input geohash and precision are valid
    if not isinstance(geohash, str) or not all(c in BASE32 for c in geohash):
        raise ValueError("Invalid geohash: " + geohash)
    if not isinstance(precision, int) or precision < 0:
        raise ValueError("Invalid precision")

    # Get the latitude and longitude range for the current geohash
    lat_range, lon_range = (-90.0, 90.0), (-180.0, 180.0)
    for i in range(precision):
        if i % 2 == 0:
            lon_mid = sum(lon_range) / 2
            if geohash[i] in "bcfguvyz":
                lon_range = (lon_mid, lon_range[1])
            else:
                lon_range = (lon_range[0], lon_mid)
        else:
            lat_mid = sum(lat_range) / 2
            if geohash[i] in "prxz":
                lat_range = (lat_mid, lat_range[1])
            else:
                lat_range = (lat_range[0], lat_mid)

    # Generate all possible geohashes of precision x+1
    geohashes = []
    for c in BASE32:
        geohash_x1 = geohash + c
        lat_range_x1, lon_range_x1 = lat_range, lon_range
        if len(geohash_x1) % 2 == 0:
            lon_mid = sum(lon_range_x1) / 2
            if geohash_x1[-1] in "bcfguvyz":
                lon_range_x1 = (lon_mid, lon_range_x1[1])
            else:
                lon_range_x1 = (lon_range_x1[0], lon_mid)
        else:
            lat_mid = sum(lat_range_x1) / 2
            if geohash_x1[-1] in "prxz":
                lat_range_x1 = (lat_mid, lat_range_x1[1])
            else:
                lat_range_x1 = (lat_range_x1[0], lat_mid)
        if lat_range_x1[0] < lat_range_x1[1] and lon_range_x1[0] < lon_range_x1[1]:
            geohashes.append(geohash_x1)

    return geohashes


# # Define Berlin polygon coordinates
# with open('geohashes_berlin.txt') as f:
#     data = f.read()
#     berlin_geohashes = data.split('\n')

# geohashes_7 = []

# for geohash in berlin_geohashes:
#     cleaned_hash = geohash.strip()
#     geohashes_7.extend(get_geohashes_of_higher_precision(
#         cleaned_hash, len(cleaned_hash)))


# with open('./cleaned_geohashes_berlin.csv', "w") as myFile:
#     writer = csv.writer(myFile)
#     for geohash in geohashes_7:
#         writer.writerow([geohash])
