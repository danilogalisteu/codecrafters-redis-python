import logging
from math import asin, cos, radians, sin, sqrt

from .data import check_key
from .zset import get_zset_range, get_zset_score, set_zset_value

EARTH_RADIUS = 6372797.560856

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

DISTANCE_UNITS = {
    "m": 1.0,
    "km": 1000.0,
    "mi": 1609.344,
    "ft": 0.3048,
}


def spread_int32_to_int64(v: int) -> int:
    v = v & 0xFFFFFFFF
    v = (v | (v << 16)) & 0x0000FFFF0000FFFF
    v = (v | (v << 8)) & 0x00FF00FF00FF00FF
    v = (v | (v << 4)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v << 2)) & 0x3333333333333333
    v = (v | (v << 1)) & 0x5555555555555555
    return v  # noqa: RET504


def compact_int64_to_int32(v: int) -> int:
    v = v & 0x5555555555555555
    v = (v | (v >> 1)) & 0x3333333333333333
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0F
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FF
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFF
    v = (v | (v >> 16)) & 0x00000000FFFFFFFF
    return v  # noqa: RET504


def encode_geo(longitude: float, latitude: float) -> int:
    normalized_longitude = int(2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)
    normalized_latitude = int(2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE)
    inter_longitude = spread_int32_to_int64(normalized_longitude)
    inter_latitude = spread_int32_to_int64(normalized_latitude)
    return (inter_longitude << 1) | inter_latitude


def decode_geo(score: int) -> tuple[float]:
    normalized_longitude = compact_int64_to_int32(score >> 1) + 0.5
    normalized_latitude = compact_int64_to_int32(score) + 0.5
    longitude = (normalized_longitude / 2**26) * LONGITUDE_RANGE + MIN_LONGITUDE
    latitude = (normalized_latitude / 2**26) * LATITUDE_RANGE + MIN_LATITUDE
    return longitude, latitude


def haversine(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    sq_sin_half_lon_diff = sin(radians(lon2 - lon1) / 2) ** 2
    sq_sin_half_lat_diff = sin(radians(lat2 - lat1) / 2) ** 2
    a = (
        sq_sin_half_lat_diff
        + cos(radians(lat1)) * cos(radians(lat2)) * sq_sin_half_lon_diff
    )
    return 2 * EARTH_RADIUS * asin(sqrt(a))


def set_geo_value(key: str, values: dict[str, tuple[float]]) -> int:
    logging.info("GEOADD key '%s' values %s", key, values)
    scores = {place: encode_geo(*coords) for place, coords in values.items()}
    set_zset_value(key, scores)
    return len(scores)


def get_geo_value(key: str, places: list[str]) -> list[tuple[str]]:
    logging.info("GEOPOS key '%s' places %s", key, places)
    if not check_key(key):
        return [[] for _ in places]

    scores = {place: get_zset_score(key, place) for place in places}
    return [
        list(map(str, decode_geo(int(float(score))))) if score else []
        for place, score in scores.items()
    ]


def get_geo_distance(key: str, place1: str, place2: str) -> str:
    logging.info("GEODIST key '%s' place1 %s place2 %s", key, place1, place2)
    if not check_key(key):
        return ""

    score1 = get_zset_score(key, place1)
    score2 = get_zset_score(key, place2)
    if not score1 or not score2:
        return ""

    return str(
        haversine(
            *decode_geo(int(float(score1))),
            *decode_geo(int(float(score2))),
        )
    )


def get_geo_closest(
    key: str,
    longitude: float,
    latitude: float,
    radius: float,
    unit: str,
) -> list[str]:
    logging.info(
        "GEOSEARCH key '%s' longitude %s latitude %s radius %s unit '%s'",
        key,
        longitude,
        latitude,
        radius,
        unit,
    )
    if not check_key(key):
        return []

    places = get_zset_range(key, 0, -1)
    scores = {place: int(float(get_zset_score(key, place))) for place in places}
    distances = {
        place: haversine(
            longitude,
            latitude,
            *decode_geo(scores[place]),
        )
        for place in places
    }

    distance_limit = radius * DISTANCE_UNITS[unit.lower()]
    return [
        place for place, distance in distances.items() if distance <= distance_limit
    ]
