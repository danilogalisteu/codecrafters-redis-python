import logging

from .data import check_key
from .zset import get_zset_score, set_zset_value

MIN_LATITUDE = -85.05112878
MAX_LATITUDE = 85.05112878
MIN_LONGITUDE = -180
MAX_LONGITUDE = 180

LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE
LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE


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


def set_geo_value(key: str, values: dict[str, tuple[float]]) -> int:
    logging.info("GEOADD key '%s' values %s", key, values)
    scores = {place: encode_geo(*coords) for place, coords in values.items()}
    set_zset_value(key, scores)
    return len(scores)


def get_geo_value(key: str, places: list[str]) -> list[tuple[float]]:
    logging.info("GEOPOS key '%s' places %s", key, places)
    if not check_key(key):
        return [[] for _ in places]

    scores = {place: get_zset_score(key, place) for place in places}
    return [
        list(map(str, decode_geo(int(float(score))))) if score else []
        for place, score in scores.items()
    ]
