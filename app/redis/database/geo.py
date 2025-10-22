import logging

from .zset import set_zset_value

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


def encode_geo(longitude: float, latitude: float) -> int:
    normalized_longitude = int(2**26 * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE)
    normalized_latitude = int(2**26 * (latitude - MIN_LATITUDE) / LATITUDE_RANGE)
    inter_longitude = spread_int32_to_int64(normalized_longitude)
    inter_latitude = spread_int32_to_int64(normalized_latitude)
    return (inter_longitude << 1) | inter_latitude


def set_geo_value(key: str, values: dict[str, tuple[float]]) -> int:
    logging.info("GEOADD key '%s' values %s", key, values)
    scores = {place: encode_geo(*coords) for place, coords in values.items()}
    set_zset_value(key, scores)
    return len(scores)
