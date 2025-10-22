import logging

from .zset import set_zset_value


def set_geo_value(key: str, values: dict[str, tuple[float]]) -> int:
    logging.info("GEOADD key '%s' values %s", key, values)
    scores = {k: 0 for k in values}
    set_zset_value(key, scores)
    return len(scores)
