import logging

from app.redis.rdb.file.constants import DBType

from .data import check_key, get_data, set_data


def set_set_value(key: str, values: dict[str, float]) -> bytes:
    logging.info("ZADD key '%s' values %s", key, values)
    if not check_key(key):
        sorted_values = list(sorted(sorted(values.items(), key=lambda x: x[0]), key=lambda x: x[1]))
        set_data(key, sorted_values, dtype=DBType.ZSET)
        return len(sorted_values)

    vzset, _ = get_data(key)
    vdict = dict(vzset["value"])
    vdict.update(values)
    sorted_values = list(sorted(sorted(vdict.items(), key=lambda x: x[0]), key=lambda x: x[1]))
    set_data(key, sorted_values, dtype=DBType.ZSET)
    return len(sorted_values) - len(vzset["value"])
