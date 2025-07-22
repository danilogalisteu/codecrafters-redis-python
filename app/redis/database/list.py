import logging

from app.redis.rdb.file.constants import DBType

from .data import check_key, get_data, set_data


def push_list_value(key: str, values: list[str]) -> int:
    logging.info("RPUSH key '%s' values %s", key, values)
    if not check_key(key):
        set_data(key, values, dtype=DBType.LIST)
        return len(values)

    vlist, _ = get_data(key)
    vlist["value"].extend(values)

    set_data(key, vlist["value"])
    return len(vlist["value"])
