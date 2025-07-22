import logging

from app.redis.rdb.file.constants import DBType

from .data import check_key, get_data, set_data


def push_list_value(key: str, value: str) -> int:
    logging.info("RPUSH key '%s' value %s", key, value)
    if not check_key(key):
        set_data(key, [value], dtype=DBType.LIST)
        return 1

    vlist, _ = get_data(key)
    vlist["value"].append(value)

    set_data(key, vlist["value"])
    return len(vlist["value"])
