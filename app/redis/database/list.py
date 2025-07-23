import logging

from app.redis.rdb.file.constants import DBType

from .data import check_key, get_data, set_data


def get_list_length(key: str) -> int:
    logging.info("LLEN key '%s'", key)
    if not check_key(key):
        return 0

    vlist, _ = get_data(key)
    return len(vlist["value"])


def get_list_values(key: str, start: int, end: int) -> list[str]:
    logging.info("LRANGE key '%s' start %d end %d", key, start, end)
    if not check_key(key):
        return []

    vlist, _ = get_data(key)
    return vlist["value"][start : end + 1 if end != -1 else None]


def pop_list_value(key: str) -> str:
    logging.info("LPOP key '%s'", key)
    if not check_key(key):
        return ""

    vlist, _ = get_data(key)
    if not vlist["value"]:
        return ""

    res = vlist["value"].pop(0)
    set_data(key, vlist["value"])
    return res


def push_list_value(key: str, values: list[str], left: bool = False) -> int:
    logging.info("RPUSH key '%s' values %s", key, values)
    if not check_key(key):
        set_data(key, values, dtype=DBType.LIST)
        return len(values)

    vlist, _ = get_data(key)
    if left:
        vlist["value"] = values[::-1] + vlist["value"]
    else:
        vlist["value"].extend(values)

    set_data(key, vlist["value"])
    return len(vlist["value"])
