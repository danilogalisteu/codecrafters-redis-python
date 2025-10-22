import logging

from app.redis.rdb.file.constants import DBType
from lib import curio

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


async def pop_block_list_value(key: str, block_time: float) -> list[str]:
    logging.info("BLPOP key '%s' block_time %s", key, block_time)

    vlist, _ = get_data(key)
    while not vlist or len(vlist["value"]) == 0:
        await curio.sleep(block_time)
        vlist, _ = get_data(key)
        if block_time > 0:
            break

    if not vlist or len(vlist["value"]) == 0:
        return []

    set_data(key, vlist["value"][1:])
    return [key, vlist["value"][0]]


def pop_list_value(key: str, count: int = 1) -> str:
    logging.info("LPOP key '%s' count %d", key, count)
    if not check_key(key):
        return ""

    vlist, _ = get_data(key)
    if not vlist["value"]:
        return ""

    res = vlist["value"][:count]
    set_data(key, vlist["value"][count:])
    return res if len(res) > 1 else res[0]


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
