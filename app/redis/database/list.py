import logging

from .data import REDIS_DB_NUM, REDIS_DB_VAL
from .value import get_value, set_value


def set_list_value(key: str, value: str) -> int:
    logging.info("RPUSH key '%s' value %s", key, value)
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        set_value(key, [value])
        return 1

    vlist = get_value(key)
    vlist.append(value)

    set_value(key, vlist)
    return len(vlist)
