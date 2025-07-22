import logging

from app.redis.rdb.file.constants import DBType
from app.redis.resp import encode_redis, encode_simple

from .data import REDIS_DB_EXP, REDIS_DB_NUM, REDIS_DB_VAL, get_current_time


def get_value(key: str) -> str:
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        return ""

    get_time = get_current_time()
    data = REDIS_DB_VAL[REDIS_DB_NUM][key]
    exp = REDIS_DB_EXP[REDIS_DB_NUM].get(key)
    logging.info("GET key '%s' data %s exp %s time %d", key, data, exp, get_time)

    if exp is not None and get_time > exp:
        logging.info("GET key '%s' expired", key)
        del REDIS_DB_VAL[REDIS_DB_NUM][key]
        del REDIS_DB_EXP[REDIS_DB_NUM][key]
        return ""

    return data.get("value", "")


def increase_value(key: str) -> bytes:
    logging.info("INCR key '%s'", key)
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        set_value(key, "1")
        return encode_redis(1)

    value = get_value(key)
    try:
        res = int(value) + 1
    except ValueError:
        return encode_simple("ERR value is not an integer or out of range", True)

    set_value(key, str(res))
    return encode_redis(res)


def set_value(key: str, value: str, options: list[str] | None = None) -> bytes:
    if options is None:
        options = []

    set_time = get_current_time()
    exp = None
    for opt in options:
        if opt in ["EX", "EXAT", "PX", "PXAT"]:
            idx = options.index(opt)
            if idx + 1 >= len(options):
                return encode_simple("ERR missing arguments for 'set' command", True)
            exp = int(options[idx + 1])
            if opt.startswith("EX"):
                exp *= 1000
            if not opt.endswith("AT"):
                exp += set_time
            break

    logging.info("SET key '%s' value %s exp %s time %d", key, value, exp, set_time)
    REDIS_DB_VAL[REDIS_DB_NUM][key] = {"value": value, "type": DBType.STR}
    if exp is not None:
        REDIS_DB_EXP[REDIS_DB_NUM][key] = exp

    return encode_simple("OK")
