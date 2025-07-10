import logging
from time import time_ns


REDIS_DB = {}


def get_current_time():
    """Current time in ms"""
    return int(time_ns() / 1e6)


def set_value(key: str, value: str, options: list[str]) -> str:
    set_time = get_current_time()
    exp = None
    for opt in options:
        if opt in ["EX", "EXAT", "PX", "PXAT"]:
            idx = options.index(opt)
            if idx + 1 >= len(options):
                return "-ERR missing arguments for 'set' command"
            exp = int(options[idx+1])
            if opt.startswith("EX"):
                exp *= 1000
            if not opt.endswith("AT"):
                exp += set_time
            break

    logging.info("SET time %d key %s value %s exp %s", set_time, key, value, exp)
    REDIS_DB[key] = {"value": value, "exp": exp}
    return "OK"


def get_value(key) -> str:
    get_time = get_current_time()
    data = REDIS_DB.get(key, {})
    logging.info("GET time %d key %s data %s", get_time, key, data)

    value = data.get("value", "")
    exp = data.get("exp", None)
    if exp is not None and get_time > exp:
        logging.debug("GET expired key %s", key)
        del REDIS_DB[key]
        return ""
    return value
