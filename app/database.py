from time import time_ns
import logging

from .rdb import read_rdb, save_rdb

REDIS_DB_NUM = 0
REDIS_DB_DATA = {REDIS_DB_NUM: {}}
REDIS_META = {}


def init_db(dirname: str, dbfilename: str):
    global REDIS_META, REDIS_DB_DATA
    REDIS_META, REDIS_DB_DATA = read_rdb(dirname, dbfilename)


def get_current_time() -> int:
    """Current time in ms"""
    return int(time_ns() / 1e6)


def get_keys(pattern: str) -> list[str]:
    global REDIS_DB_DATA, REDIS_DB_NUM
    keys = list(REDIS_DB_DATA[REDIS_DB_NUM].keys())
    # TODO filter keys using pattern
    return keys


def get_value(key: str) -> str:
    global REDIS_DB_DATA, REDIS_DB_NUM
    get_time = get_current_time()
    data = REDIS_DB_DATA[REDIS_DB_NUM].get(key, {})
    logging.info("GET time %d key %s data %s", get_time, key, data)

    value = data.get("value", "")
    exp = data.get("exp", None)
    if exp is not None and get_time > exp:
        logging.debug("GET expired key %s", key)
        del REDIS_DB_DATA[REDIS_DB_NUM][key]
        return ""
    return value


def save_db(dirname: str, dbfilename: str) -> None:
    global REDIS_META, REDIS_DB_DATA
    save_rdb(dirname, dbfilename, REDIS_META, REDIS_DB_DATA)


def set_value(key: str, value: str, options: list[str]) -> str:
    global REDIS_DB_DATA, REDIS_DB_NUM
    set_time = get_current_time()
    exp = None
    for opt in options:
        if opt in ["EX", "EXAT", "PX", "PXAT"]:
            idx = options.index(opt)
            if idx + 1 >= len(options):
                return "-ERR missing arguments for 'set' command"
            exp = int(options[idx + 1])
            if opt.startswith("EX"):
                exp *= 1000
            if not opt.endswith("AT"):
                exp += set_time
            break

    logging.info("SET time %d key %s value %s exp %s", set_time, key, value, exp)
    REDIS_DB_DATA[REDIS_DB_NUM][key] = {"value": value, "exp": exp}
    return "OK"
