import logging
from pathlib import Path
from time import time_ns

from .rdb import read_rdb, write_rdb
from .rdb.file.constants import DBType
from .resp import encode_simple

REDIS_DB_NUM = 0
REDIS_DB_VAL: dict[int, dict[str, dict[str, str | dict[str, str]]]] = {REDIS_DB_NUM: {}}
REDIS_DB_EXP: dict[int, dict[str, int]] = {REDIS_DB_NUM: {}}
REDIS_META: dict[str, str | int] = {}


def get_current_time() -> int:
    """Current time in ms"""
    return int(time_ns() / 1e6)


def get_keys(pattern: str) -> list[str]:
    keys = list(REDIS_DB_VAL[REDIS_DB_NUM].keys())
    # TODO filter keys using pattern
    return keys


def get_type(key: str) -> DBType:
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        return DBType.NONE
    return DBType(REDIS_DB_VAL[REDIS_DB_NUM][key]["type"])


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


def load_db(dirname: str, dbfilename: str) -> None:
    db_fn = Path(dirname) / dbfilename
    if db_fn.is_file():
        read_db(db_fn.read_bytes())


def read_db(data: bytes) -> None:
    global REDIS_META, REDIS_DB_VAL
    REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP = read_rdb(data)
    if not REDIS_DB_VAL:
        REDIS_DB_VAL[REDIS_DB_NUM] = {}
        REDIS_DB_EXP[REDIS_DB_NUM] = {}
    logging.info("updated db meta %s", repr(REDIS_META))
    logging.info("updated db data %s", repr(REDIS_DB_VAL))
    logging.info("updated db exp %s", repr(REDIS_DB_EXP))


def save_db(dirname: str, dbfilename: str) -> None:
    db_fn = Path(dirname) / dbfilename
    db_fn.write_bytes(write_db())


def set_value(key: str, value: str, options: list[str]) -> bytes:
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


def set_value_stream(key: str, kid: str, values: dict[str, str]) -> str:
    set_time = get_current_time()

    logging.info("XADD key '%s' id '%s' value %s time %d", key, kid, values, set_time)
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        REDIS_DB_VAL[REDIS_DB_NUM][key] = {
            "value": {kid: values},
            "type": DBType.STREAM,
        }
    else:
        REDIS_DB_VAL[REDIS_DB_NUM][key]["value"][kid] = values

    return kid


def write_db() -> bytes:
    return write_rdb(REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP)
