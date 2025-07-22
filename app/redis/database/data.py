import logging
from pathlib import Path
from time import time_ns

from app.redis.rdb import read_rdb, write_rdb
from app.redis.rdb.file.constants import DBType

REDIS_DB_NUM = 0
REDIS_DB_VAL: dict[int, dict[str, dict[str, str | dict[str, str]]]] = {REDIS_DB_NUM: {}}
REDIS_DB_EXP: dict[int, dict[str, int]] = {REDIS_DB_NUM: {}}
REDIS_META: dict[str, str | int] = {}


def check_key(key: str) -> bool:
    return key in REDIS_DB_VAL[REDIS_DB_NUM]


def get_current_time() -> int:
    """Current time in ms"""
    return int(time_ns() / 1e6)


def get_data(key: str) -> tuple[dict[str, str | dict[str, str]], int | None]:
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        return {}, None

    get_time = get_current_time()
    data = REDIS_DB_VAL[REDIS_DB_NUM][key]
    exp = REDIS_DB_EXP[REDIS_DB_NUM].get(key)
    logging.debug("get_data key '%s' data %s exp %s time %d", key, data, exp, get_time)

    if exp is not None and get_time > exp:
        logging.warning("GET key '%s' expired exp %s time %d", key, exp, get_time)
        del REDIS_DB_VAL[REDIS_DB_NUM][key]
        del REDIS_DB_EXP[REDIS_DB_NUM][key]
        return {}, None

    return data, exp


def get_keys(pattern: str) -> list[str]:
    keys = list(REDIS_DB_VAL[REDIS_DB_NUM].keys())
    # TODO filter keys using pattern
    return keys


def get_type(key: str) -> DBType:
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        return DBType.NONE
    return DBType(REDIS_DB_VAL[REDIS_DB_NUM][key]["type"])


def load_db(dirname: str, dbfilename: str) -> None:
    db_fn = Path(dirname) / dbfilename
    if db_fn.is_file():
        read_db(db_fn.read_bytes())


def read_db(data: bytes) -> None:
    global REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP
    REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP = read_rdb(data)
    if not REDIS_DB_VAL:
        REDIS_DB_VAL[REDIS_DB_NUM] = {}
        REDIS_DB_EXP[REDIS_DB_NUM] = {}
    logging.warning("updated db meta %s", repr(REDIS_META))
    logging.warning("updated db data %s", repr(REDIS_DB_VAL))
    logging.warning("updated db exp %s", repr(REDIS_DB_EXP))


def save_db(dirname: str, dbfilename: str) -> None:
    db_fn = Path(dirname) / dbfilename
    db_fn.write_bytes(write_db())


def set_data(
    key: str,
    value: str | dict[str, str],
    exp: int | None = None,
    dtype: DBType = DBType.STR,
) -> None:
    logging.debug(
        "set_data key '%s' value %s exp %s time %d",
        key,
        value,
        exp,
        get_current_time(),
    )
    REDIS_DB_VAL[REDIS_DB_NUM][key] = {"value": value, "type": dtype}
    if exp is not None:
        REDIS_DB_EXP[REDIS_DB_NUM][key] = exp


def set_stream_data(
    key: str, millisecondsTime: int, sequenceNumber: int, values: dict[str, str]
) -> None:
    logging.debug(
        "set_stream_data key '%s' millisecondsTime %d sequenceNumber %d values %s time %d",
        key,
        millisecondsTime,
        sequenceNumber,
        values,
        get_current_time(),
    )
    if key not in REDIS_DB_VAL[REDIS_DB_NUM]:
        set_data(key, {millisecondsTime: {sequenceNumber: values}}, dtype=DBType.STREAM)
        return

    if millisecondsTime not in REDIS_DB_VAL[REDIS_DB_NUM][key]["value"]:
        REDIS_DB_VAL[REDIS_DB_NUM][key]["value"][millisecondsTime] = {}

    REDIS_DB_VAL[REDIS_DB_NUM][key]["value"][millisecondsTime][sequenceNumber] = values


def write_db() -> bytes:
    return write_rdb(REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP)
