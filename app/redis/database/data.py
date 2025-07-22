import logging
from pathlib import Path
from time import time_ns

from app.redis.rdb import read_rdb, write_rdb
from app.redis.rdb.file.constants import DBType

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
    logging.info("updated db meta %s", repr(REDIS_META))
    logging.info("updated db data %s", repr(REDIS_DB_VAL))
    logging.info("updated db exp %s", repr(REDIS_DB_EXP))


def save_db(dirname: str, dbfilename: str) -> None:
    db_fn = Path(dirname) / dbfilename
    db_fn.write_bytes(write_db())


def write_db() -> bytes:
    return write_rdb(REDIS_META, REDIS_DB_VAL, REDIS_DB_EXP)
