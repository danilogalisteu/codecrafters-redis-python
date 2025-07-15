import logging

from .file.checksum import read_rdb_checksum, write_rdb_checksum
from .file.crc64 import crc64_redis
from .file.data import read_rdb_data, write_rdb_data
from .file.header import read_rdb_header, write_rdb_header
from .file.metadata import read_rdb_meta, write_rdb_meta


def read_rdb(
    buffer: bytes,
) -> tuple[dict[str, str], dict[int, dict[str, dict[str, str | int | None]]]]:
    logging.debug("%s", repr(buffer))

    db_pos, db_version = read_rdb_header(buffer)
    db_pos, db_meta = read_rdb_meta(buffer, db_pos)

    db_data = {}
    while True:
        db_pos, db_num, db_num_data = read_rdb_data(buffer, db_pos)
        if db_num is None:
            break
        db_data[db_num] = db_num_data

    db_calc = crc64_redis(buffer[:db_pos])
    db_pos, db_check = read_rdb_checksum(buffer, db_pos)
    logging.debug("checksum %d %d", db_check, db_calc)

    logging.debug("%s", repr(db_data))
    return db_meta, db_data


def write_rdb(
    meta: dict[str, str | int],
    data: dict[int, dict[str, dict[str, str | int | None]]],
) -> bytes:
    buffer = write_rdb_header()
    buffer += write_rdb_meta(meta)
    buffer += write_rdb_data(data)
    buffer += write_rdb_checksum(crc64_redis(buffer))
    return buffer
