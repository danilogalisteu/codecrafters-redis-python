from pathlib import Path

from .file.checksum import read_rdb_checksum, write_rdb_checksum
from .file.data import read_rdb_data, write_rdb_data
from .file.header import read_rdb_header, write_rdb_header
from .file.metadata import read_rdb_meta, write_rdb_meta


def read_rdb(
    db_fn: Path,
) -> tuple[dict[str, str], dict[int, dict[str, dict[str, str | int | None]]]]:
    if not db_fn.is_file():
        return {}, {}

    buffer = db_fn.read_bytes()
    print(buffer)

    db_pos, db_version = read_rdb_header(buffer)
    db_pos, db_meta = read_rdb_meta(buffer, db_pos)

    db_data = {}
    while True:
        db_pos, db_num, db_num_data = read_rdb_data(buffer, db_pos)
        if db_num is None:
            break
        db_data[db_num] = db_num_data

    db_pos, db_check = read_rdb_checksum(buffer, db_pos)

    print(db_data)
    return db_meta, db_data


def write_rdb(
    db_fn: Path,
    meta: dict[str, str],
    data: dict[int, dict[str, dict[str, str | int | None]]],
) -> None:
    buffer = write_rdb_header()
    buffer += write_rdb_meta(meta)
    buffer += write_rdb_data(data)
    buffer += write_rdb_checksum(0)
    with open(db_fn, "wb") as file:
        file.write(buffer)
