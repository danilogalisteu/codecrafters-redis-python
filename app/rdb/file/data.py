from .value import read_rdb_value, save_rdb_value
from ..constants import RDBOpCode
from ..length import decode_length, encode_length


def read_rdb_size(buffer: bytes) -> tuple[int, int, int]:
    if buffer[0] == RDBOpCode.RESIZEDB:
        shash, vhash = decode_length(buffer[1:])
        sexph, vexph = decode_length(buffer[1 + shash :])
        return 1 + shash + sexph, vhash, vexph
    return 0, 0, 0


def save_rdb_size(size_hash: int, size_exph: int) -> bytes:
    return (
        bytes([RDBOpCode.RESIZEDB])
        + encode_length(size_hash)
        + encode_length(size_exph)
    )


def read_rdb_data(
    buffer: bytes, pos: int = 0
) -> tuple[int, int | None, dict[str, dict[str, str | int | None]]]:
    num = None
    data = {}

    if buffer[pos] != RDBOpCode.SELECTDB:
        return pos, num, data

    pos += 1
    num = buffer[pos]
    pos += 1
    ssize, size_hash, size_exph = read_rdb_size(buffer[pos:])
    pos += ssize

    while True:
        if buffer[pos] in [RDBOpCode.SELECTDB, RDBOpCode.EOF]:
            return pos, num, data

        sval, vkey, vval, vexp = read_rdb_value(buffer[pos:])
        pos += sval

        data[vkey] = {"value": vval, "exp": vexp}


def save_rdb_data(data: dict[int, dict[str, dict[str, str | int | None]]]) -> bytes:
    buffer = bytes()
    for db_num, db_data in data.items():
        buffer += bytes([RDBOpCode.SELECTDB, db_num])
        buffer += save_rdb_size(
            len(db_data),
            len([val for val in db_data.values() if val["exp"] is not None]),
        )
        for key, value in db_data.items():
            buffer += save_rdb_value(key, value["value"], value["exp"])
    return buffer
