from app.redis.rdb.length import decode_length, encode_length

from .constants import RDBOpCode
from .value import read_rdb_value, write_rdb_value


def read_rdb_size(buffer: bytes) -> tuple[int, int, int]:
    if buffer[0] == RDBOpCode.RESIZEDB:
        shash, vhash = decode_length(buffer[1:])
        sexph, vexph = decode_length(buffer[1 + shash :])
        return 1 + shash + sexph, vhash, vexph
    return 0, 0, 0


def write_rdb_size(size_hash: int, size_exph: int) -> bytes:
    return (
        bytes([RDBOpCode.RESIZEDB])
        + encode_length(size_hash)
        + encode_length(size_exph)
    )


def read_rdb_data(
    buffer: bytes, pos: int = 0
) -> tuple[int, int | None, dict[str, dict[str, str]], dict[str, int]]:
    num = None
    data = {}
    dexp = {}

    if buffer[pos] != RDBOpCode.SELECTDB:
        return pos, num, data, dexp

    pos += 1
    num = buffer[pos]
    pos += 1
    ssize, _, _ = read_rdb_size(buffer[pos:])
    pos += ssize

    while True:
        if buffer[pos] in [RDBOpCode.SELECTDB, RDBOpCode.EOF]:
            return pos, num, data, dexp

        sval, vkey, vval, dbtype, vexp = read_rdb_value(buffer[pos:])
        pos += sval

        data[vkey] = {"value": vval, "type": dbtype}
        if vexp is not None:
            dexp[vkey] = vexp


def write_rdb_data(
    data: dict[int, dict[str, dict[str, str]]],
    dexp: dict[int, dict[str, int]],
) -> bytes:
    buffer = b""
    for db_num, db_data in data.items():
        db_exp = dexp[db_num]
        buffer += bytes([RDBOpCode.SELECTDB, db_num])
        buffer += write_rdb_size(len(db_data), len(db_exp))
        for key, value in db_data.items():
            buffer += write_rdb_value(key, value["value"], db_exp.get(key))
    return buffer
