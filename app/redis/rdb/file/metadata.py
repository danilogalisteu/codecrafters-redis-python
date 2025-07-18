from app.redis.rdb.string import decode_string, encode_string

from .constants import RDBOpCode


def read_rdb_meta(buffer: bytes, pos: int = 0) -> tuple[int, dict[str, str | int]]:
    data = {}
    while True:
        if buffer[pos] != RDBOpCode.AUX:
            return pos, data
        pos += 1
        skey, vkey = decode_string(buffer[pos:])
        if skey == 0:
            raise ValueError("error reading RDB AUX name")
        pos += skey
        sval, vval = decode_string(buffer[pos:])
        if sval == 0:
            raise ValueError("error reading RDB AUX value")
        pos += sval
        data[vkey] = vval


def write_rdb_meta(meta: dict[str, str | int]) -> bytes:
    buffer = b""
    for key, value in meta.items():
        buffer += bytes([RDBOpCode.AUX])
        buffer += encode_string(key)
        buffer += encode_string(value)
    return buffer
