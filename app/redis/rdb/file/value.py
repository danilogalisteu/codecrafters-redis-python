from typing import Any
import struct

from ..constants import RDBOpCode, RDBValue
from ..string import decode_string, encode_string


def read_rdb_value(
    buffer: bytes, pos: int = 0
) -> tuple[int, str, str | int, int | None]:
    vexp = None
    if buffer[pos] == RDBOpCode.EXPIRETIME:
        vexp = struct.unpack("<L", buffer[pos + 1 : pos + 5])[0] * 1000
        pos += 5
    elif buffer[pos] == RDBOpCode.EXPIRETIMEMS:
        vexp = struct.unpack("<Q", buffer[pos + 1 : pos + 9])[0]
        pos += 9

    vtype = buffer[pos]
    pos += 1
    skey, vkey = decode_string(buffer[pos:])
    if skey == 0:
        raise ValueError("error reading RDB key")
    pos += skey
    match vtype:
        case RDBValue.STR:
            sval, vval = decode_string(buffer[pos:])
            if skey == 0:
                raise ValueError("error reading RDB string value")
            pos += sval
        case _:
            raise ValueError(f"unhandled RDB type {vtype} {RDBValue(vtype).name}")

    return pos, str(vkey), vval, vexp


def write_rdb_value(key: str, value: Any, exp: int | None) -> bytes:
    buffer = b""
    if exp is not None:
        if exp % 1000 == 0:
            buffer += bytes([RDBOpCode.EXPIRETIME]) + struct.pack("<L", int(exp / 1000))
        else:
            buffer += bytes([RDBOpCode.EXPIRETIMEMS]) + struct.pack("<Q", exp)

    if isinstance(value, str):
        buffer += bytes([RDBValue.STR])
        buffer += encode_string(key)
        buffer += encode_string(value)
        return buffer

    raise TypeError(f"unhandled RDB type {type(value)}")
