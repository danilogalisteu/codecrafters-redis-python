import struct

from app.redis.rdb.constants import RDBOpCode


def read_rdb_checksum(buffer: bytes, pos: int = 0) -> tuple[int, int]:
    if len(buffer[pos:]) < 9:
        return pos, 0

    if buffer[pos] != RDBOpCode.EOF:
        return pos, 0

    pos += 1
    return pos + 8, struct.unpack("<Q", buffer[pos : pos + 8])[0]


def write_rdb_checksum(checksum: int) -> bytes:
    return bytes([RDBOpCode.EOF]) + struct.pack("<Q", checksum)
