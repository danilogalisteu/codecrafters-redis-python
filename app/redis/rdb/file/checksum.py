import struct

from ..constants import RDBOpCode


def read_rdb_checksum(buffer: bytes, pos: int = 0) -> tuple[int, int]:
    if len(buffer[pos:]) < 9:
        return pos, 0

    if buffer[pos] != RDBOpCode.EOF:
        return pos, 0

    checksum = struct.unpack("<Q", buffer[pos + 1 : pos + 9])[0]
    return pos + 9, checksum


def write_rdb_checksum(checksum: int) -> bytes:
    return bytes([RDBOpCode.EOF]) + struct.pack("<Q", checksum)
