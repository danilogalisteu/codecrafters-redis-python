from .constants import RDB_NAME, RDB_VERSION


def read_rdb_header(buffer: bytes) -> tuple[int, str]:
    assert buffer[:5].decode() == RDB_NAME
    return 9, buffer[5:9].decode()


def write_rdb_header(version: str = RDB_VERSION) -> bytes:
    return RDB_NAME.encode() + version.encode()
