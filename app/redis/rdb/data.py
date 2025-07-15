from app.redis.resp import REDIS_SEPARATOR, IDAggregate

from .constants import RDB_NAME


def decode_data(data: bytes) -> bytes:
    if len(data) < 1:
        return b""
    if data[0] != IDAggregate.BSTRING:
        return b""
    rdb_start = data.find(RDB_NAME.encode())
    rdb_length = int(data[1:rdb_start])
    if len(data) < rdb_start + rdb_length:
        return b""
    return data[rdb_start:]


def encode_data(data: bytes) -> bytes:
    if len(data) == 0:
        return (IDAggregate.BSTRING + "-1").encode()
    return (IDAggregate.BSTRING + str(len(data)) + REDIS_SEPARATOR).encode() + data
