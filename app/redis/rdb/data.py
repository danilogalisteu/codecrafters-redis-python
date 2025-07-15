from app.redis.resp import REDIS_SEPARATOR, IDAggregate


def decode_data(buffer: bytes) -> bytes:
    if len(buffer) < 1:
        return b""
    if buffer[0] != IDAggregate.BSTRING:
        return b""

    rdb_length_end = buffer.find(REDIS_SEPARATOR.encode())
    rdb_length = int(buffer[1:rdb_length_end])
    rdb_data_start = rdb_length_end + len(REDIS_SEPARATOR.encode())
    if len(buffer) < rdb_data_start + rdb_length:
        return b""
    return buffer[rdb_data_start:]


def encode_data(data: bytes) -> bytes:
    if len(data) == 0:
        return (IDAggregate.BSTRING + "-1").encode()
    return (IDAggregate.BSTRING + str(len(data)) + REDIS_SEPARATOR).encode() + data
