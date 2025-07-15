import logging

from app.redis.resp import REDIS_SEPARATOR, IDAggregate


def decode_data(buffer: bytes) -> tuple[bytes, int]:
    logging.info("buffer %d %s", len(buffer), repr(buffer))
    if len(buffer) < 1:
        return b"", 0
    if chr(buffer[0]) != IDAggregate.BSTRING:
        logging.info("not expected")
        return b"", 0

    rdb_length_end = buffer.find(REDIS_SEPARATOR.encode())
    rdb_length = int(buffer[1:rdb_length_end])
    rdb_data_start = rdb_length_end + len(REDIS_SEPARATOR.encode())
    logging.info(
        "%d %d %d %d %d",
        len(buffer),
        rdb_length_end,
        rdb_length,
        rdb_data_start,
        rdb_data_start + rdb_length,
    )
    if len(buffer) < rdb_data_start + rdb_length:
        return b"", 0
    return buffer[rdb_data_start : rdb_data_start + rdb_length], rdb_data_start + rdb_length


def encode_data(data: bytes) -> bytes:
    if len(data) == 0:
        return (IDAggregate.BSTRING + "-1").encode()
    return (IDAggregate.BSTRING + str(len(data)) + REDIS_SEPARATOR).encode() + data
