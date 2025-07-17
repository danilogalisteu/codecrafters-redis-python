import asyncio
import logging

from .database import read_db
from .rdb.data import decode_data
from .resp import REDIS_SEPARATOR, decode_redis, encode_redis


async def send_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, slave_port: int
) -> tuple[str, int, bytes]:
    message = encode_redis(["PING"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message)
    await writer.drain()

    logging.info(
        "send_handshake reader.read %s", str(writer.get_extra_info("peername"))
    )
    data = await reader.read(100)
    logging.info("Received %s", repr(data))
    recv_message, pos = decode_redis(data)
    assert pos > 0, data
    assert recv_message == "PONG", recv_message
    data = data[pos:]

    message = (
        encode_redis(["REPLCONF", "listening-port", str(slave_port)]) + REDIS_SEPARATOR
    )
    logging.info("Sending %s", repr(message))
    writer.write(message)
    await writer.drain()

    data += await reader.read(100)
    logging.info("Received %s", repr(data))
    recv_message, pos = decode_redis(data)
    assert pos > 0, data
    assert recv_message == "OK", recv_message
    data = data[pos:]

    message = encode_redis(["REPLCONF", "capa", "psync2"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message)
    await writer.drain()

    data += await reader.read(100)
    logging.info("Received %s", repr(data))
    recv_message, pos = decode_redis(data)
    assert pos > 0, data
    assert recv_message == "OK", recv_message
    data = data[pos:]

    message = encode_redis(["PSYNC", "?", "-1"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message)
    await writer.drain()

    data += await reader.read(100)
    logging.info("Received %s", repr(data))
    recv_message, pos = decode_redis(data)
    assert pos > 0, data
    command = recv_message.split(" ")
    assert len(command) == 3, recv_message
    assert command[0] == "FULLRESYNC", recv_message
    data = data[pos:]
    master_id, master_offset = command[1], int(command[2])

    data += await reader.read(100)
    logging.info("Received %d %s", len(data), repr(data))

    rdb_data = b""
    while True:
        rdb_data, pos = decode_data(data)
        if pos > 0:
            data = data[pos:]
            break
        data += await reader.read(100)

    read_db(rdb_data)

    return master_id, master_offset, data
