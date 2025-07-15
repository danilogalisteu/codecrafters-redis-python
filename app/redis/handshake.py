import asyncio
import logging

from .database import read_db
from .rdb.data import decode_data
from .resp import REDIS_SEPARATOR, encode_redis


async def send_handshake(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, slave_port: int
) -> tuple[str, int]:
    message = encode_redis(["PING"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    logging.info("Received %s", repr(data.decode()))
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+PONG"

    message = (
        encode_redis(["REPLCONF", "listening-port", str(slave_port)]) + REDIS_SEPARATOR
    )
    logging.info("Sending %s", repr(message))
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    logging.info("Received %s", repr(data.decode()))
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+OK"

    message = encode_redis(["REPLCONF", "capa", "psync2"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    logging.info("Received %s", repr(data.decode()))
    assert data.decode().removesuffix(REDIS_SEPARATOR) == "+OK"

    message = encode_redis(["PSYNC", "?", "-1"]) + REDIS_SEPARATOR
    logging.info("Sending %s", repr(message))
    writer.write(message.encode())
    await writer.drain()

    data = await reader.read(100)
    logging.info("Received %s", repr(data.decode()))

    recv_message = data.decode().removesuffix(REDIS_SEPARATOR).split(" ")
    assert recv_message[0] == "+FULLRESYNC"
    master_id, master_offset = recv_message[1], int(recv_message[2])

    data = await reader.read()
    logging.info("Received %s", repr(data))

    read_db(decode_data(data))

    return master_id, master_offset
