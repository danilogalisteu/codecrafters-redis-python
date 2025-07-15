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
    recv_sep = data.find(REDIS_SEPARATOR.encode())
    recv_message = data[: recv_sep + len(REDIS_SEPARATOR.encode())]
    logging.info("Received %s", repr(recv_message.decode()))

    recv_message = recv_message.decode().removesuffix(REDIS_SEPARATOR).split(" ")
    assert recv_message[0] == "+FULLRESYNC"
    master_id, master_offset = recv_message[1], int(recv_message[2])

    data = data[recv_sep + len(REDIS_SEPARATOR.encode()) :]
    data += await reader.read(100)
    logging.info("Received %s", repr(data))

    rdb_data = b""
    while not rdb_data:
        data += await reader.read(100)
        logging.info("Received %s", len(data), repr(data))
        rdb_data = decode_data(data)

    read_db(rdb_data)

    return master_id, master_offset
