import asyncio
import logging

from .database import write_db
from .rdb.data import encode_data

REDIS_SLAVES: list[asyncio.StreamWriter] = []


async def init_slave(writer: asyncio.StreamWriter) -> None:
    writer.write(encode_data(write_db()))
    await writer.drain()
    REDIS_SLAVES.append(writer)


async def send_write(send_message: str) -> None:
    logging.info("Replicating message %s", repr(send_message))
    closed = []
    for writer in REDIS_SLAVES:
        if not writer.is_closing():
            writer.write(send_message.encode())
            await writer.drain()
        else:
            closed.append(writer)
    for writer in closed:
        try:
            REDIS_SLAVES.remove(writer)
        except ValueError:
            pass
