import asyncio
import logging

from .database import write_db
from .rdb.data import encode_data

REDIS_SLAVES: list[asyncio.StreamWriter] = []


async def init_slave(writer: asyncio.StreamWriter) -> None:
    logging.info("Adding slave %s", repr(writer))
    writer.write(encode_data(write_db()))
    await writer.drain()
    REDIS_SLAVES.append(writer)


async def send_write(send_message: str) -> None:
    logging.info("Replicating message %s...", repr(send_message))
    closed = []
    for writer in REDIS_SLAVES:
        logging.info("...to %s", str(writer.get_extra_info("peername")))
        if not writer.is_closing():
            writer.write(send_message.encode())
            await writer.drain()
        else:
            closed.append(writer)
    for writer in closed:
        if writer in REDIS_SLAVES:
            logging.info("Removing slave %s", repr(writer))
            REDIS_SLAVES.remove(writer)


async def wait_slaves(num_slaves: int, timeout_ms: int) -> int:
    async def async_wait(num: int) -> None:
        while len(REDIS_SLAVES) < num:
            await asyncio.sleep(0)

    await asyncio.wait_for(
        async_wait(num_slaves),
        float(timeout_ms) / 1000,
    )

    return len(REDIS_SLAVES)
