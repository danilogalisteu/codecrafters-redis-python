import asyncio
import logging

from .database import write_db
from .rdb.data import encode_data
from .resp import decode_redis, encode_redis

REDIS_OFFSET = 0
REDIS_SLAVES: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()


async def add_offset(offset: int) -> None:
    global REDIS_OFFSET
    REDIS_OFFSET += offset
    logging.info("updated offset %d", REDIS_OFFSET)


async def init_slave(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    logging.info("Adding slave %s", str(writer.get_extra_info("peername")))
    writer.write(encode_data(write_db()))
    await writer.drain()
    REDIS_SLAVES.add((reader, writer))


async def get_offset(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> int:
    send_message = encode_redis(["REPLCONF", "GETACK", "*"])
    writer.write(send_message)
    await writer.drain()

    recv_message = b""
    while True:
        logging.info(
            "get_offset reader.read %s", str(writer.get_extra_info("peername"))
        )
        recv_message += await reader.read(100)
        logging.info(
            "get_offset reader.read done %s", str(writer.get_extra_info("peername"))
        )
        command_line, parsed_length = decode_redis(recv_message)
        if parsed_length > 0:
            break

    assert command_line[0] == "REPLCONF"
    assert command_line[1] == "ACK"
    return int(command_line[2])


async def send_write(send_message: bytes) -> None:
    logging.info("Replicating message %d %s...", len(send_message), repr(send_message))
    await add_offset(len(send_message))
    closed = []
    for reader, writer in REDIS_SLAVES:
        logging.info("...to %s", str(writer.get_extra_info("peername")))
        if not writer.is_closing():
            writer.write(send_message)
            await writer.drain()
        else:
            closed.append((reader, writer))
    for reader, writer in closed:
        logging.info("Removing slave %s", repr(writer))
        REDIS_SLAVES.discard((reader, writer))


async def wait_slaves(num_slaves: int, timeout_ms: int) -> int:
    logging.info("Checking offsets %d", REDIS_OFFSET)
    if REDIS_OFFSET == 0:
        return len(REDIS_SLAVES)

    tasks = [
        asyncio.create_task(get_offset(reader, writer))
        for reader, writer in REDIS_SLAVES
    ]
    done, pending = await asyncio.wait(tasks, timeout=float(timeout_ms) / 1000)
    for t in pending:
        t.cancel()
    slave_offsets = [t.result() for t in done]
    logging.info("Slave offsets %s", repr(slave_offsets))
    updated_slaves = len([1 for o in slave_offsets if o == REDIS_OFFSET])

    send_message = encode_redis(["REPLCONF", "GETACK", "*"])
    await add_offset(len(send_message))
    return updated_slaves
