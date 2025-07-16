import asyncio
import logging

from .database import write_db
from .rdb.data import encode_data
from .resp import REDIS_SEPARATOR, decode_redis, encode_redis

REDIS_SLAVES: set[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = set()


async def init_slave(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    logging.info("Adding slave %s", str(writer.get_extra_info("peername")))
    writer.write(encode_data(write_db()))
    await writer.drain()
    REDIS_SLAVES.add((reader, writer))


async def get_offset(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> int:
    send_message = encode_redis(["REPLCONF", "GETACK", "*"]) + REDIS_SEPARATOR
    writer.write(send_message.encode())
    await writer.drain()

    recv_message = ""
    while True:
        logging.info("get_offset reader.read %s", str(writer.get_extra_info("peername")))
        recv_message += (await reader.read(100)).decode()
        logging.info("get_offset reader.read done %s", str(writer.get_extra_info("peername")))
        command_line, parsed_length = decode_redis(recv_message)
        if parsed_length > 0:
            break

    assert command_line[0] == "REPLCONF"
    assert command_line[1] == "ACK"
    return int(command_line[2])


async def send_write(send_message: str) -> None:
    logging.info("Replicating message %s...", repr(send_message))
    closed = []
    for reader, writer in REDIS_SLAVES:
        logging.info("...to %s", str(writer.get_extra_info("peername")))
        if not writer.is_closing():
            writer.write(send_message.encode())
            await writer.drain()
        else:
            closed.append((reader, writer))
    for reader, writer in closed:
        logging.info("Removing slave %s", repr(writer))
        REDIS_SLAVES.discard((reader, writer))


async def wait_slaves(master_offset: int, num_slaves: int, timeout_ms: int) -> int:
    logging.info("Checking offsets %d", master_offset)
    tasks = [
        asyncio.create_task(get_offset(reader, writer))
        for reader, writer in REDIS_SLAVES
    ]
    done, _ = await asyncio.wait(tasks, timeout=float(timeout_ms) / 1000)
    slave_offsets = [t.result() for t in done]
    logging.info("Slave offsets %s", repr(slave_offsets))
    return len([1 for o in slave_offsets if o == master_offset])
