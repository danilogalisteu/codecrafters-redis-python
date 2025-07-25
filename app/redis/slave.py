import logging

import curio

from app.redis.database import write_db
from app.redis.rdb.data import encode_data
from app.redis.resp import decode_redis, encode_redis

REDIS_OFFSET = 0
REDIS_SLAVES: set[curio.io.Socket] = set()


async def add_offset(offset: int) -> None:
    global REDIS_OFFSET
    REDIS_OFFSET += offset
    logging.info("Updated offset %d", REDIS_OFFSET)


async def register_slave(sock: curio.io.Socket) -> None:
    logging.info("Adding slave %s", str(sock.getpeername()))
    await sock.sendall(encode_data(write_db()))
    REDIS_SLAVES.add(sock)


async def get_offset(res_queue: curio.Queue, sid: int, sock: curio.io.Socket) -> None:
    send_message = encode_redis(["REPLCONF", "GETACK", "*"])
    await sock.sendall(send_message)

    recv_message = b""
    while True:
        recv_message += await sock.recv(100)
        command_line, parsed_length = decode_redis(recv_message)
        if parsed_length > 0:
            break

    assert command_line[0] == "REPLCONF"
    assert command_line[1] == "ACK"
    await res_queue.put((sid, int(command_line[2])))


async def send_write(send_message: bytes) -> None:
    logging.info("Replicating message %d %s...", len(send_message), repr(send_message))
    await add_offset(len(send_message))
    closed = []
    for sock in REDIS_SLAVES:
        logging.info("...to %s", str(sock.getpeername()))
        try:
            await sock.sendall(send_message)
        except:
            closed.append(sock)
    for sock in closed:
        REDIS_SLAVES.discard(sock)


async def wait_slaves(num_slaves: int, timeout_ms: int) -> int:
    logging.info("Checking offsets %d", REDIS_OFFSET)
    if REDIS_OFFSET == 0:
        return len(REDIS_SLAVES)

    res_queue = curio.Queue()
    slaves = dict(enumerate(REDIS_SLAVES))

    try:
        async with (
            curio.timeout_after(float(timeout_ms) / 1000),
            curio.TaskGroup(wait=all) as g,
        ):
            for sid, sock in slaves.items():
                await g.spawn(get_offset(res_queue, sid, sock))
    except curio.TaskTimeout:
        pass

    slave_offsets = {}
    while not res_queue.empty():
        sid, offset = await res_queue.get()
        slave_offsets[sid] = offset
    logging.info("Slave offsets %s", repr(list(slave_offsets.values())))
    updated_slaves = len([1 for o in slave_offsets.values() if o == REDIS_OFFSET])

    send_message = encode_redis(["REPLCONF", "GETACK", "*"])
    await add_offset(len(send_message))
    return updated_slaves
