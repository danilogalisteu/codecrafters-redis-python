import logging

import curio

from app.redis import handle_redis, setup_redis

from .client import run_client
from .server import run_server


async def run_manager(
    dbdirname: str, dbfilename: str, port: int, replicaof: str
) -> None:
    is_slave = replicaof is not None
    await setup_redis(dbdirname, dbfilename, is_slave)

    cmd_queue = curio.Queue()
    if is_slave:
        _ = await curio.spawn(run_client, cmd_queue, replicaof, port)
        await cmd_queue.get()
    _ = await curio.spawn(run_server, cmd_queue, port)
    await cmd_queue.get()

    while True:
        (
            res_queue,
            command_line,
            master_offset,
            multi_state,
            multi_commands,
        ) = await cmd_queue.get()
        logging.info("New command: %s", command_line)
        await res_queue.put(
            await handle_redis(command_line, master_offset, multi_state, multi_commands)
        )
