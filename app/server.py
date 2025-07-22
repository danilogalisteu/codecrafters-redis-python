import asyncio
import logging

from .client import run_client
from .redis import (
    REDIS_QUIT,
    decode_redis,
    handle_redis,
    init_slave,
    send_write,
    setup_redis,
)

REDIS_HOST = "localhost"
REDIS_PORT = 6379


async def client_connected_cb(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    addr = writer.get_extra_info("peername")
    logging.info("[%s] New connection", str(addr))

    is_replica = False
    multi_state = False
    multi_commands: list[list[str]] | None = None
    recv_message = b""
    while True:
        await asyncio.sleep(0)
        recv_message += await reader.read(100)
        if recv_message == b"":
            break

        if len(recv_message) > 0:
            logging.info("[%s] Recv %s", str(addr), repr(recv_message))
            command_line, parsed_length = decode_redis(recv_message)
            if parsed_length == 0:
                continue
            logging.info(
                "Command line %s (%d)",
                str(command_line),
                parsed_length,
            )

            (
                send_message,
                is_replica,
                send_replica,
                _,
                multi_state,
                multi_commands,
            ) = await handle_redis(command_line, 0, multi_state, multi_commands)
            recv_message = recv_message[parsed_length:]

            if send_message == REDIS_QUIT:
                break

            logging.info("[%s] Send %s", str(addr), repr(send_message))
            writer.write(send_message)
            await writer.drain()

            if is_replica:
                await init_slave(reader, writer)
                break

            if send_replica:
                await send_write(send_replica)

    while is_replica:
        await asyncio.sleep(0)

    logging.info("[%s] Closing connection", str(addr))
    writer.close()
    await writer.wait_closed()


async def run_server(
    dirname: str | None,
    dbfilename: str | None = None,
    port: int = REDIS_PORT,
    replicaof: str | None = None,
) -> None:
    is_slave = replicaof is not None
    await setup_redis(dirname, dbfilename, is_slave)

    if is_slave:
        master_host, master_port = replicaof.split(" ")
        asyncio.create_task(run_client(master_host, int(master_port), port))

    redis_server = await asyncio.start_server(
        client_connected_cb,
        host=REDIS_HOST,
        port=port,
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in redis_server.sockets)
    logging.info("Serving on %s", str(addrs))

    async with redis_server:
        await redis_server.serve_forever()
