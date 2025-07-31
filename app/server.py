import logging

from app.redis import (
    REDIS_QUIT,
    decode_redis,
    handle_redis,
    pub_message,
    register_slave,
    send_write,
    setup_redis,
    sub_channel,
)
from lib import curio

from .client import run_client

REDIS_HOST = "localhost"
REDIS_PORT = 6379


async def client_connected_cb(client: curio.io.Socket, addr: str) -> None:
    logging.info("[%s] New connection", addr)

    is_replica = False
    multi_state = False
    multi_commands: list[list[str]] | None = None
    sub_mode = False
    subbed_channels: set[str] | None = None
    recv_message = b""
    while True:
        recv_message += await client.recv(100)
        if recv_message == b"":
            break

        if len(recv_message) > 0:
            logging.info("[%s] Recv %s", str(addr), repr(recv_message))
            command_line, parsed_length = decode_redis(recv_message)
            if parsed_length == 0:
                continue
            logging.info(
                "[%s] Command line %s (%d)",
                str(addr),
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
                sub_mode,
                new_channels,
                send_pub,
            ) = await handle_redis(
                command_line,
                0,
                multi_state,
                multi_commands,
                sub_mode,
                subbed_channels,
            )

            recv_message = recv_message[parsed_length:]

            if send_message == REDIS_QUIT:
                break

            if new_channels is not None:
                if subbed_channels is None:
                    subbed_channels = set()
                added_channels = new_channels - subbed_channels
                for ch in added_channels:
                    sub_channel(ch, client)
                subbed_channels.update(added_channels)

            if send_pub is not None:
                await pub_message(send_pub[0], send_pub[1])

            logging.info("[%s] Send %s", str(addr), repr(send_message))
            await client.sendall(send_message)

            if is_replica:
                await register_slave(client)
                break

            if send_replica:
                await send_write(send_replica)

    while is_replica:
        await curio.sleep(0)

    logging.info("[%s] Connection closed", addr)


async def run_server(
    dbdirname: str, dbfilename: str, port: int = REDIS_PORT, replicaof: str = ""
) -> None:
    is_slave = replicaof is not None
    await setup_redis(dbdirname, dbfilename, is_slave)
    if is_slave:
        _ = await curio.spawn(run_client, replicaof, port)

    print(f"Serving on {REDIS_HOST}:{port}")
    await curio.tcp_server(REDIS_HOST, port, client_connected_cb)
