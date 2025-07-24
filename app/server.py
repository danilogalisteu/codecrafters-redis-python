import logging

import curio

from app.redis import REDIS_QUIT, decode_redis, register_slave, send_write

REDIS_HOST = "localhost"
REDIS_PORT = 6379


async def client_connected_cb(
    client: curio.io.Socket, addr: str, cmd_queue: curio.Queue
) -> None:
    logging.info("[%s] New connection", addr)

    res_queue = curio.Queue()
    is_replica = False
    multi_state = False
    multi_commands: list[list[str]] | None = None
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

            await cmd_queue.put(
                (res_queue, command_line, 0, multi_state, multi_commands)
            )
            (
                send_message,
                is_replica,
                send_replica,
                _,
                multi_state,
                multi_commands,
            ) = await res_queue.get()

            recv_message = recv_message[parsed_length:]

            if send_message == REDIS_QUIT:
                break

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


async def run_server(cmd_queue: curio.Queue, port: int = REDIS_PORT) -> None:
    logging.info("Serving on %s:%d", REDIS_HOST, port)

    async def client_connected_task(client: curio.io.Socket, addr: str) -> None:
        return await client_connected_cb(client, addr, cmd_queue)

    await cmd_queue.put(None)
    await curio.tcp_server(REDIS_HOST, port, client_connected_task)
