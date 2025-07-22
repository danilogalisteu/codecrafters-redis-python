import asyncio
import logging

from .redis import REDIS_QUIT, decode_redis, handle_redis
from .redis.handshake import send_handshake


async def run_client(master_host: str, master_port: int, slave_port: int) -> None:
    logging.info("Connecting to master on %s:%d", master_host, master_port)
    reader, writer = await asyncio.open_connection(master_host, master_port)

    master_id, master_offset, recv_message = await send_handshake(
        reader, writer, slave_port
    )
    logging.info("Connected master %s %d", master_id, master_offset)

    master_offset = 0
    multi_state = False
    multi_commands: list[list[str]] = []
    while True:
        if len(recv_message) > 0:
            logging.info("Master recv  %s", repr(recv_message))
            send_message = ""
            while len(recv_message) > 0:
                command_line, parsed_length = decode_redis(recv_message)
                if parsed_length == 0:
                    break
                logging.info(
                    "Command line %s (%d)",
                    str(command_line),
                    parsed_length,
                )

                (
                    send_message,
                    _,
                    _,
                    send_master,
                    multi_state,
                    multi_commands,
                ) = await handle_redis(
                    command_line, master_offset, multi_state, multi_commands
                )
                recv_message = recv_message[parsed_length:]
                master_offset += parsed_length

                if send_master:
                    logging.info("Master send %s", repr(send_master))
                    writer.write(send_master)
                    await writer.drain()

                if send_message == REDIS_QUIT:
                    break

            if send_message == REDIS_QUIT:
                break

        await asyncio.sleep(0)
        recv_message += await reader.read(100)
        if recv_message == b"":
            break

    logging.info("Close the connection")
    writer.close()
    await writer.wait_closed()
