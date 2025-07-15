import asyncio
import logging

from .redis import REDIS_QUIT, REDIS_SEPARATOR, handle_redis
from .redis.handshake import send_handshake


async def run_client(master_host: str, master_port: int, slave_port: int) -> None:
    logging.info("Connecting to master on %s:%d", master_host, master_port)
    reader, writer = await asyncio.open_connection(master_host, master_port)

    master_id, master_offset, recv_message = await send_handshake(
        reader, writer, slave_port
    )
    logging.info("Connected master %s %d", master_id, master_offset)

    while True:
        if len(recv_message) > 0:
            logging.info("Master recv  %s", repr(recv_message))
            while True:
                parsed_length, send_message, _, send_replica = handle_redis(recv_message)
                recv_message = recv_message[parsed_length:]
                if (parsed_length == 0) or (send_message == REDIS_QUIT):
                    break
                if send_replica:
                    logging.info("Master send %s", repr(send_replica))
                    writer.write(send_replica.encode())
                    await writer.drain()

            if send_message == REDIS_QUIT:
                break

        await asyncio.sleep(0)
        recv_message += (await reader.read()).decode()

    logging.info("Close the connection")
    writer.close()
    await writer.wait_closed()
