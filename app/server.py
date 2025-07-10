import asyncio
import logging

from .handler import REDIS_CONFIG, REDIS_QUIT, handle_redis
from .redis import REDIS_SEPARATOR

REDIS_HOST = "localhost"
REDIS_PORT = 6379


async def client_connected_cb(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    addr = writer.get_extra_info("peername")
    logging.info("[%s] New connection", str(addr))

    recv_message = ""
    while True:
        await asyncio.sleep(0)
        recv_message += (await reader.read(100)).decode()

        if len(recv_message) > 0:
            logging.info("[%s] Recv %s", str(addr), repr(recv_message))
            parsed_length, send_message = handle_redis(recv_message)
            recv_message = recv_message[parsed_length:]

            if send_message == REDIS_SEPARATOR:
                continue
            if send_message == REDIS_QUIT:
                break

            logging.info("[%s] Send %s", str(addr), repr(send_message))
            writer.write(send_message.encode())
            await writer.drain()

    logging.info("[%s] Closing connection", str(addr))
    writer.close()
    await writer.wait_closed()


async def run_server(dirname: str | None, dbfilename: str | None = None) -> None:
    if dirname:
        REDIS_CONFIG["dir"] = dirname
    if dbfilename:
        REDIS_CONFIG["dbfilename"] = dbfilename

    redis_server = await asyncio.start_server(
        client_connected_cb,
        host=REDIS_HOST,
        port=REDIS_PORT,
    )

    addrs = ", ".join(str(sock.getsockname()) for sock in redis_server.sockets)
    logging.info("Serving on %s", str(addrs))

    async with redis_server:
        await redis_server.serve_forever()
