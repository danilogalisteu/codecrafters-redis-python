import asyncio
import logging
from typing import Any

from .database import get_value, set_value
from .redis import REDIS_SEPARATOR, decode_redis, encode_redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_CONFIG = {}


async def parse_redis(message: str) -> tuple[Any, int]:
    logging.debug("new parse %s", repr(message))
    return decode_redis(message)


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
            command_line, parsed_length = await parse_redis(recv_message)
            logging.debug(
                "buffer %d %d %s",
                parsed_length,
                len(recv_message),
                recv_message,
            )
            recv_message = recv_message[parsed_length:]
            logging.debug("new buffer %d %d %s", 0, len(recv_message), recv_message)

            logging.info(
                "[%s] Command line %s (%d)",
                str(addr),
                str(command_line),
                parsed_length,
            )
            command = command_line[0].upper()
            arguments = command_line[1:] if len(command_line) > 0 else []

            send_message = ""
            match command:
                case "QUIT":
                    break
                case "PING":
                    if arguments:
                        if len(arguments) == 1:
                            send_message = encode_redis(arguments[0])
                        else:
                            send_message = (
                                "-ERR wrong number of arguments for 'ping' command"
                            )
                    else:
                        send_message = "+PONG"
                case "ECHO":
                    if len(arguments) == 1:
                        send_message = encode_redis(arguments[0])
                    else:
                        send_message = (
                            "-ERR wrong number of arguments for 'echo' command"
                        )
                case "SET":
                    if len(arguments) < 2:
                        send_message = (
                            "-ERR wrong number of arguments for 'set' command"
                        )
                    else:
                        res = set_value(
                            arguments[0],
                            arguments[1],
                            [arg.upper() for arg in arguments[2:]],
                        )
                        send_message = encode_redis(res)
                case "GET":
                    if len(arguments) != 1:
                        send_message = (
                            "-ERR wrong number of arguments for 'get' command"
                        )
                    else:
                        res = get_value(arguments[0])
                        send_message = encode_redis(res)
                case _:
                    logging.info("unhandled command %s", command)

            send_message += REDIS_SEPARATOR
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
