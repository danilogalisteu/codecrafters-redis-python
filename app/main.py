import asyncio
import logging

from .database import set_value, get_value
from .redis import REDIS_SEPARATOR, decode_redis, encode_redis


logging.basicConfig(format="[%(asctime)s|%(levelname)s] %(message)s", level=logging.WARNING)

REDIS_PORT = 6379


async def parse_redis(message: str):
    logging.debug(f"new parse {message!r}")
    return decode_redis(message)


async def client_connected_cb(reader, writer):
    addr = writer.get_extra_info('peername')
    logging.info(f"[{addr!r}] New connection")

    recv_message = ""
    while True:
        await asyncio.sleep(0)
        recv_message += (await reader.read(100)).decode()

        if len(recv_message) > 0:
            logging.info(f"[{addr!r}] Recv {recv_message!r}")
            command_line, parsed_length = await parse_redis(recv_message)
            logging.debug("buffer %d %d %s", parsed_length, len(recv_message), recv_message)
            recv_message = recv_message[parsed_length:]
            logging.debug("new buffer %d %d %s", 0, len(recv_message), recv_message)

            logging.info(f"[{addr!r}] Command line {command_line} ({parsed_length})")
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
                            send_message = "-ERR wrong number of arguments for 'ping' command"
                    else:
                        send_message = "+PONG"
                case "ECHO":
                    if len(arguments) == 1:
                        send_message = encode_redis(arguments[0])
                    else:
                        send_message = "-ERR wrong number of arguments for 'echo' command"
                case "SET":
                    if len(arguments) < 2:
                        send_message = "-ERR wrong number of arguments for 'set' command"
                    else:
                        res = set_value(arguments[0], arguments[1], [arg.upper() for arg in arguments[2:]])
                        send_message = encode_redis(res)
                case "GET":
                    if len(arguments) != 1:
                        send_message = "-ERR wrong number of arguments for 'get' command"
                    else:
                        res = get_value(arguments[0])
                        send_message = encode_redis(res)
                case _:
                    logging.info("unhandled command %s", command)

            send_message += REDIS_SEPARATOR
            logging.info(f"[{addr!r}] Send {send_message!r}")
            writer.write(send_message.encode())
            await writer.drain()

    logging.info(f"[{addr!r}] Closing connection")
    writer.close()
    await writer.wait_closed()


async def run_server():
    redis_server = await asyncio.start_server(
        client_connected_cb,
        host="localhost",
        port=REDIS_PORT,
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in redis_server.sockets)
    logging.info(f'Serving on {addrs}')

    async with redis_server:
        await redis_server.serve_forever()


def main():
    asyncio.run(run_server())

if __name__ == "__main__":
    main()
