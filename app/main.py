import asyncio
from time import time_ns

from .redis import REDIS_SEPARATOR, decode_redis, encode_redis


REDIS_PORT = 6379
REDIS_DB = {}


async def parse_redis(message: str):
    print(f"new parse {message!r}")
    return decode_redis(message)


async def client_connected_cb(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"[{addr!r}] New connection")

    recv_message = ""
    while True:
        await asyncio.sleep(0.1)
        recv_message += (await reader.read(100)).decode()

        if len(recv_message) > 0:
            print(f"[{addr!r}] Recv {recv_message!r}")
            command_line, parsed_length = await parse_redis(recv_message)
            print("buffer", parsed_length, len(recv_message), recv_message)
            recv_message = recv_message[parsed_length:]
            print("new buffer", 0, len(recv_message), recv_message)

            print(f"[{addr!r}] Command line {command_line} ({parsed_length})")
            command = command_line[0]
            arguments = command_line[1:] if len(command_line) > 0 else []

            send_message = ""
            match command.upper():
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
                        set_key = arguments[0]
                        set_value = arguments[1]
                        set_dict = {"value": set_value, "exp": None}
                        REDIS_DB[set_key] = set_dict
                        send_message = encode_redis("OK")

                case "GET":
                    if len(arguments) != 1:
                        send_message = "-ERR wrong number of arguments for 'get' command"
                    else:
                        get_key = arguments[0]
                        get_dict = REDIS_DB.get(get_key, {})
                        get_exp = get_dict.get("exp", None)
                        if get_exp is not None:
                            get_time = time_ns()
                            if get_exp < get_time:
                                del REDIS_DB[get_key]
                                send_message = encode_redis("")
                            else:
                                get_value = get_dict.get("value", "")
                                send_message = encode_redis(get_value)
                        else:
                            get_value = get_dict.get("value", "")
                            send_message = encode_redis(get_value)
                case _:
                    print("unhandled command")

            send_message += REDIS_SEPARATOR
            print(f"[{addr!r}] Send {send_message!r}")
            writer.write(send_message.encode())
            await writer.drain()

    print(f"[{addr!r}] Closing connection")
    writer.close()
    await writer.wait_closed()


async def run_server():
    redis_server = await asyncio.start_server(
        client_connected_cb,
        host="localhost",
        port=REDIS_PORT,
    )

    addrs = ', '.join(str(sock.getsockname()) for sock in redis_server.sockets)
    print(f'Serving on {addrs}')

    async with redis_server:
        await redis_server.serve_forever()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    asyncio.run(run_server())

if __name__ == "__main__":
    main()
