import asyncio

from .redis import REDIS_SEPARATOR, decode_redis, encode_redis


REDIS_PORT = 6379


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
