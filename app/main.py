import asyncio
import socket  # noqa: F401


REDIS_PORT = 6379


async def parse_redis(message: str):
    recv_id = message[0]
    if recv_id in "+-:_#,()":
        # simple message
        assert message[-2:] == "\r\n"
        payload = message[1:-2]
        match recv_id:
            case "+":
                print("received string", payload)
            case _:
                print("unhandled simple id", recv_id, payload)
    else:
        # aggregate message
        assert message[-2:] == "\r\n"
        payload = message[:-2]
        values = payload.split("\r\n")
        print("received string", values)


async def client_connected_cb(reader, writer):
    addr = writer.get_extra_info('peername')
    print(f"[{addr!r}] New connection")

    while True:
        recv_message = (await reader.read(100)).decode()
        print(f"[{addr!r}] Recv {recv_message!r}")
        await parse_redis(recv_message)

        send_message = "+PONG\r\n"
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

    # server_socket = socket.create_server(("localhost", REDIS_PORT), reuse_port=True)
    # server_socket.accept() # wait for client

    asyncio.run(run_server())

if __name__ == "__main__":
    main()
